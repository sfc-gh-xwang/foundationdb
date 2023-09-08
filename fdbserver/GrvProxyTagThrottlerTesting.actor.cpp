/**
 * GrvProxyTagThrottlerTesting.actor.cpp
 */

#include "fdbclient/Knobs.h"
#include "fdbserver/GrvProxyTagThrottler.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

ACTOR static Future<Void> mockClient(GrvProxyTagThrottler* throttler,
                                     TransactionPriority priority,
                                     TransactionTag tag,
                                     int batchSize,
                                     double desiredRate,
                                     TransactionTagMap<uint32_t>* counters) {
	state Future<Void> timer;
	loop {
		timer = delayJittered(static_cast<double>(batchSize) / desiredRate);
		GetReadVersionRequest req;
		req.transactionCount = batchSize;
		req.throttlingId = ThrottlingIdRef::fromTag(tag);
		req.priority = priority;
		if (CLIENT_KNOBS->TRACK_AVERAGE_COST_ON_CLIENTS) {
			req.expectedTotalCost = batchSize;
		}
		throttler->addRequest(req);
		wait(success(req.reply.getFuture()) && timer);
		(*counters)[tag] += batchSize;
	}
}

ACTOR static Future<Void> mockFifoClient(GrvProxyTagThrottler* throttler) {
	state std::vector<GetReadVersionRequest> reqs;
	state int i = 0;
	// Used to track the order in which replies are received
	state std::vector<int> replyIndices;

	// Tag half of requests with one tag, half with another, then randomly shuffle
	for (i = 0; i < 2000; ++i) {
		auto& req = reqs.emplace_back();
		req.priority = TransactionPriority::DEFAULT;
		req.transactionCount = 1;
		if (i < 1000) {
			req.throttlingId = ThrottlingIdRef::fromTag("sampleTag1"_sr);
		} else {
			req.throttlingId = ThrottlingIdRef::fromTag("sampleTag2"_sr);
		}
	}
	deterministicRandom()->randomShuffle(reqs);

	// Send requests to throttler and assert that responses are received in FIFO order
	for (const auto& req : reqs) {
		throttler->addRequest(req);
	}
	state std::vector<Future<Void>> futures;
	for (int j = 0; j < 2000; ++j) {
		// Flow hack to capture replyIndices
		auto* _replyIndices = &replyIndices;
		futures.push_back(map(reqs[j].reply.getFuture(), [_replyIndices, j](auto const&) {
			(*_replyIndices).push_back(j);
			return Void();
		}));
	}
	wait(waitForAll(futures));
	for (i = 0; i < 2000; ++i) {
		ASSERT_EQ(replyIndices[i], i);
	}
	return Void();
}

ACTOR static Future<Void> mockServer(GrvProxyTagThrottler* throttler) {
	state Deque<GetReadVersionRequest> outBatchPriority;
	state Deque<GetReadVersionRequest> outDefaultPriority;
	loop {
		state double elapsed = (0.009 + 0.002 * deterministicRandom()->random01());
		wait(delay(elapsed));
		throttler->releaseTransactions(elapsed, outBatchPriority, outDefaultPriority);
		while (!outBatchPriority.empty()) {
			outBatchPriority.front().reply.send(GetReadVersionReply{});
			outBatchPriority.pop_front();
		}
		while (!outDefaultPriority.empty()) {
			outDefaultPriority.front().reply.send(GetReadVersionReply{});
			outDefaultPriority.pop_front();
		}
	}
}

static TransactionTag getRandomTag() {
	TransactionTag result;
	auto arr = new (result.arena()) uint8_t[32];
	for (int i = 0; i < 32; ++i) {
		arr[i] = (uint8_t)deterministicRandom()->randomInt(0, 256);
	}
	result.contents() = TransactionTagRef(arr, 32);
	return result;
}

static bool isNear(double desired, uint64_t actual) {
	return std::abs(desired - actual) * 10 < desired;
}

// Rate limit set at 10, but client attempts 20 transactions per second.
// Client should be throttled to only 10 transactions per second.
TEST_CASE("/GrvProxyTagThrottler/Simple") {
	state GrvProxyTagThrottler throttler(5.0);
	state TransactionTagMap<uint32_t> counters;
	{
		ThrottlingIdMap<double> rates;
		rates[ThrottlingId::fromTag("sampleTag"_sr)] = 10.0;
		throttler.updateRates(rates);
	}

	state Future<Void> client =
	    mockClient(&throttler, TransactionPriority::DEFAULT, "sampleTag"_sr, 1, 20.0, &counters);
	state Future<Void> server = mockServer(&throttler);
	wait(timeout(client && server, 60.0, Void()));
	TraceEvent("TagQuotaTest_Simple").detail("Counter", counters["sampleTag"_sr]);
	ASSERT(isNear(counters["sampleTag"_sr], 60.0 * 10.0));
	return Void();
}

// Clients share the available 30 transaction/second budget
TEST_CASE("/GrvProxyTagThrottler/MultiClient") {
	state GrvProxyTagThrottler throttler(5.0);
	state TransactionTagMap<uint32_t> counters;
	{
		ThrottlingIdMap<double> rates;
		rates[ThrottlingIdRef::fromTag("sampleTag"_sr)] = 30.0;
		throttler.updateRates(rates);
	}

	state std::vector<Future<Void>> clients;
	clients.reserve(10);
	for (int i = 0; i < 10; ++i) {
		clients.push_back(mockClient(&throttler, TransactionPriority::DEFAULT, "sampleTag"_sr, 1, 10.0, &counters));
	}

	state Future<Void> server = mockServer(&throttler);
	wait(timeout(waitForAll(clients) && server, 60.0, Void()));
	TraceEvent("TagQuotaTest_MultiClient").detail("Counter", counters["sampleTag"_sr]);
	ASSERT(isNear(counters["sampleTag"_sr], 60.0 * 30.0));
	return Void();
}

// Each tag receives 10 transaction/second budget
TEST_CASE("/GrvProxyTagThrottler/MultiTag") {
	state GrvProxyTagThrottler throttler(5.0);
	state TransactionTagMap<uint32_t> counters;
	{
		ThrottlingIdMap<double> rates;
		rates[ThrottlingIdRef::fromTag("sampleTag1"_sr)] = 10.0;
		rates[ThrottlingIdRef::fromTag("sampleTag2"_sr)] = 10.0;
		throttler.updateRates(rates);
	}
	state Future<Void> client1 =
	    mockClient(&throttler, TransactionPriority::DEFAULT, "sampleTag1"_sr, 1, 20.0, &counters);
	state Future<Void> client2 =
	    mockClient(&throttler, TransactionPriority::DEFAULT, "sampleTag2"_sr, 1, 20.0, &counters);
	state Future<Void> server = mockServer(&throttler);
	wait(timeout(client1 && client2 && server, 60.0, Void()));
	TraceEvent("TagQuotaTest_MultiTag")
	    .detail("Counter1", counters["sampleTag1"_sr])
	    .detail("Counter2", counters["sampleTag2"_sr]);
	ASSERT(isNear(counters["sampleTag1"_sr], 60 * 10.0));
	ASSERT(isNear(counters["sampleTag2"_sr], 60 * 10.0));
	return Void();
}

// Test processing GetReadVersionRequests that batch several transactions
TEST_CASE("/GrvProxyTagThrottler/Batch") {
	state GrvProxyTagThrottler throttler(5.0);
	state TransactionTagMap<uint32_t> counters;
	{
		ThrottlingIdMap<double> rates;
		rates[ThrottlingIdRef::fromTag("sampleTag"_sr)] = 10.0;
		throttler.updateRates(rates);
	}

	state Future<Void> client =
	    mockClient(&throttler, TransactionPriority::DEFAULT, "sampleTag"_sr, 5, 20.0, &counters);
	state Future<Void> server = mockServer(&throttler);
	wait(timeout(client && server, 60.0, Void()));

	TraceEvent("TagQuotaTest_Batch").detail("Counter", counters["sampleTag"_sr]);
	ASSERT(isNear(counters["sampleTag"_sr], 60.0 * 10.0));
	return Void();
}

// Tests cleanup of tags that are no longer throttled.
TEST_CASE("/GrvProxyTagThrottler/Cleanup1") {
	GrvProxyTagThrottler throttler(5.0);
	for (int i = 0; i < 1000; ++i) {
		auto const tag = getRandomTag();
		ThrottlingIdMap<double> rates;
		rates[ThrottlingIdRef::fromTag(tag)] = 10.0;
		throttler.updateRates(rates);
		ASSERT_EQ(throttler.size(), 1);
	}
	return Void();
}

// Tests cleanup of tags once queues have been emptied
TEST_CASE("/GrvProxyTagThrottler/Cleanup2") {
	GrvProxyTagThrottler throttler(5.0);
	{
		GetReadVersionRequest req;
		req.transactionCount = 1;
		req.throttlingId = ThrottlingIdRef::fromTag("sampleTag"_sr);
		req.priority = TransactionPriority::DEFAULT;
		throttler.addRequest(req);
	}
	ASSERT_EQ(throttler.size(), 1);
	throttler.updateRates(ThrottlingIdMap<double>{});
	ASSERT_EQ(throttler.size(), 1);
	{
		Deque<GetReadVersionRequest> outBatchPriority;
		Deque<GetReadVersionRequest> outDefaultPriority;
		throttler.releaseTransactions(0.1, outBatchPriority, outDefaultPriority);
	}
	// Calling updates cleans up the queues in throttler
	throttler.updateRates(ThrottlingIdMap<double>{});
	ASSERT_EQ(throttler.size(), 0);
	return Void();
}

// Tests that unthrottled transactions are released in FIFO order, even when they
// have different tags
TEST_CASE("/GrvProxyTagThrottler/Fifo") {
	state GrvProxyTagThrottler throttler(5.0);
	state Future<Void> server = mockServer(&throttler);
	wait(mockFifoClient(&throttler));
	return Void();
}

// Tests that while throughput is low, the tag throttler
// does not accumulate too much budget.
//
// A server is setup to server 100 transactions per second,
// then runs idly for 60 seconds. Then a client starts
// and attempts 200 transactions per second for 60 seconds.
// The server throttles the client to only achieve
// 10 transactions per second during this 60 second window.
// If the throttler is allowed to accumulate budget indefinitely
// during the idle 60 seconds, this test will fail.
TEST_CASE("/GrvProxyTagThrottler/LimitedIdleBudget") {
	state GrvProxyTagThrottler throttler(5.0);
	state TransactionTagMap<uint32_t> counters;
	{
		ThrottlingIdMap<double> rates;
		rates[ThrottlingIdRef::fromTag("sampleTag"_sr)] = 100.0;
		throttler.updateRates(rates);
	}

	state Future<Void> server = mockServer(&throttler);
	wait(delay(60.0));
	state Future<Void> client =
	    mockClient(&throttler, TransactionPriority::DEFAULT, "sampleTag"_sr, 1, 200.0, &counters);
	wait(timeout(client && server, 60.0, Void()));
	TraceEvent("TagQuotaTest_LimitedIdleBudget").detail("Counter", counters["sampleTag"_sr]);
	ASSERT(isNear(counters["sampleTag"_sr], 60.0 * 100.0));
	return Void();
}
