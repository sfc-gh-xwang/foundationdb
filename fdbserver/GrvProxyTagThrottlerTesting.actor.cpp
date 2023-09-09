/**
 * GrvProxyTagThrottlerTesting.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/Knobs.h"
#include "fdbserver/GrvProxyTagThrottler.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

class TransactionCounters {
	TransactionTagMap<uint32_t> defaultPriorityCounters, lowPriorityCounters;

public:
	uint32_t getCount(TransactionTag tag, TransactionPriority priority = TransactionPriority::DEFAULT) const {
		auto const& counters =
		    (priority == TransactionPriority::DEFAULT) ? defaultPriorityCounters : lowPriorityCounters;
		auto it = counters.find(tag);
		return (it == counters.end()) ? 0 : it->second;
	}

	void incrementCount(TransactionTag tag, int newCount, TransactionPriority priority = TransactionPriority::DEFAULT) {
		auto& counters = (priority == TransactionPriority::DEFAULT) ? defaultPriorityCounters : lowPriorityCounters;
		counters[tag] += newCount;
	}
};

} // namespace

ACTOR static Future<Void> mockClient(GrvProxyTagThrottler* throttler,
                                     TransactionPriority priority,
                                     TransactionTag tag,
                                     int batchSize,
                                     double desiredRate,
                                     TransactionCounters* counters) {
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
		counters->incrementCount(tag, batchSize, priority);
	}
}

// Given a vector of GRV requests, returns a vector of indices
// ordered by the order in which replies were received.
ACTOR static Future<std::vector<int>> getReplyOrder(std::vector<GetReadVersionRequest> reqs) {
	state std::vector<int> result;
	state std::vector<Future<Void>> futures;
	for (int i = 0; i < reqs.size(); ++i) {
		// Flow hack to capture result:
		auto* _result = &result;
		futures.push_back(map(reqs[i].reply.getFuture(), [_result, i](auto const&) {
			_result->push_back(i);
			return Void();
		}));
	}
	wait(waitForAll(futures));
	return result;
}

ACTOR static Future<Void> mockFifoClient(GrvProxyTagThrottler* throttler) {
	state std::vector<GetReadVersionRequest> reqs;

	// Tag half of requests with one tag, half with another, then randomly shuffle
	for (int i = 0; i < 2000; ++i) {
		auto& req = reqs.emplace_back();
		req.priority = TransactionPriority::DEFAULT;
		req.transactionCount = 1;
		if (CLIENT_KNOBS->TRACK_AVERAGE_COST_ON_CLIENTS) {
			req.expectedTotalCost = 1;
		}
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
	std::vector<int> replyIndices = wait(getReplyOrder(reqs));
	for (int i = 0; i < 2000; ++i) {
		ASSERT_EQ(replyIndices[i], i);
	}
	return Void();
}

ACTOR static Future<Void> mockVariablePriorityClient(GrvProxyTagThrottler* throttler) {
	state std::vector<GetReadVersionRequest> reqs;

	for (int i = 0; i < 2000; ++i) {
		auto& req = reqs.emplace_back();
		req.transactionCount = 1;
		if (CLIENT_KNOBS->TRACK_AVERAGE_COST_ON_CLIENTS) {
			req.expectedTotalCost = 1;
		}
		req.throttlingId = ThrottlingIdRef::fromTag("sampleTag"_sr);
		if (i < 1000) {
			req.priority = TransactionPriority::LOW;
		} else {
			req.priority = TransactionPriority::DEFAULT;
		}
	}

	for (const auto& req : reqs) {
		throttler->addRequest(req);
	}

	std::vector<int> replyIndices = wait(getReplyOrder(reqs));

	// The set of the first 1000 requests to receive a reply should
	// be precisely the set of the last 1000 requests issued, because
	// these requests have default priority:
	for (int i = 0; i < 1000; ++i) {
		ASSERT_GE(replyIndices[i], 1000);
	}
	for (int i = 1000; i < 2000; ++i) {
		ASSERT_LT(replyIndices[i], 1000);
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
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state TransactionCounters counters;
	{
		ThrottlingIdMap<double> rates;
		rates[ThrottlingId::fromTag("sampleTag"_sr)] = 10.0;
		throttler.updateRates(rates);
	}

	state Future<Void> client =
	    mockClient(&throttler, TransactionPriority::DEFAULT, "sampleTag"_sr, 1, 20.0, &counters);
	state Future<Void> server = mockServer(&throttler);
	wait(timeout(client && server, 60.0, Void()));
	TraceEvent("TagQuotaTest_Simple").detail("Counter", counters.getCount("sampleTag"_sr));
	ASSERT(isNear(counters.getCount("sampleTag"_sr), 60.0 * 10.0));
	return Void();
}

// Clients share the available 30 transaction/second budget
TEST_CASE("/GrvProxyTagThrottler/MultiClient") {
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state TransactionCounters counters;
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
	TraceEvent("TagQuotaTest_MultiClient").detail("Counter", counters.getCount("sampleTag"_sr));
	ASSERT(isNear(counters.getCount("sampleTag"_sr), 60.0 * 30.0));
	return Void();
}

// Each tag receives 10 transaction/second budget
TEST_CASE("/GrvProxyTagThrottler/MultiTag") {
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state TransactionCounters counters;
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
	    .detail("Counter1", counters.getCount("sampleTag1"_sr))
	    .detail("Counter2", counters.getCount("sampleTag2"_sr));
	ASSERT(isNear(counters.getCount("sampleTag1"_sr), 60 * 10.0));
	ASSERT(isNear(counters.getCount("sampleTag2"_sr), 60 * 10.0));
	return Void();
}

// Test processing GetReadVersionRequests that batch several transactions
TEST_CASE("/GrvProxyTagThrottler/Batch") {
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state TransactionCounters counters;
	{
		ThrottlingIdMap<double> rates;
		rates[ThrottlingIdRef::fromTag("sampleTag"_sr)] = 10.0;
		throttler.updateRates(rates);
	}

	state Future<Void> client =
	    mockClient(&throttler, TransactionPriority::DEFAULT, "sampleTag"_sr, 5, 20.0, &counters);
	state Future<Void> server = mockServer(&throttler);
	wait(timeout(client && server, 60.0, Void()));

	TraceEvent("TagQuotaTest_Batch").detail("Counter", counters.getCount("sampleTag"_sr));
	ASSERT(isNear(counters.getCount("sampleTag"_sr), 60.0 * 10.0));
	return Void();
}

// Tests cleanup of tags that are no longer throttled.
TEST_CASE("/GrvProxyTagThrottler/Cleanup1") {
	GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
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
	GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
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

// Tests that unthrottled transactions with the same priority are released in
// FIFO order, even when they have different tags
TEST_CASE("/GrvProxyTagThrottler/Fifo") {
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state Future<Void> server = mockServer(&throttler);
	wait(mockFifoClient(&throttler));
	return Void();
}

// Tests that unthrottled transactions are released according
// to their priorities
TEST_CASE("/GrvProxyTagThrottler/VariablePriorities") {
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state Future<Void> server = mockServer(&throttler);
	wait(mockVariablePriorityClient(&throttler));
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
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state TransactionCounters counters;
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
	TraceEvent("TagQuotaTest_LimitedIdleBudget").detail("Counter", counters.getCount("sampleTag"_sr));
	ASSERT(isNear(counters.getCount("sampleTag"_sr), 60.0 * 100.0));
	return Void();
}

TEST_CASE("/GrvProxyTagThrottler/PrioritizedThroughput") {
	state double starvationToleranceRatio = deterministicRandom()->random01();
	state double duration = 60.0;
	state double throttlingRate = 100.0;
	state int clientsPerPriority = 10;
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration*/ duration * 2.0, starvationToleranceRatio);
	state TransactionCounters counters;
	{
		ThrottlingIdMap<double> rates;
		rates[ThrottlingIdRef::fromTag("sampleTag"_sr)] = throttlingRate;
		throttler.updateRates(rates);
	}

	state Future<Void> server = mockServer(&throttler);
	state std::vector<Future<Void>> defaultPriorityClients;
	state std::vector<Future<Void>> lowPriorityClients;
	for (int i = 0; i < clientsPerPriority; ++i) {
		defaultPriorityClients.push_back(mockClient(&throttler,
		                                            TransactionPriority::DEFAULT,
		                                            "sampleTag"_sr,
		                                            1,
		                                            throttlingRate / clientsPerPriority,
		                                            &counters));
		lowPriorityClients.push_back(mockClient(
		    &throttler, TransactionPriority::LOW, "sampleTag"_sr, 1, throttlingRate / clientsPerPriority, &counters));
	}
	wait(timeout(server && waitForAll(defaultPriorityClients) && waitForAll(lowPriorityClients), duration, Void()));
	auto const defaultPriorityThroughput = counters.getCount("sampleTag"_sr, TransactionPriority::DEFAULT);
	auto const lowPriorityThroughput = counters.getCount("sampleTag"_sr, TransactionPriority::LOW);
	TraceEvent("TagQuotaTest_PrioritizedThroughput")
	    .detail("DefaultPriorityCounter", defaultPriorityThroughput)
	    .detail("LowPriorityCounter", lowPriorityThroughput)
	    .detail("StarvationToleranceRatio", starvationToleranceRatio);
	// The released default priority throughput should be at least the
	// throughput released using the prioritized threshold. Also, at least half
	// of the total throughput should be default priority.
	double const minExpectedDefaultPriorityThroughput =
	    std::max(0.5, starvationToleranceRatio) * duration * throttlingRate;
	// This reflects half the quantity of FIFO releases (since default priority
	// transactions can still be released during the FIFO release stages).
	double const minExpectedLowPriorityThroughput = 0.5 * (1 - starvationToleranceRatio) * duration * throttlingRate;
	double const errorTolerance = 0.2;
	ASSERT_GE(defaultPriorityThroughput, minExpectedDefaultPriorityThroughput * (1 - errorTolerance));
	ASSERT_GE(lowPriorityThroughput, minExpectedLowPriorityThroughput * (1 - errorTolerance));
	ASSERT(isNear(defaultPriorityThroughput + lowPriorityThroughput, duration * throttlingRate));
	return Void();
}
