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

TransactionTag getRandomTag() {
	TransactionTag result;
	auto arr = new (result.arena()) uint8_t[32];
	for (int i = 0; i < 32; ++i) {
		arr[i] = (uint8_t)deterministicRandom()->randomInt(0, 256);
	}
	result.contents() = TransactionTagRef(arr, 32);
	return result;
}

GetReadVersionRequest getTestRequest(ThrottlingId const& throttlingId,
                                     int transactionCount = 1,
                                     TransactionPriority priority = TransactionPriority::DEFAULT) {
	GetReadVersionRequest result;
	result.throttlingId = throttlingId;
	result.transactionCount = transactionCount;
	result.expectedTotalCost = transactionCount;
	result.priority = priority;
	return result;
}

void setRate(GrvProxyTagThrottler& throttler, ThrottlingId const& throttlingId, double rate) {
	ThrottlingIdMap<double> rates;
	rates[throttlingId] = rate;
	throttler.updateRates(rates);
}

void clearRates(GrvProxyTagThrottler& throttler) {
	throttler.updateRates(ThrottlingIdMap<double>{});
}

bool isNear(double desired, uint64_t actual) {
	return std::abs(desired - actual) * 10 < desired;
}

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
		auto const req = getTestRequest(ThrottlingIdRef::fromTag(tag), batchSize, priority);
		if (throttler->tryAddRequest(req)) {
			wait(success(req.reply.getFuture()) && timer);
			counters->incrementCount(tag, batchSize, priority);
		} else {
			wait(timer);
			counters->incrementCount(tag, batchSize, priority);
		}
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

class MockFifoClient {
	std::vector<GetReadVersionRequest> reqs;

public:
	void sendRequests(GrvProxyTagThrottler& throttler,
	                  ThrottlingId const& throttlingId1,
	                  ThrottlingId const& throttlingId2) {
		// Tag half of requests with one tag, half with another, then randomly shuffle
		for (int i = 0; i < 2000; ++i) {
			auto& req = reqs.emplace_back();
			req.priority = TransactionPriority::DEFAULT;
			req.transactionCount = 1;
			req.expectedTotalCost = 1;
			if (i < 1000) {
				req.throttlingId = throttlingId1;
			} else {
				req.throttlingId = throttlingId2;
			}
		}
		deterministicRandom()->randomShuffle(reqs);

		// Send requests to throttler
		for (const auto& req : reqs) {
			ASSERT(throttler.tryAddRequest(req));
		}
	}

	ACTOR static Future<Void> checkReplyOrder(MockFifoClient* self) {
		std::vector<int> replyIndices = wait(getReplyOrder(self->reqs));
		for (int i = 0; i < 2000; ++i) {
			ASSERT_EQ(replyIndices[i], i);
		}
		return Void();
	}
};

class MockVariablePriorityClient {
	std::vector<GetReadVersionRequest> reqs;

public:
	void sendRequests(GrvProxyTagThrottler& throttler, ThrottlingId const& throttlingId) {
		for (int i = 0; i < 2000; ++i) {
			auto& req = reqs.emplace_back();
			req.transactionCount = 1;
			req.expectedTotalCost = 1;
			req.throttlingId = throttlingId;
			if (i < 1000) {
				req.priority = TransactionPriority::LOW;
			} else {
				req.priority = TransactionPriority::DEFAULT;
			}
		}

		for (const auto& req : reqs) {
			ASSERT(throttler.tryAddRequest(req));
		}
	}

	ACTOR static Future<Void> checkReplyOrder(MockVariablePriorityClient* self) {
		std::vector<int> replyIndices = wait(getReplyOrder(self->reqs));

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
};

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

// Rate limit set at 10, but client attempts 20 transactions per second.
// Client should be throttled to only 10 transactions per second.
TEST_CASE("/GrvProxyTagThrottler/Simple") {
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state double throttledThroughput = 10.0;
	state double attemptedThroughput = 20.0;
	state double duration = 60.0;
	state TransactionTag tag = "sampleTag"_sr;
	state TransactionCounters counters;

	setRate(throttler, ThrottlingIdRef::fromTag(tag), throttledThroughput);

	state Future<Void> client =
	    mockClient(&throttler, TransactionPriority::DEFAULT, tag, /*batchSize=*/1, attemptedThroughput, &counters);
	state Future<Void> server = mockServer(&throttler);
	wait(timeout(client && server, duration, Void()));
	TraceEvent("TagQuotaTest_Simple").detail("Counter", counters.getCount(tag));
	ASSERT(isNear(counters.getCount(tag), duration * throttledThroughput));
	return Void();
}

// Clients share the available 30 transaction/second budget
TEST_CASE("/GrvProxyTagThrottler/MultiClient") {
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state double duration = 60.0;
	state double attemptedThroughputPerClient = 10.0;
	state double throttledThroughput = 30.0;
	state TransactionTag tag = "sampleTag"_sr;
	state TransactionCounters counters;

	setRate(throttler, ThrottlingIdRef::fromTag(tag), throttledThroughput);

	state std::vector<Future<Void>> clients;
	clients.reserve(10);
	for (int i = 0; i < 10; ++i) {
		clients.push_back(mockClient(&throttler,
		                             TransactionPriority::DEFAULT,
		                             tag,
		                             /*batchSize=*/1,
		                             attemptedThroughputPerClient,
		                             &counters));
	}

	state Future<Void> server = mockServer(&throttler);
	wait(timeout(waitForAll(clients) && server, duration, Void()));
	TraceEvent("TagQuotaTest_MultiClient").detail("Counter", counters.getCount(tag));
	ASSERT(isNear(counters.getCount(tag), duration * throttledThroughput));
	return Void();
}

// Each tag receives 10 transaction/second budget
TEST_CASE("/GrvProxyTagThrottler/MultiTag") {
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state double duration = 60.0;
	state double attemptedThroughputPerTag = 20.0;
	state double throttledThroughputPerTag = 10.0;
	state TransactionTag tag1 = "sampleTag1"_sr;
	state TransactionTag tag2 = "sampleTag2"_sr;
	state TransactionCounters counters;

	{
		ThrottlingIdMap<double> rates;
		rates[ThrottlingIdRef::fromTag(tag1)] = rates[ThrottlingIdRef::fromTag(tag2)] = throttledThroughputPerTag;
		throttler.updateRates(rates);
	}

	state Future<Void> client1 = mockClient(&throttler,
	                                        TransactionPriority::DEFAULT,
	                                        tag1,
	                                        /*batchSize=*/1,
	                                        attemptedThroughputPerTag,
	                                        &counters);
	state Future<Void> client2 = mockClient(&throttler,
	                                        TransactionPriority::DEFAULT,
	                                        tag2,
	                                        /*batchSize=*/1,
	                                        attemptedThroughputPerTag,
	                                        &counters);
	state Future<Void> server = mockServer(&throttler);
	wait(timeout(client1 && client2 && server, duration, Void()));
	TraceEvent("TagQuotaTest_MultiTag")
	    .detail("Counter1", counters.getCount(tag1))
	    .detail("Counter2", counters.getCount(tag2));
	ASSERT(isNear(counters.getCount(tag1), duration * throttledThroughputPerTag));
	ASSERT(isNear(counters.getCount(tag2), duration * throttledThroughputPerTag));
	return Void();
}

// Test processing GetReadVersionRequests that batch several transactions
TEST_CASE("/GrvProxyTagThrottler/Batch") {
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state double duration = 60.0;
	state double attemptedThroughput = 20.0;
	state double throttledThroughput = 10.0;
	state int batchSize = 5;
	state TransactionTag tag = "sampleTag"_sr;
	state TransactionCounters counters;

	setRate(throttler, ThrottlingIdRef::fromTag(tag), throttledThroughput);

	state Future<Void> client =
	    mockClient(&throttler, TransactionPriority::DEFAULT, tag, batchSize, attemptedThroughput, &counters);
	state Future<Void> server = mockServer(&throttler);
	wait(timeout(client && server, duration, Void()));

	TraceEvent("TagQuotaTest_Batch").detail("Counter", counters.getCount(tag));
	ASSERT(isNear(counters.getCount(tag), duration * throttledThroughput));
	return Void();
}

// Tests cleanup of tags that are no longer throttled.
TEST_CASE("/GrvProxyTagThrottler/Cleanup1") {
	GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	for (int i = 0; i < 1000; ++i) {
		auto const tag = getRandomTag();
		setRate(throttler, ThrottlingIdRef::fromTag(tag), /*rate=*/10.0);
		ASSERT_EQ(throttler.size(), 1);
	}
	return Void();
}

// Tests cleanup of tags once queues have been emptied
TEST_CASE("/GrvProxyTagThrottler/Cleanup2") {
	auto const throttlingId = ThrottlingIdRef::fromTag("sampleTag"_sr);
	GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);

	// Exact rate does not matter here:
	setRate(throttler, throttlingId, 1000000.0);

	ASSERT(throttler.tryAddRequest(getTestRequest(throttlingId)));
	ASSERT_EQ(throttler.size(), 1);
	clearRates(throttler);
	ASSERT_EQ(throttler.size(), 1);
	{
		Deque<GetReadVersionRequest> outBatchPriority;
		Deque<GetReadVersionRequest> outDefaultPriority;
		throttler.releaseTransactions(0.1, outBatchPriority, outDefaultPriority);
	}
	// Calling updateRates cleans up the queues in throttler
	clearRates(throttler);
	ASSERT_EQ(throttler.size(), 0);
	return Void();
}

// Tests that unthrottled transactions with the same priority are released in
// FIFO order, even when they have different tags
TEST_CASE("/GrvProxyTagThrottler/Fifo") {
	state ThrottlingId throttlingId1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	state ThrottlingId throttlingId2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state MockFifoClient client;

	// First set rates so that requests are queued in throttler
	{
		ThrottlingIdMap<double> rates;
		// Exact rate doesn't matter here:
		rates[throttlingId1] = rates[throttlingId2] = 10.0;
		throttler.updateRates(rates);
	}
	client.sendRequests(throttler, throttlingId1, throttlingId2);

	// Clear rates here so that all requests are guaranteed to be released in the first release window
	clearRates(throttler);

	// Now run server to start release windows
	state Future<Void> server = mockServer(&throttler);
	wait(MockFifoClient::checkReplyOrder(&client));
	return Void();
}

// Tests that unthrottled transactions are released according
// to their priorities
TEST_CASE("/GrvProxyTagThrottler/VariablePriorities") {
	state ThrottlingId throttlingId = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state MockVariablePriorityClient client;

	// First set rate so that requests are queued in throttler.
	// Exact rate doesn't matter here:
	setRate(throttler, throttlingId, 10.0);
	client.sendRequests(throttler, throttlingId);

	// Clear rates here so that all requests are guaranteed to be released in the first release window
	clearRates(throttler);

	// Clear rates here so that all requests are guaranteed to be released in the first release window
	state Future<Void> server = mockServer(&throttler);
	wait(MockVariablePriorityClient::checkReplyOrder(&client));
	return Void();
}

// Tests that while throughput is low, the tag throttler
// does not accumulate too much budget.
//
// A server is setup to serve 100 transactions per second,
// then runs idly for 60 seconds. Then a client starts
// and attempts 200 transactions per second for 60 seconds.
// The server throttles the client to only achieve
// 10 transactions per second during this 60 second window.
// If the throttler is allowed to accumulate budget indefinitely
// during the idle 60 seconds, this test will fail.
TEST_CASE("/GrvProxyTagThrottler/LimitedIdleBudget") {
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	state TransactionTag tag = "sampleTag"_sr;
	state TransactionCounters counters;

	setRate(throttler, ThrottlingIdRef::fromTag(tag), 100.0);

	state Future<Void> server = mockServer(&throttler);
	wait(delay(60.0));
	state Future<Void> client = mockClient(&throttler, TransactionPriority::DEFAULT, tag, 1, 200.0, &counters);
	wait(timeout(client && server, 60.0, Void()));
	TraceEvent("TagQuotaTest_LimitedIdleBudget").detail("Counter", counters.getCount(tag));
	ASSERT(isNear(counters.getCount(tag), 60.0 * 100.0));
	return Void();
}

// Tests that throughput is properly distributed when using
// different transaction priorities.
//
// The server throttles clients to only achieve 100 transactions
// per second. 20 clients attempt 10 transactions per second each.
// Half of these clients run with low priority and the other half
// run with default priority. Based on the randomly chosen
// starvation tolerance ratio, aggregate throughput from each set
// of clients is expected to fall within certain bounds.
TEST_CASE("/GrvProxyTagThrottler/PrioritizedThroughput") {
	state double starvationToleranceRatio = deterministicRandom()->random01();
	state double duration = 60.0;
	state double throttlingRate = 100.0;
	state int clientsPerPriority = 10;
	state GrvProxyTagThrottler throttler(/*maxThrottleDuration*/ duration * 2.0, starvationToleranceRatio);
	state TransactionTag tag = "sampleTag"_sr;
	state TransactionCounters counters;

	setRate(throttler, ThrottlingIdRef::fromTag(tag), throttlingRate);

	state Future<Void> server = mockServer(&throttler);
	state std::vector<Future<Void>> defaultPriorityClients;
	state std::vector<Future<Void>> lowPriorityClients;
	for (int i = 0; i < clientsPerPriority; ++i) {
		defaultPriorityClients.push_back(mockClient(
		    &throttler, TransactionPriority::DEFAULT, tag, 1, throttlingRate / clientsPerPriority, &counters));
		lowPriorityClients.push_back(
		    mockClient(&throttler, TransactionPriority::LOW, tag, 1, throttlingRate / clientsPerPriority, &counters));
	}
	wait(timeout(server && waitForAll(defaultPriorityClients) && waitForAll(lowPriorityClients), duration, Void()));
	auto const defaultPriorityThroughput = counters.getCount(tag, TransactionPriority::DEFAULT);
	auto const lowPriorityThroughput = counters.getCount(tag, TransactionPriority::LOW);
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
