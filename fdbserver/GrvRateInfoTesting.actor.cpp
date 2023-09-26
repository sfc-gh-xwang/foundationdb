/**
 * GrvRateInfoTesting.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbserver/GrvRateInfo.h"
#include "fdbserver/Knobs.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

bool isNear(double desired, int64_t actual) {
	return std::abs(desired - actual) * 10 < desired;
}

struct MockClient {
	int reqsToRelease;
	double rate;
	Future<Void> reqGenerator;

	ACTOR static Future<Void> generateRequests(MockClient* self) {
		loop {
			wait(delayJittered(1.0 / self->rate));
			++(self->reqsToRelease);
		}
	}

public:
	MockClient(double rate) : reqsToRelease(0), rate(rate) {}

	void start() { reqGenerator = generateRequests(this); }

	void releaseReqs(int count) {
		ASSERT_GE(reqsToRelease, count);
		reqsToRelease -= count;
	}

	bool hasReqs(int count) { return reqsToRelease >= count; }

	bool isEmpty() const { return (reqsToRelease == 0); }
};

ACTOR static Future<Void> runThrottlingTest(GrvRateInfo* rateInfo, MockClient* client, int64_t* counter) {
	state int batchSize = deterministicRandom()->randomInt(1, 11);
	loop {
		state double elapsed = 0.1 * deterministicRandom()->random01();
		wait(delay(elapsed));
		rateInfo->startReleaseWindow();
		int numReleased = 0;
		while (client->hasReqs(batchSize) && rateInfo->canStart(numReleased, /*count=*/batchSize)) {
			client->releaseReqs(batchSize);
			numReleased += batchSize;
		}
		*counter += numReleased;
		rateInfo->endReleaseWindow(numReleased, client->isEmpty(), elapsed);
	}
}

} // namespace

// Rate limit set at 10, but client attempts 20 transactions per second.
// Client should be throttled to only 10 transactions per second.
TEST_CASE("/GrvRateInfo/Simple") {
	state double testDuration = 60.0;
	state double desiredRate = 20.0;
	state double throttledRate = 10.0;
	state MockClient client(desiredRate);
	state GrvRateInfo rateInfo(
	    /*rateWindow=*/2.0, /*maxEmptyQueueBudget=*/100, throttledRate, /*maxToStart=*/100000);
	state int64_t counter = 0;

	client.start();
	wait(timeout(runThrottlingTest(&rateInfo, &client, &counter), testDuration, Void()));
	TraceEvent("GrvRateInfoSimpleTest").detail("Counter", counter);
	ASSERT(isNear(testDuration * throttledRate, counter));
	return Void();
}

// Rate limit set at 30, but client only attempts 20 transactions per second.
// Client should be unthrottled, and achieve its attempted throughput.
TEST_CASE("/GrvRateInfo/Unthrottled") {
	state double testDuration = 60.0;
	state double desiredRate = 20.0;
	state double throttledRate = 30.0;
	state MockClient client(desiredRate);
	state GrvRateInfo rateInfo(
	    /*rateWindow=*/2.0, /*maxEmptyQueueBudget=*/100, throttledRate, /*maxToStart=*/100000);
	state int64_t counter = 0;

	client.start();
	rateInfo.setRate(throttledRate);
	wait(timeout(runThrottlingTest(&rateInfo, &client, &counter), testDuration, Void()));
	TraceEvent("GrvRateInfoUnthrottledTest").detail("Counter", counter);
	ASSERT(isNear(testDuration * desiredRate, counter));
	return Void();
}

// Rate limit set at 10, but client attempts 20 transactions per second.
// The maximum empty queue budget is set to 100 transactions, and the
// client waits 20 seconds before starting, allowing the maximum budget
// to fill up. Therefore, the client is able to achieve 100 + 10 * 60 =
// 700 transactions in a minute.
TEST_CASE("/GrvRateInfo/MaxEmptyQueueBudget") {
	state double testDuration = 60.0;
	state double desiredRate = 20.0;
	state double throttledRate = 10.0;
	state int maxEmptyQueueBudget = 100;
	state MockClient client(desiredRate);
	state GrvRateInfo rateInfo(
	    /*rateWindow=*/2.0, maxEmptyQueueBudget, throttledRate, /*maxToStart=*/100000);
	state int64_t counter = 0;
	state Future<Void> testFuture = runThrottlingTest(&rateInfo, &client, &counter);

	// Wait for budget to fill up
	wait(delay(2 * maxEmptyQueueBudget / throttledRate));

	client.start();
	wait(timeout(testFuture, testDuration, Void()));
	TraceEvent("GrvRateInfoMaxEmptyQueueBudgetTest").detail("Counter", counter);
	ASSERT(isNear(maxEmptyQueueBudget + testDuration * throttledRate, counter));
	return Void();
}
