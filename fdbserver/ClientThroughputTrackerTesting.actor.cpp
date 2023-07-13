/**
 * ClientThroughputTrackerTesting.actor.cpp
 */

#include "fdbserver/IRKThroughputTracker.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

static bool isNear(double a, double b) {
	return std::abs(a - b) < 0.1 * std::max(std::abs(a), 1.0);
}

static ThrottlingId getThrottlingId(int index) {
	return ThrottlingIdRef::fromTag(BinaryWriter::toValue(index, Unversioned()).withPrefix("tag/"_sr));
}

// Simulate a workload being reported to the provided ClientThroughputTracker
ACTOR static Future<Void> runWorkload(ClientThroughputTracker* throughputTracker,
                                      double throughput,
                                      int numThrottlingIds) {
	state ThrottlingIdMap<uint64_t> throughputPerInterval;
	for (int i = 0; i < numThrottlingIds; ++i) {
		throughputPerInterval[getThrottlingId(i)] = static_cast<uint64_t>(throughput * 0.1);
	}
	loop {
		throughputTracker->update(throughputPerInterval);
		wait(delay(0.1));
	}
}

static bool checkThroughput(ClientThroughputTracker* throughputTracker,
                            double desiredThroughput,
                            int numThrottlingIds) {
	for (int i = 0; i < numThrottlingIds; ++i) {
		auto const throughput = throughputTracker->getThroughput(getThrottlingId(i));
		if (!isNear(desiredThroughput, throughput)) {
			TraceEvent("CheckThroughputMismatch")
			    .detail("Index", i)
			    .detail("DesiredThroughput", desiredThroughput)
			    .detail("Throughput", throughput);
			return false;
		}
	}
	return true;
}

TEST_CASE("/fdbserver/ClientThroughputTracker") {
	state double const throughput = 4096000;
	state int const numThrottlingIds = 10;
	state ClientThroughputTracker throughputTracker(/*expirationCheckInterval=*/5.0);
	state Future<Void> workloadFuture = runWorkload(&throughputTracker, throughput, numThrottlingIds);
	while (!checkThroughput(&throughputTracker, throughput, numThrottlingIds)) {
		wait(delay(1.0));
	}
	workloadFuture = runWorkload(&throughputTracker, throughput, 0);
	while (!checkThroughput(&throughputTracker, 0.0, numThrottlingIds)) {
		wait(delay(1.0));
	}
	return Void();
}
