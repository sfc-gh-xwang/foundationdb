/**
 * RKThroughputTrackerTesting.actor.cpp
 */

#include "fdbserver/IRKClusterThroughputTracker.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

static bool isNear(double a, double b) {
	return std::abs(a - b) < 0.1 * std::max(std::abs(a), 1.0);
}

static ThrottlingId getThrottlingId(int index) {
	return ThrottlingIdRef::fromTag(BinaryWriter::toValue(index, Unversioned()).withPrefix("tag/"_sr));
}

// Simulate a workload being reported to the provided RKClientThroughputTracker
ACTOR static Future<Void> runWorkload(RKClientThroughputTracker* throughputTracker,
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

ACTOR static Future<Void> runWorkload(RKServerThroughputTracker* throughputTracker,
                                      double throughput,
                                      int numStorageServers,
                                      int numThrottlingIds,
                                      bool isRead) {
	state Map<UID, StorageQueueInfo> ssInfos;
	for (int i = 0; i < numStorageServers; ++i) {
		auto ssInfo = StorageQueueInfo::createTestInfo(UID(i, i), /*storageQueueBytes=*/0);
		for (int j = 0; j < numThrottlingIds; ++j) {
			if (isRead) {
				ssInfo.busiestReaders.emplace_back(getThrottlingId(j), throughput / numThrottlingIds);
			} else {
				ssInfo.busiestWriters.emplace_back(getThrottlingId(j), throughput / numThrottlingIds);
			}
		}
		ssInfos.insert(mapPair(ssInfo.id, ssInfo));
	}
	loop {
		throughputTracker->update(ssInfos);
		wait(delay(0.1));
	}
}

template <class T>
static bool checkThroughput(T const& throughputTracker, double desiredThroughput, int numThrottlingIds) {
	for (int i = 0; i < numThrottlingIds; ++i) {
		auto const throughput = throughputTracker.getThroughput(getThrottlingId(i));
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

TEST_CASE("/fdbserver/ratekeeper/RKClientThroughputTracker") {
	state double const throughput = 4096000.0;
	state int const numThrottlingIds = 5;
	state RKClientThroughputTracker throughputTracker(/*expirationCheckInterval=*/5.0);
	state Future<Void> workloadFuture = runWorkload(&throughputTracker, throughput, numThrottlingIds);
	while (!checkThroughput(throughputTracker, throughput, numThrottlingIds) &&
	       (throughputTracker.throttlingIdsTracked() != numThrottlingIds)) {
		wait(delay(1.0));
	}

	// Stop all workload, but continue reporting. Eventually,
	// the throughput tracker will report a throughput of 0 for all throttlingIds,
	// and stop tracking all thorttlingIds.
	workloadFuture = runWorkload(&throughputTracker, throughput, /*numThrottlingIds=*/0);
	while (!checkThroughput(throughputTracker, /*desiredThroughput=*/0.0, numThrottlingIds) &&
	       (throughputTracker.throttlingIdsTracked() > 0)) {
		wait(delay(1.0));
	}
	return Void();
}

TEST_CASE("/fdbserver/ratekeeper/RKServerThroughputTracker") {
	state double const throughput = 4096000.0;
	state int const numStorageServers = 10;
	state int const numThrottlingIds = 5;
	state RKServerThroughputTracker throughputTracker;

	state Future<Void> workloadFuture = runWorkload(&throughputTracker,
	                                                throughput,
	                                                numStorageServers,
	                                                numThrottlingIds,
	                                                /*isRead=*/deterministicRandom()->coinflip());
	while (!checkThroughput(throughputTracker, throughput, numThrottlingIds) ||
	       (throughputTracker.storageServersTracked() != numStorageServers) ||
	       (throughputTracker.throttlingIdsTracked() != numThrottlingIds)) {
		wait(delay(1.0));
	}

	// Stop all workload, but continue reporting. Eventually,
	// the throughput tracker will report a throughput of 0 for all throttlingIds,
	// and stop tracking all throttlingIds.
	workloadFuture = runWorkload(&throughputTracker,
	                             throughput,
	                             numStorageServers,
	                             /*numThrottlingIds=*/0,
	                             /*isRead=*/deterministicRandom()->coinflip());
	while (!checkThroughput(throughputTracker, /*desiredThroughput=*/0.0, numThrottlingIds) ||
	       (throughputTracker.throttlingIdsTracked() > 0)) {
		wait(delay(1.0));
	}

	// Now stop reporting from all storage servers. Eventually,
	// the throughput tracker will clean up its records of storage servers.
	workloadFuture = runWorkload(&throughputTracker,
	                             throughput,
	                             /*numStorageServer=*/0,
	                             /*numThrottlingIds=*/0,
	                             /*isRead=*/deterministicRandom()->coinflip());
	while (throughputTracker.storageServersTracked() > 0) {
		wait(delay(1.0));
	}
	ASSERT(checkThroughput(throughputTracker, /*desiredThroughput=*/0.0, numThrottlingIds));
	ASSERT_EQ(throughputTracker.throttlingIdsTracked(), 0);
	return Void();
}
