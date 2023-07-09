/**
 * QuotaThrottlerTesting.actor.cpp
 */

#include "fdbserver/Knobs.h"
#include "fdbserver/QuotaThrottler.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

enum class LimitType { RESERVED, TOTAL };
enum class OpType { READ, WRITE };

Optional<double> getTPSLimit(QuotaThrottler& quotaThrottler,
                             IRKThroughputQuotaCache const& quotaCache,
                             IRKMetricsTracker const& metricsTracker,
                             ThrottlingId throttlingId) {
	auto rates = quotaThrottler.getProxyRates(/*numProxies=*/1, quotaCache, metricsTracker);
	auto it = rates.find(throttlingId);
	if (it != rates.end()) {
		return it->second;
	}
	return {};
}

class MockStorageServer {
	class Cost {
		HoltLinearSmoother smoother;

	public:
		Cost() : smoother(1.0) {}
		Cost& operator+=(double delta) {
			smoother.addDelta(delta);
			return *this;
		}
		double smoothRate() const { return smoother.smoothRate(); }
	};

	UID id;
	// pages/second that this storage server can handle
	double capacity;
	ThrottlingIdMap<Cost> readCosts, writeCosts;
	Cost totalReadCost, totalWriteCost;

public:
	explicit MockStorageServer(UID id, double capacity) : id(id), capacity(capacity) { ASSERT_GT(capacity, 0); }
	void addReadCost(ThrottlingId throttlingId, double cost) {
		readCosts[throttlingId] += cost;
		totalReadCost += cost;
	}
	void addWriteCost(ThrottlingId throttlingId, double cost) {
		writeCosts[throttlingId] += cost;
		totalWriteCost += cost;
	}

	void setCapacity(double pagesPerSecond) { capacity = pagesPerSecond; }

	StorageQueueInfo getStorageQueueInfo() const {
		StorageQueueInfo result(id, LocalityData({}, Value(id.toString()), {}, {}));
		for (const auto& [throttlingId, readCost] : readCosts) {
			result.busiestReaders.emplace_back(throttlingId, readCost.smoothRate());
		}
		for (const auto& [throttlingId, writeCost] : writeCosts) {
			result.busiestWriters.emplace_back(throttlingId, writeCost.smoothRate());
		}
		result.lastReply.bytesInput = ((totalReadCost.smoothRate() + totalWriteCost.smoothRate()) /
		                               (capacity * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE)) *
		                              SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES;
		result.valid = true;
		return result;
	}
};

class StorageServerCollection {
	std::vector<MockStorageServer> storageServers;

public:
	StorageServerCollection(size_t size, double capacity) {
		ASSERT_GT(size, 0);
		storageServers.reserve(size);
		for (int i = 0; i < size; ++i) {
			storageServers.emplace_back(UID(i, i), capacity);
		}
	}

	void addCost(ThrottlingId throttlingId,
	             double pagesPerSecond,
	             std::vector<int> const& storageServerIndices,
	             OpType opType) {
		if (storageServerIndices.empty()) {
			auto costPerSS = CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE * (pagesPerSecond / storageServers.size());
			if (opType == OpType::WRITE) {
				costPerSS *= CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO;
			}
			for (auto& storageServer : storageServers) {
				if (opType == OpType::READ) {
					storageServer.addReadCost(throttlingId, costPerSS);
				} else {
					storageServer.addWriteCost(throttlingId, costPerSS);
				}
			}
		} else {
			auto const costPerSS =
			    CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE * (pagesPerSecond / storageServerIndices.size());
			for (auto i : storageServerIndices) {
				if (opType == OpType::READ) {
					storageServers[i].addReadCost(throttlingId, costPerSS);
				} else {
					storageServers[i].addWriteCost(throttlingId, costPerSS);
				}
			}
		}
	}

	void setCapacity(int index, double pagesPerSecond) { storageServers[index].setCapacity(pagesPerSecond); }

	Map<UID, StorageQueueInfo> getStorageQueueInfos() const {
		Map<UID, StorageQueueInfo> result;
		for (const auto& storageServer : storageServers) {
			auto const sqInfo = storageServer.getStorageQueueInfo();
			result.insert(mapPair(sqInfo.id, sqInfo));
		}
		return result;
	}
};

void updateThroughput(QuotaThrottler& quotaThrottler,
                      ThrottlingId const& throttlingId,
                      double numPages,
                      OpType opType) {
	auto cost = numPages * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	if (opType == OpType::WRITE) {
		cost *= CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO;
	}
	ThrottlingIdMap<uint64_t> newThroughput;
	newThroughput[throttlingId] = cost;
	quotaThrottler.updateThroughput(newThroughput);
}

ACTOR Future<Void> runClient(QuotaThrottler* quotaThrottler,
                             IRKThroughputQuotaCache const* quotaCache,
                             IRKMetricsTracker const* metricsTracker,
                             StorageServerCollection* storageServers,
                             ThrottlingId throttlingId,
                             double tpsRate,
                             double costPerTransaction,
                             OpType opType,
                             std::vector<int> storageServerIndices = std::vector<int>()) {
	loop {
		auto tpsLimit = getTPSLimit(*quotaThrottler, *quotaCache, *metricsTracker, throttlingId);
		state double enforcedRate = tpsLimit.present() ? std::min<double>(tpsRate, tpsLimit.get()) : tpsRate;
		wait(delay(1 / enforcedRate));
		storageServers->addCost(throttlingId, costPerTransaction, storageServerIndices, opType);
		quotaThrottler->addRequests(throttlingId, 1);
		updateThroughput(*quotaThrottler, throttlingId, costPerTransaction, opType);
	}
}

ACTOR template <class Check>
Future<Void> monitorActor(QuotaThrottler* quotaThrottler,
                          MockRKThroughputQuotaCache const* quotaCache,
                          MockRKMetricsTracker const* metricsTracker,
                          Check check) {
	state int successes = 0;
	loop {
		wait(delay(1.0));
		if (check(*quotaThrottler, *quotaCache, *metricsTracker)) {
			// Wait for 10 consecutive successes so we're certain
			// than a stable equilibrium has been reached
			if (++successes == 10) {
				return Void();
			}
		} else {
			successes = 0;
		}
	}
}

bool isNear(double a, double b) {
	return abs(a - b) < 0.1 * a;
}

bool isNear(Optional<double> a, Optional<double> b) {
	if (a.present()) {
		return b.present() && isNear(a.get(), b.get());
	} else {
		return !b.present();
	}
}

bool targetRateIsNear(QuotaThrottler& quotaThrottler,
                      IRKThroughputQuotaCache const& quotaCache,
                      IRKMetricsTracker const& metricsTracker,
                      ThrottlingId throttlingId,
                      Optional<double> expected) {
	Optional<double> rate;
	auto targetRates = quotaThrottler.getProxyRates(/*numProxies=*/1, quotaCache, metricsTracker);
	auto it = targetRates.find(throttlingId);
	if (it != targetRates.end()) {
		rate = it->second;
	}
	TraceEvent("QuotaThrottling_RateMonitor")
	    .detail("ThrottlingId", throttlingId)
	    .detail("CurrentTPSRate", rate)
	    .detail("ExpectedTPSRate", expected);
	return isNear(rate, expected);
}

ACTOR Future<Void> updateQuotaThrottler(MockRKMetricsTracker* metricsTracker,
                                        QuotaThrottler* quotaThrottler,
                                        StorageServerCollection const* storageServers) {
	loop {
		wait(delay(1.0));
		auto const storageQueueInfos = storageServers->getStorageQueueInfos();
		quotaThrottler->updateThrottling(storageQueueInfos);
		for (auto it = storageQueueInfos.begin(); it != storageQueueInfos.end(); ++it) {
			metricsTracker->updateStorageQueueInfo(it->value);
		}
	}
}

} // namespace

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 5 6-byte read transactions per second.
// Limit should adjust to allow 100/6 transactions per second.
TEST_CASE("/QuotaThrottler/Simple") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client =
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(
	    &quotaThrottler, &quotaCache, &metricsTracker, [testTag](auto& qt, auto const& qc, auto const& mt) {
		    return targetRateIsNear(qt, qc, mt, testTag, 100.0 / 6.0);
	    });
	state Future<Void> updater = updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 5 6-byte write transactions per second.
// Limit should adjust to allow 100/(6*<fungibility_ratio>) transactions per second.
TEST_CASE("/QuotaThrottler/WriteThrottling") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client =
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag, 5.0, 6.0, OpType::WRITE);
	state Future<Void> monitor = monitorActor(
	    &quotaThrottler, &quotaCache, &metricsTracker, [testTag](auto& qt, auto const& qc, auto const& mt) {
		    return targetRateIsNear(
		        qt, qc, mt, testTag, 100.0 / (6.0 * CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO));
	    });

	state Future<Void> updater = updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second for each tag.
// 2 clients each attempt 5 6-byte read transactions per second.
// Both limits should adjust to allow 100/6 transactions per second.
TEST_CASE("/QuotaThrottler/MultiTagThrottling") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	auto testTag1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	auto testTag2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	quotaCache.setQuota(testTag1, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	quotaCache.setQuota(testTag2, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state std::vector<Future<Void>> futures;
	state std::vector<Future<Void>> monitorFutures;
	futures.push_back(
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag1, 5.0, 6.0, OpType::READ));
	futures.push_back(
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag2, 5.0, 6.0, OpType::READ));
	futures.push_back(updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers));
	state Future<Void> monitor = monitorActor(
	    &quotaThrottler, &quotaCache, &metricsTracker, [testTag1, testTag2](auto& qt, auto const& qc, auto const& mt) {
		    return targetRateIsNear(qt, qc, mt, testTag1, 100.0 / 6.0) &&
		           targetRateIsNear(qt, qc, mt, testTag2, 100.0 / 6.0);
	    });
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 20 10-page read transactions per second.
// Limit should adjust to allow 100/10 transactions per second.
TEST_CASE("/QuotaThrottler/AttemptWorkloadAboveQuota") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client =
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag, 20.0, 10.0, OpType::READ);
	state Future<Void> monitor = monitorActor(
	    &quotaThrottler, &quotaCache, &metricsTracker, [testTag](auto& qt, auto const& qc, auto const& mt) {
		    return targetRateIsNear(qt, qc, mt, testTag, 10.0);
	    });
	state Future<Void> updater = updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota is initially set to 100 pages/second.
// Client attempts 5 6-page transactions per second.
// Test that the tag throttler can reach equilibrium, then adjust to a new equilibrium once the quota is changed
// Target rate should adjust to allow 100/6 transactions per second.
// Total quota is modified to 50 pages/second.
// Target rate should adjust to allow 50/6 transactions per second.
TEST_CASE("/QuotaThrottler/UpdateQuota") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	state ThrottlingId testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client =
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor =
	    monitorActor(&quotaThrottler, &quotaCache, &metricsTracker, [](auto& qt, auto const& qc, auto const& mt) {
		    return targetRateIsNear(qt, qc, mt, ThrottlingIdRef::fromTag("sampleTag1"_sr), 100.0 / 6.0);
	    });
	state Future<Void> updater = updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	quotaCache.setQuota(testTag, 50 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	monitor = monitorActor(&quotaThrottler, &quotaCache, &metricsTracker, [](auto& qt, auto const& qc, auto const& mt) {
		return targetRateIsNear(qt, qc, mt, ThrottlingIdRef::fromTag("sampleTag1"_sr), 50.0 / 6.0);
	});
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota is initially set to 100 pages/second.
// Client attempts 5 6-page read transactions per second.
// Target limit adjusts to allow 100/6 transactions per second.
// Then Quota is removed.
// Target limit is removed as a result.
TEST_CASE("/QuotaThrottler/RemoveQuota") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	state ThrottlingId testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client =
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor =
	    monitorActor(&quotaThrottler, &quotaCache, &metricsTracker, [](auto& qt, auto const& qc, auto const& mt) {
		    return targetRateIsNear(qt, qc, mt, ThrottlingIdRef::fromTag("sampleTag1"_sr), 100.0 / 6.0);
	    });
	state Future<Void> updater = updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	quotaCache.removeQuota(testTag);
	monitor = monitorActor(&quotaThrottler, &quotaCache, &metricsTracker, [](auto& qt, auto const& qc, auto const& mt) {
		return targetRateIsNear(qt, qc, mt, ThrottlingIdRef::fromTag("sampleTag1"_sr), {});
	});
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 5 pages/second each.
// Total quota is set to 100 pages/second.
// Client attempts 10 6-page transactions per second
// Target is adjusted to 50/6 transactions per second, to match the total capacity all storage servers.
TEST_CASE("/QuotaThrottler/ActiveThrottling") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client =
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag, 10.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(
	    &quotaThrottler, &quotaCache, &metricsTracker, [testTag](auto& qt, auto const& qc, auto const& mt) {
		    return targetRateIsNear(qt, qc, mt, testTag, 50 / 6.0) && qt.throttleCount() == 1;
	    });
	state Future<Void> updater = updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 5 pages/second each.
// Total quota is set to 50 pages/second for one tag, 100 pages/second for another.
// For each tag, a client attempts to execute 10 6-page read transactions per second.
// Target rates are adjusted to utilize the full 50 pages/second capacity of the
//   add storage servers. The two tags receive this capacity with a 2:1 ratio,
//   matching the ratio of their total quotas.
TEST_CASE("/QuotaThrottler/MultiTagActiveThrottling") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	auto testTag1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	auto testTag2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	quotaCache.setQuota(testTag1, 50 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	quotaCache.setQuota(testTag2, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	std::vector<Future<Void>> futures;
	futures.push_back(
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag1, 10.0, 6.0, OpType::READ));
	futures.push_back(
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag2, 10.0, 6.0, OpType::READ));
	state Future<Void> monitor = monitorActor(
	    &quotaThrottler, &quotaCache, &metricsTracker, [testTag1, testTag2](auto& qt, auto const& qc, auto const& mt) {
		    return targetRateIsNear(qt, qc, mt, testTag1, (50 / 6.0) / 3) &&
		           targetRateIsNear(qt, qc, mt, testTag2, 2 * (50 / 6.0) / 3) && qt.throttleCount() == 2;
	    });
	futures.push_back(updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 3 storage servers can handle 50 pages/second each.
// Total quota is set to 100 pages/second for each tag.
// Each client attempts 10 6-page read transactions per second.
// This workload is sent to 2 storage servers per client (with an overlap of one storage server).
// Target rates for both tags are adjusted to 50/6 transactions per second to match the throughput
//   that the busiest server can handle.
TEST_CASE("/QuotaThrottler/MultiTagActiveThrottling2") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(3, 50);
	auto testTag1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	auto testTag2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	quotaCache.setQuota(testTag1, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	quotaCache.setQuota(testTag2, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	std::vector<Future<Void>> futures;
	futures.push_back(runClient(
	    &quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag1, 10.0, 6.0, OpType::READ, { 0, 1 }));
	futures.push_back(runClient(
	    &quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag2, 10.0, 6.0, OpType::READ, { 1, 2 }));
	state Future<Void> monitor = monitorActor(
	    &quotaThrottler, &quotaCache, &metricsTracker, [testTag1, testTag2](auto& qt, auto const& qc, auto const& mt) {
		    return targetRateIsNear(qt, qc, mt, testTag1, 50 / 6.0) &&
		           targetRateIsNear(qt, qc, mt, testTag2, 50 / 6.0) && qt.throttleCount() == 2;
	    });
	futures.push_back(updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 3 storage servers can handle 50 pages/second each.
// Total quota is set to 100 pages/second for each tag.
// One client attempts 10 6-page read transactions per second, all directed towards a single storage server.
// Another client, using a different tag, attempts 10 6-page read transactions split across the other two storage
// servers. Target rates adjust to 50/6 and 100/6 transactions per second for the two clients, based on the capacities
// of the
//   storage servers being accessed.
TEST_CASE("/QuotaThrottler/MultiTagActiveThrottling3") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(3, 50);
	auto testTag1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	auto testTag2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	quotaCache.setQuota(testTag1, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	quotaCache.setQuota(testTag2, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	std::vector<Future<Void>> futures;
	futures.push_back(runClient(
	    &quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag1, 10.0, 6.0, OpType::READ, { 0 }));
	futures.push_back(runClient(
	    &quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag2, 10.0, 6.0, OpType::READ, { 1, 2 }));
	state Future<Void> monitor = monitorActor(
	    &quotaThrottler, &quotaCache, &metricsTracker, [testTag1, testTag2](auto& qt, auto const& qc, auto const& mt) {
		    return targetRateIsNear(qt, qc, mt, testTag1, 50 / 6.0) &&
		           targetRateIsNear(qt, qc, mt, testTag2, 100 / 6.0) && qt.throttleCount() == 1;
	    });
	futures.push_back(updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 10 storage servers can serve 5 pages/second each.
// Total quota is set to 100 pages/second.
// Reserved quota is set to 70 pages/second.
// A client attempts to execute 10 6-page read transactions per second.
// Despite the storage server only having capacity to serve 50/6 transactions per second,
//   the reserved quota will ensure the target rate adjusts to 70/6 transactions per second.
TEST_CASE("/QuotaThrottler/ReservedQuota") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(
	    testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 70 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
	state Future<Void> client =
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag, 10.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(
	    &quotaThrottler, &quotaCache, &metricsTracker, [testTag](auto& qt, auto const& qc, auto const& mt) {
		    return targetRateIsNear(qt, qc, mt, testTag, 70 / 6.0);
	    });
	state Future<Void> updater = updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// Test that tags are expired iff a sufficient amount of time has passed since the
// last transaction with that tag
TEST_CASE("/QuotaThrottler/ExpireTags") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);

	state Future<Void> client = timeout(
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag, 10.0, 6.0, OpType::READ),
	    60.0,
	    Void());
	state Future<Void> updater =
	    timeout(updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers), 60.0, Void());
	wait(client && updater);
	client.cancel();
	updater.cancel();
	ASSERT_EQ(quotaThrottler.throttlingIdsTracked(), 1);
	quotaThrottler.removeExpiredThrottlingIds();
	ASSERT_EQ(quotaThrottler.throttlingIdsTracked(), 1);
	wait(delay(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER + 1.0));
	ASSERT_EQ(quotaThrottler.throttlingIdsTracked(), 1);
	quotaThrottler.removeExpiredThrottlingIds();
	ASSERT_EQ(quotaThrottler.throttlingIdsTracked(), 0);
	return Void();
}

// Test that the number of tags tracked does not grow beyond SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED
TEST_CASE("/QuotaThrottler/TagLimit") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	std::vector<Future<Void>> futures;
	for (int i = 0; i < 2 * SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED; ++i) {
		Arena arena;
		TransactionTag tag = makeString(8, arena);
		deterministicRandom()->randomBytes(mutateString(tag), tag.size());
		futures.push_back(runClient(&quotaThrottler,
		                            &quotaCache,
		                            &metricsTracker,
		                            &storageServers,
		                            ThrottlingIdRef::fromTag(tag),
		                            1.0,
		                            6.0,
		                            OpType::READ));
	}
	wait(timeout(waitForAll(futures), 60.0, Void()));
	ASSERT_EQ(quotaThrottler.throttlingIdsTracked(), SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED);
	return Void();
}

// 9 storage servers can handle 100 pages/second each.
// 1 unhealthy storage server can only handle 1 page/second.
// Total quota is set to 100 pages/second.
// Client attempts 5 6-page transactions per second.
// Target rate adjusts to 100/6 transactions per second, ignoring the worst storage server.
// Then, a second storage server becomes unhealthy and can only handle 1 page/second.
// Target rate adjusts down to 10/6 transactions per second, because only one bad zone can be ignored.
TEST_CASE("/QuotaThrottler/IgnoreWorstZone") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state QuotaThrottler quotaThrottler(UID{}, 1);
	state StorageServerCollection storageServers(10, 100);
	state ThrottlingId testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	storageServers.setCapacity(/*index=*/0, /*pagesPerSecond=*/1);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client =
	    runClient(&quotaThrottler, &quotaCache, &metricsTracker, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor =
	    monitorActor(&quotaThrottler, &quotaCache, &metricsTracker, [](auto& qt, auto const& qc, auto const& mt) {
		    return targetRateIsNear(qt, qc, mt, ThrottlingIdRef::fromTag("sampleTag1"_sr), 100.0 / 6.0);
	    });
	state Future<Void> updater = updateQuotaThrottler(&metricsTracker, &quotaThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	storageServers.setCapacity(/*index=*/1, /*pagesPerSecond=*/1);
	monitor = monitorActor(&quotaThrottler, &quotaCache, &metricsTracker, [](auto& qt, auto const& qc, auto const& mt) {
		return targetRateIsNear(qt, qc, mt, ThrottlingIdRef::fromTag("sampleTag1"_sr), 10.0 / 6.0);
	});
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}
