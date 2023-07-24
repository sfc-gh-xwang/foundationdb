/**
 * QuotaThrottlerTesting.actor.cpp
 */

#include "fdbclient/Knobs.h"
#include "fdbclient/ThroughputTracker.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/QuotaThrottler.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

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

enum class LimitType { RESERVED, TOTAL };
enum class OpType { READ, WRITE };

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
		auto result = StorageQueueInfo::createTestInfo(id,
		                                               ((totalReadCost.smoothRate() + totalWriteCost.smoothRate()) /
		                                                (capacity * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE)) *
		                                                   SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES);
		for (const auto& [throttlingId, readCost] : readCosts) {
			result.busiestReaders.emplace_back(throttlingId, readCost.smoothRate());
		}
		for (const auto& [throttlingId, writeCost] : writeCosts) {
			result.busiestWriters.emplace_back(throttlingId, writeCost.smoothRate());
		}
		return result;
	}
}; // class MockStorageServer

class StorageServerCollection {
	std::vector<MockStorageServer> storageServers;

public:
	StorageServerCollection(size_t size, double throughputPerSS) {
		ASSERT_GT(size, 0);
		storageServers.reserve(size);
		for (int i = 0; i < size; ++i) {
			storageServers.emplace_back(UID(i, i), throughputPerSS);
		}
	}

	void addCost(ThrottlingId throttlingId,
	             double numPages,
	             std::vector<int> const& storageServerIndices,
	             OpType opType) {
		if (storageServerIndices.empty()) {
			auto costPerSS = CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE * (numPages / storageServers.size());
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
			auto const costPerSS = CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE * (numPages / storageServerIndices.size());
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
}; // class StorageServerCollection

class TestEnv {
	MockRKThroughputQuotaCache quotaCache;
	MockRKMetricsTracker metricsTracker;
	Optional<RKClientThroughputTracker> clientThroughputTracker;
	RKServerThroughputTracker serverThroughputTracker;
	QuotaThrottler quotaThrottler;
	StorageServerCollection storageServers;

	void updateThroughput(ThrottlingId const& throttlingId, double numPages, OpType opType) {
		auto cost = numPages * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
		if (opType == OpType::WRITE) {
			cost *= CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO;
		}
		if (clientThroughputTracker.present()) {
			ThrottlingIdMap<uint64_t> newThroughput;
			newThroughput[throttlingId] = cost;
			clientThroughputTracker.get().update(newThroughput);
		}
	}

	ACTOR static Future<Void> _runClient(TestEnv* self,
	                                     ThrottlingId throttlingId,
	                                     double tps,
	                                     double pagesPerTransaction,
	                                     OpType opType,
	                                     std::vector<int> storageServerIndices) {
		loop {
			auto tpsLimit = self->getTPSLimit(throttlingId);
			state double enforcedRate = tpsLimit.present() ? std::min<double>(tps, tpsLimit.get()) : tps;
			wait(delay(1 / enforcedRate));
			self->storageServers.addCost(throttlingId, pagesPerTransaction, storageServerIndices, opType);
			self->quotaThrottler.addRequests(throttlingId, 1);
			self->updateThroughput(throttlingId, pagesPerTransaction, opType);
		}
	}

	ACTOR static Future<Void> _update(TestEnv* self) {
		loop {
			wait(delay(1.0));
			auto const storageQueueInfos = self->storageServers.getStorageQueueInfos();
			self->serverThroughputTracker.update(storageQueueInfos);
			for (auto it = storageQueueInfos.begin(); it != storageQueueInfos.end(); ++it) {
				self->metricsTracker.updateStorageQueueInfo(it->value);
			}
		}
	}

	ThrottlingIdMap<double> getTargetRates() {
		if (clientThroughputTracker.present()) {
			return quotaThrottler.getTargetRates(
			    /*numProxies=*/1, quotaCache, metricsTracker, clientThroughputTracker.get(), serverThroughputTracker);
		} else {
			return quotaThrottler.getTargetRates(
			    /*numProxies=*/1, quotaCache, metricsTracker, serverThroughputTracker, serverThroughputTracker);
		}
	}

public:
	TestEnv(int numSS, int throughputPerSS, int maxFallingBehind = 0)
	  : quotaThrottler(UID{}, maxFallingBehind, /*limitingThreshold=*/CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE),
	    storageServers(numSS, throughputPerSS) {
		if (CLIENT_KNOBS->TRACK_THROUGHPUT_ON_CLIENTS) {
			clientThroughputTracker = RKClientThroughputTracker(/*expirationCheckInterval=*/5.0);
		}
	}

	Optional<double> getTPSLimit(ThrottlingId const& throttlingId) {
		auto rates = getTargetRates();
		auto it = rates.find(throttlingId);
		if (it != rates.end()) {
			return it->second;
		}
		return {};
	}

	Future<Void> runClient(ThrottlingId const& throttlingId,
	                       double tps,
	                       double pagesPerTransaction,
	                       OpType opType,
	                       std::vector<int> const& storageServerIndices = std::vector<int>()) {
		return _runClient(this, throttlingId, tps, pagesPerTransaction, opType, storageServerIndices);
	}

	bool targetRateIsNear(ThrottlingId const& throttlingId, Optional<double> expected) {
		Optional<double> rate;
		auto const targetRates = getTargetRates();
		auto it = targetRates.find(throttlingId);
		if (it != targetRates.end()) {
			rate = it->second;
		}
		TraceEvent("QuotaThrottling_RateMonitor")
		    .detail("ThrottlingId", throttlingId)
		    .detail("CurrentTPSRate", rate)
		    .detail("ExpectedTPSRate", expected)
		    .detail("Busy", isThrottlingIdBusy(throttlingId));
		return isNear(rate, expected);
	}

	bool totalTargetRateIsNear(double expected) {
		auto const targetRates = getTargetRates();
		double targetRateSum = 0.0;
		for (auto const& [_, targetRate] : targetRates) {
			targetRateSum += targetRate;
		}
		TraceEvent("QuotaThrottling_TotalRateMonitor")
		    .detail("NumThrottlingIds", targetRates.size())
		    .detail("CurrentTotalTPSRate", targetRateSum)
		    .detail("ExpectedTotalTPSRate", expected)
		    .detail("BusySet", quotaThrottler.getBusyThrottlingIds().size());
		return isNear(targetRateSum, expected);
	}

	Future<Void> update() { return _update(this); }

	void setQuota(ThrottlingId const& throttlingId, double totalQuotaInPages, double reservedQuotaInPages = 0.0) {
		quotaCache.setQuota(throttlingId,
		                    totalQuotaInPages * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE,
		                    reservedQuotaInPages * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
	}

	void removeQuota(ThrottlingId const& throttlingId) { quotaCache.removeQuota(throttlingId); }

	int throttlingIdsTracked() const { return quotaThrottler.throttlingIdsTracked(); }

	void removeExpiredThrottlingIds() { quotaThrottler.removeExpiredThrottlingIds(); }

	int throttleCount() const { return quotaThrottler.throttleCount(); }

	void setCapacity(int index, double pagesPerSecond) { storageServers.setCapacity(index, pagesPerSecond); }

	bool isThrottlingIdBusy(ThrottlingId const& throttlingId) {
		return quotaThrottler.getBusyThrottlingIds().contains(throttlingId);
	}
}; // class TestEnv

ACTOR template <class Check>
Future<Void> monitorActor(TestEnv* env, Check check) {
	state int successes = 0;
	loop {
		wait(delay(1.0));
		if (check(*env)) {
			// Wait for 10 consecutive successes so we're certain
			// that a stable equilibrium has been reached
			if (++successes == 10) {
				return Void();
			}
		} else {
			successes = 0;
		}
	}
}

} // namespace

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 5 6-byte read transactions per second.
// Limit should adjust to allow 100/6 transactions per second.
// Client is only using about 30% of its quota, not busy.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/Simple") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/100);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	env.setQuota(testTag, /*totalQuotaInPages=*/100);
	state Future<Void> client = env.runClient(testTag, /*tps=*/5.0, /*pagesPerTransaction=*/6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&env, [testTag](auto& env) {
		if (isBusyThrottlingIDsTrackingEnabled()) {
			return !env.isThrottlingIdBusy(testTag);
		} else {
			return env.targetRateIsNear(testTag, 100.0 / 6.0);
		}
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 5 6-byte write transactions per second.
// Limit should adjust to allow 100/(6*<fungibility_ratio>) transactions per second.
// Client using more than 100% of its quota, should be tracked as busy and should be throttled.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/WriteThrottling") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/100);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	env.setQuota(testTag, /*totalQuotaInPages=*/100);
	state Future<Void> client = env.runClient(testTag, /*tps=*/5.0, /*pagesPerTransaction=*/6.0, OpType::WRITE);
	state Future<Void> monitor = monitorActor(&env, [testTag](auto& env) {
		return env.targetRateIsNear(testTag,
		                            100.0 / (6.0 * CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO)) &&
		       (!isBusyThrottlingIDsTrackingEnabled() || env.isThrottlingIdBusy(testTag));
	});

	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second for each tag.
// 2 clients each attempt 5 6-byte read transactions per second.
// Both limits should adjust to allow 100/6 transactions per second.
// Each client using about 30% of its quota, they are not busy and no target rate should be reported.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/MultiTagThrottling") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/100);
	auto testTag1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	auto testTag2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	env.setQuota(testTag1, /*totalQuotaInPages=*/100);
	env.setQuota(testTag2, /*totalQuotaInPages=*/100);
	state std::vector<Future<Void>> futures;
	state std::vector<Future<Void>> monitorFutures;
	futures.push_back(env.runClient(testTag1, /*tps=*/5.0, /*pagesPerTransaction=*/6.0, OpType::READ));
	futures.push_back(env.runClient(testTag2, /*tps=*/5.0, /*pagesPerTransaction=*/6.0, OpType::READ));
	futures.push_back(env.update());
	state Future<Void> monitor = monitorActor(&env, [testTag1, testTag2](auto& env) {
		if (isBusyThrottlingIDsTrackingEnabled()) {
			return !env.isThrottlingIdBusy(testTag1) && !env.isThrottlingIdBusy(testTag2) &&
			       env.targetRateIsNear(testTag1, {}) && env.targetRateIsNear(testTag2, {});
		} else {
			return env.targetRateIsNear(testTag1, 100.0 / 6.0) && env.targetRateIsNear(testTag2, 100.0 / 6.0);
		}
	});

	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 20 10-page read transactions per second.
// Limit should adjust to allow 100/10 transactions per second.
// Client should be tracked as busy.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/AttemptWorkloadAboveQuota") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/100);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	env.setQuota(testTag, /*totalQuotaInPages=*/100);
	state Future<Void> client = env.runClient(testTag, /*tps=*/20.0, /*pagesPerTransaction=*/10.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&env, [testTag](auto& env) {
		return env.targetRateIsNear(testTag, 10.0) &&
		       (!isBusyThrottlingIDsTrackingEnabled() || env.isThrottlingIdBusy(testTag));
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota is initially set to 100 pages/second.
// Client attempts 5 6-page transactions per second.
// Test that the tag throttler can reach equilibrium, then adjust to a new equilibrium once the quota is changed
// Target rate should adjust to allow 100/6 transactions per second.
// Roughly 30% quota usage, should not be reported as busy.
// Total quota is modified to 30 pages/second.
// Target rate should adjust to allow 30/6 transactions per second.
// Throttling ID should now be tracked as busy.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/UpdateQuota") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/100);
	state ThrottlingId testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	env.setQuota(testTag, /*totalQuotaInPages=*/100);
	state Future<Void> client = env.runClient(testTag, /*tps=*/5.0, /*pagesPerTransaction=*/6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&env, [](auto& env) {
		auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
		if (isBusyThrottlingIDsTrackingEnabled()) {
			return !env.isThrottlingIdBusy(testTag) && env.targetRateIsNear(testTag, {});
		} else {
			return env.targetRateIsNear(testTag, 100.0 / 6.0);
		}
	});

	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, 600.0));
	env.setQuota(testTag, /*totalQuotaInPages=*/30);
	monitor = monitorActor(&env, [](auto& env) {
		auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
		return env.targetRateIsNear(testTag, 30.0 / 6.0) &&
		       (!isBusyThrottlingIDsTrackingEnabled() || env.isThrottlingIdBusy(testTag));
	});
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota is initially set to 100 pages/second.
// Client attempts 5 6-page read transactions per second.
// Target limit adjusts to allow 100/6 transactions per second.
// Throttling ID not considered busy, and target rate will not be reported.
// Then Quota is removed.
// Target limit is removed as a result.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/RemoveQuota") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/100);
	state ThrottlingId testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	env.setQuota(testTag, /*totalQuotaInPages=*/100);
	state Future<Void> client = env.runClient(testTag, /*tps=*/5.0, /*pagesPerTransaction=*/6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&env, [](auto& env) {
		auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
		if (isBusyThrottlingIDsTrackingEnabled()) {
			return !env.isThrottlingIdBusy(testTag) && env.targetRateIsNear(testTag, {});
		} else {
			return env.targetRateIsNear(testTag, 100.0 / 6.0);
		}
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, 600.0));
	env.removeQuota(testTag);
	monitor = monitorActor(&env, [](auto& env) {
		auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
		return env.targetRateIsNear(testTag, {}) &&
		       (!isBusyThrottlingIDsTrackingEnabled() || !env.isThrottlingIdBusy(testTag));
	});
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota is initially set to 30 pages/second.
// Client attempts 5 6-page read transactions per second.
// Target limit adjusts to allow 30/6 transactions per second.
// Throttling ID should be reported as busy
// Then Quota is removed.
// Target limit is removed as a result. Throttling id should also be removed from the busy set.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/RemoveQuota2") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/100);
	state ThrottlingId testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	env.setQuota(testTag, /*totalQuotaInPages=*/30);
	state Future<Void> client = env.runClient(testTag, /*tps=*/5.0, /*pagesPerTransaction=*/6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&env, [](auto& env) {
		auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
		if (isBusyThrottlingIDsTrackingEnabled()) {
			return env.isThrottlingIdBusy(testTag) && env.targetRateIsNear(testTag, 30.0 / 6.0);
		} else {
			return env.targetRateIsNear(testTag, 30.0 / 6.0);
		}
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, 600.0));
	env.removeQuota(testTag);
	monitor = monitorActor(&env, [](auto& env) {
		auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
		return env.targetRateIsNear(testTag, {}) &&
		       (!isBusyThrottlingIDsTrackingEnabled() || !env.isThrottlingIdBusy(testTag));
	});
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 5 pages/second each.
// Total quota is set to 100 pages/second.
// Client attempts 10 6-page transactions per second
// Target is adjusted to 50/6 transactions per second, to match the total capacity all storage servers.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/ActiveThrottling") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/5);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	env.setQuota(testTag, /*totalQuotaInPages=*/100);
	state Future<Void> client = env.runClient(testTag, /*tps=*/10.0, /*pagesPerTransaction=*/6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&env, [testTag](auto& env) {
		return env.targetRateIsNear(testTag, 50 / 6.0) && env.throttleCount() == 1 &&
		       (!isBusyThrottlingIDsTrackingEnabled() || env.isThrottlingIdBusy(testTag));
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 5 pages/second each.
// Total quota is set to 50 pages/second for one tag, 100 pages/second for another.
// For each tag, a client attempts to execute 10 6-page read transactions per second.
// Target rates are adjusted to utilize the full 50 pages/second capacity of the
// storage servers. The two tags' total target rate will stabilize at 50 / 6.0
// transactions per second.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/MultiTagActiveThrottling1") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/5);
	auto testTag1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	auto testTag2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	env.setQuota(testTag1, /*totalQuotaInPages=*/50);
	env.setQuota(testTag2, /*totalQuotaInPages=*/100);
	std::vector<Future<Void>> futures;
	futures.push_back(env.runClient(testTag1, /*tps=*/10.0, /*pagesPerTransaction=*/6.0, OpType::READ));
	futures.push_back(env.runClient(testTag2, /*tps=*/10.0, /*pagesPerTransaction=*/6.0, OpType::READ));
	state Future<Void> monitor = monitorActor(&env, [testTag1, testTag2](auto& env) {
		return env.totalTargetRateIsNear(50 / 6.0) && env.throttleCount() == 2 &&
		       // Both tags should be busy
		       (!isBusyThrottlingIDsTrackingEnabled() ||
		        (env.isThrottlingIdBusy(testTag1) && env.isThrottlingIdBusy(testTag2)));
	});
	futures.push_back(env.update());
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 3 storage servers can handle 50 pages/second each.
// Total quota is set to 100 pages/second for each tag.
// Each client attempts 10 6-page read transactions per second.
// This workload is sent to 2 storage servers per client (with an overlap of one storage server).
// Target rates for both tags are adjusted to 50/6 transactions per second to match the throughput
//   that the busiest server can handle.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/MultiTagActiveThrottling2") {
	state TestEnv env(/*numSS=*/3, /*throughputPerSS=*/50);
	auto testTag1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	auto testTag2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	env.setQuota(testTag1, /*totalQuotaInPages=*/100);
	env.setQuota(testTag2, /*totalQuotaInPages=*/100);
	std::vector<Future<Void>> futures;
	futures.push_back(env.runClient(testTag1, /*tps=*/10.0, /*pagesPerTransaction=*/6.0, OpType::READ, { 0, 1 }));
	futures.push_back(env.runClient(testTag2, /*tps=*/10.0, /*pagesPerTransaction=*/6.0, OpType::READ, { 1, 2 }));
	state Future<Void> monitor = monitorActor(&env, [testTag1, testTag2](auto& env) {
		return env.totalTargetRateIsNear(100 / 6.0) && env.throttleCount() == 2 &&
		       // Both tags should be busy
		       (!isBusyThrottlingIDsTrackingEnabled() ||
		        (env.isThrottlingIdBusy(testTag1) && env.isThrottlingIdBusy(testTag2)));
	});
	futures.push_back(env.update());
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 3 storage servers can handle 50 pages/second each.
// Total quota is set to 100 pages/second for each tag.
// One client attempts 10 6-page read transactions per second, all directed towards a single storage server.
// Another client, using a different tag, attempts 10 6-page read transactions split across the other two storage
// servers. Target rates adjust to 50/6 and 100/6 transactions per second for the two clients, based on the capacities
// of the storage servers being accessed.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/MultiTagActiveThrottling3") {
	state TestEnv env(/*numSS=*/3, /*throughputPerSS=*/50);
	auto testTag1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	auto testTag2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	env.setQuota(testTag1, /*totalQuotaInPages=*/100);
	env.setQuota(testTag2, /*totalQuotaInPages=*/100);
	std::vector<Future<Void>> futures;
	futures.push_back(env.runClient(testTag1, /*tps=*/10.0, /*pagesPerTransaction=*/6.0, OpType::READ, { 0 }));
	futures.push_back(env.runClient(testTag2, /*tps=*/10.0, /*pagesPerTransaction=*/6.0, OpType::READ, { 1, 2 }));
	state Future<Void> monitor = monitorActor(&env, [testTag1, testTag2](auto& env) {
		if (isBusyThrottlingIDsTrackingEnabled()) {
			return env.targetRateIsNear(testTag1, 50 / 6.0) && env.throttleCount() == 1 &&
			       env.isThrottlingIdBusy(testTag1) && !env.isThrottlingIdBusy(testTag2);
		} else {
			return env.targetRateIsNear(testTag1, 50 / 6.0) && env.targetRateIsNear(testTag2, 100 / 6.0) &&
			       env.throttleCount() == 1;
		}
	});
	futures.push_back(env.update());
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 10 storage servers can serve 5 pages/second each.
// Total quota is set to 100 pages/second.
// Reserved quota is set to 70 pages/second.
// A client attempts to execute 10 6-page read transactions per second.
// Despite the storage server only having capacity to serve 50/6 transactions per second,
// the reserved quota will ensure the target rate adjusts to 70/6 transactions per second.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/ReservedQuota") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/5);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	env.setQuota(testTag, /*totalQuotaInPages=*/100, /*reservedQuotaInPages=*/70);
	state Future<Void> client = env.runClient(testTag, /*tps=*/10.0, /*pagesPerTransaction=*/6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&env, [testTag](auto& env) {
		return env.targetRateIsNear(testTag, 70 / 6.0) &&
		       (!isBusyThrottlingIDsTrackingEnabled() || env.isThrottlingIdBusy(testTag));
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can serve 5 pages/second each.
// Total quota is set to 100 pages/second.
// Reserved quota is set to 70 pages/second.
// A client attempts to execute 10 6-page read transactions per second.
// Despite the storage server only having capacity to serve 50/6 transactions per second,
// the reserved quota will ensure the target rate adjusts to 70/6 transactions per second.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/ReservedQuota") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/5);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	env.setQuota(testTag, /*totalQuotaInPages=*/100, /*reservedQuotaInPages=*/70);
	state Future<Void> client = env.runClient(testTag, /*tps=*/10.0, /*pagesPerTransaction=*/6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&env, [testTag](auto& env) {
		return env.targetRateIsNear(testTag, 70 / 6.0) &&
		       (!isBusyThrottlingIDsTrackingEnabled() || env.isThrottlingIdBusy(testTag));
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// Test that tags are expired iff a sufficient amount of time has passed since the
// last transaction with that tag
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/ExpireTags") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/5);
	state ThrottlingId testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);

	env.setQuota(testTag, /*totalQuotaInPages=*/30);

	state Future<Void> client =
	    timeout(env.runClient(testTag, /*tps=*/10.0, /*pagesPerTransaction=*/6.0, OpType::READ), 60.0, Void());
	state Future<Void> updater = timeout(env.update(), 60.0, Void());
	wait(client && updater);
	client.cancel();
	updater.cancel();
	ASSERT_EQ(env.throttlingIdsTracked(), 1);
	ASSERT(!isBusyThrottlingIDsTrackingEnabled() || env.isThrottlingIdBusy(testTag));

	env.removeExpiredThrottlingIds();
	ASSERT_EQ(env.throttlingIdsTracked(), 1);
	ASSERT(!isBusyThrottlingIDsTrackingEnabled() || env.isThrottlingIdBusy(testTag));

	wait(delay(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER + 1.0));
	ASSERT_EQ(env.throttlingIdsTracked(), 1);
	ASSERT(!isBusyThrottlingIDsTrackingEnabled() || env.isThrottlingIdBusy(testTag));

	env.removeExpiredThrottlingIds();
	ASSERT_EQ(env.throttlingIdsTracked(), 0);
	ASSERT(!isBusyThrottlingIDsTrackingEnabled() || !env.isThrottlingIdBusy(testTag));
	return Void();
}

// Test that the number of tags tracked does not grow beyond SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/TagLimit") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/5);
	std::vector<Future<Void>> futures;
	for (int i = 0; i < 2 * SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED; ++i) {
		Arena arena;
		TransactionTag tag = makeString(8, arena);
		deterministicRandom()->randomBytes(mutateString(tag), tag.size());
		futures.push_back(env.runClient(ThrottlingIdRef::fromTag(tag),
		                                /*tps=*/1.0,
		                                /*pagesPerTransaction=*/6.0,
		                                OpType::READ));
	}
	wait(timeout(waitForAll(futures), 60.0, Void()));
	ASSERT_EQ(env.throttlingIdsTracked(), SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED);
	return Void();
}

// 9 storage servers can handle 100 pages/second each.
// 1 unhealthy storage server can only handle 1 page/second.
// Total quota is set to 100 pages/second.
// Client attempts 5 6-page transactions per second.
// Target rate adjusts to 100/6 transactions per second, ignoring the worst storage server.
// Then, a second storage server becomes unhealthy and can only handle 1 page/second.
// Target rate adjusts down to 10/6 transactions per second, because only one bad zone can be ignored.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/IgnoreWorstZone") {
	state TestEnv env(/*numSS=*/10, /*throughputPerSS=*/100, /*maxFallingBehind=*/1);
	state ThrottlingId testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	env.setCapacity(/*index=*/0, /*pagesPerSecond=*/1);
	env.setQuota(testTag, /*totalQuotaInPages=*/100);
	state Future<Void> client = env.runClient(testTag, /*tps=*/5.0, /*pagesPerTransaction=*/6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&env, [](auto& env) {
		auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);

		if (isBusyThrottlingIDsTrackingEnabled()) {
			return !env.isThrottlingIdBusy(testTag);
		} else {
			return env.targetRateIsNear(testTag, 100.0 / 6.0);
		}
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, 600.0));
	env.setCapacity(/*index=*/1, /*pagesPerSecond=*/1);
	monitor = monitorActor(&env, [](auto& env) {
		auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
		return env.targetRateIsNear(testTag, 10.0 / 6.0) &&
		       (!isBusyThrottlingIDsTrackingEnabled() || env.isThrottlingIdBusy(testTag));
	});
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}
