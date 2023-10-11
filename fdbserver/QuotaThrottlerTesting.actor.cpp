/**
 * QuotaThrottlerTesting.actor.cpp
 */

#include "fdbclient/Knobs.h"
#include "fdbclient/ThroughputTracker.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/QuotaThrottler.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include
#include "flow/genericactors.actor.h"

namespace {

enum class LimitType { RESERVED, TOTAL };
enum class OpType { READ, WRITE };

class WorkloadConfig {
	ThrottlingId throttlingId;
	int totalQuotaInPages;
	int reservedQuotaInPages;
	double attemptedTps;
	double pagesPerTransaction;
	OpType opType;

	WorkloadConfig(ThrottlingId const& throttlingId,
	               int totalQuotaInPages,
	               int reservedQuotaInPages,
	               double attemptedTps,
	               double pagesPerTransaction,
	               OpType opType)
	  : throttlingId(throttlingId),
	    totalQuotaInPages(totalQuotaInPages),
	    reservedQuotaInPages(reservedQuotaInPages),
	    attemptedTps(attemptedTps),
	    pagesPerTransaction(pagesPerTransaction),
	    opType(opType) {}

public:
	ThrottlingId getThrottlingId() const { return throttlingId; }

	int getTotalQuotaInPages() const { return totalQuotaInPages; }

	int getReservedQuotaInPages() const { return reservedQuotaInPages; }

	int getPagesPerTransaction() const { return pagesPerTransaction; }

	double getAttemptedTps() const { return attemptedTps; }

	OpType getOpType() const { return opType; }

	class Builder {
		ThrottlingId throttlingId;
		int totalQuotaInPages{ 100 };
		int reservedQuotaInPages{ 0 };
		double attemptedTps{ 5.0 };
		double pagesPerTransaction{ 6.0 };
		OpType opType{ OpType::READ };

	public:
		Builder() : throttlingId(ThrottlingIdRef::fromTag("sampleTag"_sr)) {}

		Builder& setThrottlingId(ThrottlingId const& throttlingId) {
			this->throttlingId = throttlingId;
			return *this;
		}
		Builder& setTotalQuotaInPages(int totalQuotaInPages) {
			this->totalQuotaInPages = totalQuotaInPages;
			return *this;
		}
		Builder& setReservedQuotaInPages(int reservedQuotaInPages) {
			this->reservedQuotaInPages = reservedQuotaInPages;
			return *this;
		}
		Builder& setAttemptedTps(double attemptedTps) {
			this->attemptedTps = attemptedTps;
			return *this;
		}
		Builder& setPagesPerTransaction(double pagesPerTransaction) {
			this->pagesPerTransaction = pagesPerTransaction;
			return *this;
		}
		Builder& setOpType(OpType opType) {
			this->opType = opType;
			return *this;
		}
		WorkloadConfig build() const {
			return WorkloadConfig(
			    throttlingId, totalQuotaInPages, reservedQuotaInPages, attemptedTps, pagesPerTransaction, opType);
		}
	};
};

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

class MockStorageServer {
	class Cost {
		HoltLinearSmoother smoother;

	public:
		Cost() : smoother(1.0, SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME) {}
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
			ThrottlingIdMap<ThroughputStats> newThroughput;
			newThroughput[throttlingId].readThroughput = cost;
			clientThroughputTracker.get().update(newThroughput);
		}
	}

	ACTOR static Future<Void> _runClient(TestEnv* self, WorkloadConfig config, std::vector<int> storageServerIndices) {
		state ThrottlingId throttlingId = config.getThrottlingId();
		loop {
			auto tpsLimit = self->getTPSLimit(throttlingId);
			state double enforcedRate = tpsLimit.present() ? std::min<double>(config.getAttemptedTps(), tpsLimit.get())
			                                               : config.getAttemptedTps();
			wait(delay(1 / enforcedRate));
			self->storageServers.addCost(
			    throttlingId, config.getPagesPerTransaction(), storageServerIndices, config.getOpType());
			self->quotaThrottler.addRequests(throttlingId, 1);
			self->updateThroughput(throttlingId, config.getPagesPerTransaction(), config.getOpType());
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

	TestEnv(int numSS,
	        int throughputPerSS,
	        int maxFallingBehind,
	        LimitingThrottlingRatioCalculator const& limitingThrottlingRatioCalculator)
	  : quotaThrottler(UID{},
	                   maxFallingBehind,
	                   /*limitingThreshold=*/CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE,
	                   limitingThrottlingRatioCalculator),
	    storageServers(numSS, throughputPerSS) {
		if (SERVER_KNOBS->USE_CLIENT_THROUGHPUT_TRACKER) {
			clientThroughputTracker = RKClientThroughputTracker(/*expirationCheckInterval=*/5.0);
		}
	}

public:
	class Builder {
		int numSS{ 10 };
		int pagesPerSecondPerSS{ 100 };
		int maxFallingBehind{ 0 };
		double limitingTargetThroughput{ 100e6 };
		double limitingSpringThroughput{ 20e6 };

	public:
		Builder& setNumSS(int numSS) {
			this->numSS = numSS;
			return *this;
		}
		Builder& setPagesPerSecondPerSS(int pagesPerSecondPerSS) {
			this->pagesPerSecondPerSS = pagesPerSecondPerSS;
			return *this;
		}
		Builder& setMaxFallingBehind(int maxFallingBehind) {
			this->maxFallingBehind = maxFallingBehind;
			return *this;
		}
		Builder& setLimitingThroughputParameters(double targetBps, double springBps) {
			this->limitingTargetThroughput = targetBps;
			this->limitingSpringThroughput = springBps;
			return *this;
		}
		TestEnv build() const {
			return TestEnv(numSS,
			               pagesPerSecondPerSS,
			               maxFallingBehind,
			               LimitingThrottlingRatioCalculator::Builder()
			                   .setThroughputParameters(limitingTargetThroughput, limitingSpringThroughput)
			                   .build());
		}
	};

	Optional<double> getTPSLimit(ThrottlingId const& throttlingId) {
		auto rates = getTargetRates();
		auto it = rates.find(throttlingId);
		if (it != rates.end()) {
			return it->second;
		}
		return {};
	}

	Future<Void> runClient(WorkloadConfig const& config,
	                       std::vector<int> const& storageServerIndices = std::vector<int>()) {
		return _runClient(this, config, storageServerIndices);
	}

	bool targetRateIsNear(ThrottlingId const& throttlingId, Optional<double> expected, bool shouldBeBusy = false) {
		Optional<double> rate;
		auto const targetRates = getTargetRates();
		auto it = targetRates.find(throttlingId);
		if (it != targetRates.end()) {
			rate = it->second;
		}

		bool isBusy = isThrottlingIdBusy(throttlingId);

		TraceEvent("QuotaThrottling_RateMonitor")
		    .detail("ThrottlingId", throttlingId)
		    .detail("CurrentTPSRate", rate)
		    .detail("ExpectedTPSRate", expected)
		    .detail("ShouldBeBusy", shouldBeBusy)
		    .detail("IsBusy", isBusy);

		if (isBusyThrottlingIdsTrackingEnabled()) {
			if (shouldBeBusy) {
				return isBusy && isNear(rate, expected);
			} else {
				// If the tag is not busy, then the target rate should not be present.
				return !isBusy && !rate.present();
			}
		} else {
			return isNear(rate, expected);
		}
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
		    .detail("ExpectedTotalTPSRate", expected);
		return isNear(targetRateSum, expected);
	}

	Future<Void> update() { return _update(this); }

	void setQuota(WorkloadConfig const& config) {
		quotaCache.setQuota(config.getThrottlingId(),
		                    config.getTotalQuotaInPages() * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE,
		                    config.getReservedQuotaInPages() * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
	}

	void removeQuota(WorkloadConfig const& config) { quotaCache.removeQuota(config.getThrottlingId()); }

	int throttlingIdsTracked() const { return quotaThrottler.throttlingIdsTracked(); }

	void removeExpiredThrottlingIds() { quotaThrottler.removeExpiredThrottlingIds(); }

	int throttleCount() const { return quotaThrottler.throttleCount(); }

	void setCapacity(int index, double pagesPerSecond) { storageServers.setCapacity(index, pagesPerSecond); }

	bool isThrottlingIdBusy(ThrottlingId const& throttlingId) {
		auto rates = getTargetRates();
		return rates.contains(throttlingId);
	}

	static double getTimeoutDuration() { return 600.0; }
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
	state TestEnv env = TestEnv::Builder().build();
	auto const config = WorkloadConfig::Builder().build();
	env.setQuota(config);
	state Future<Void> client = env.runClient(config);
	state Future<Void> monitor = monitorActor(&env, [config](auto& env) {
		return env.targetRateIsNear(config.getThrottlingId(),
		                            config.getTotalQuotaInPages() / config.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/false);
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 5 6-byte write transactions per second.
// Limit should adjust to allow 100/(6*<fungibility_ratio>) transactions per second.
// Client using more than 100% of its quota, should be tracked as busy and should be throttled.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/WriteThrottling") {
	state TestEnv env = TestEnv::Builder().build();
	auto const config = WorkloadConfig::Builder().setOpType(OpType::WRITE).build();
	env.setQuota(config);
	state Future<Void> client = env.runClient(config);
	state Future<Void> monitor = monitorActor(&env, [config](auto& env) {
		return env.targetRateIsNear(
		    config.getThrottlingId(),
		    config.getTotalQuotaInPages() /
		        (config.getPagesPerTransaction() * CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO),
		    /*shouldBeBusy=*/true);
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second for each tag.
// 2 clients each attempt 5 6-byte read transactions per second.
// Both limits should adjust to allow 100/6 transactions per second.
// Each client using about 30% of its quota, they are not busy and no target rate should be reported.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/MultiTagThrottling") {
	state TestEnv env = TestEnv::Builder().build();
	auto const config1 = WorkloadConfig::Builder().setThrottlingId(ThrottlingIdRef::fromTag("sampleTag1"_sr)).build();
	auto const config2 = WorkloadConfig::Builder().setThrottlingId(ThrottlingIdRef::fromTag("sampleTag1"_sr)).build();
	env.setQuota(config1);
	env.setQuota(config2);
	state std::vector<Future<Void>> futures;
	state std::vector<Future<Void>> monitorFutures;
	futures.push_back(env.runClient(config1));
	futures.push_back(env.runClient(config2));
	futures.push_back(env.update());
	state Future<Void> monitor = monitorActor(&env, [config1, config2](auto& env) {
		return env.targetRateIsNear(config1.getThrottlingId(),
		                            config1.getTotalQuotaInPages() / config1.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/false) &&
		       env.targetRateIsNear(config2.getThrottlingId(),
		                            config2.getTotalQuotaInPages() / config2.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/false);
	});
	wait(timeoutError(waitForAny(futures) || monitor, TestEnv::getTimeoutDuration()));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 20 10-page read transactions per second.
// Limit should adjust to allow 100/10 transactions per second.
// Client should be tracked as busy.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/AttemptWorkloadAboveQuota") {
	state TestEnv env = TestEnv::Builder().build();
	auto const config = WorkloadConfig::Builder().setAttemptedTps(20.0).setPagesPerTransaction(10.0).build();
	env.setQuota(config);
	state Future<Void> client = env.runClient(config);
	state Future<Void> monitor = monitorActor(&env, [config](auto& env) {
		return env.targetRateIsNear(config.getThrottlingId(),
		                            config.getTotalQuotaInPages() / config.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/true);
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
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
	state TestEnv env = TestEnv::Builder().build();
	auto const initialConfig = WorkloadConfig::Builder().build();
	env.setQuota(initialConfig);
	state Future<Void> client = env.runClient(initialConfig);
	state Future<Void> monitor = monitorActor(&env, [initialConfig](auto& env) {
		return env.targetRateIsNear(initialConfig.getThrottlingId(),
		                            initialConfig.getTotalQuotaInPages() / initialConfig.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/false);
	});

	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));

	auto const updatedConfig = WorkloadConfig::Builder().setTotalQuotaInPages(30).build();
	env.setQuota(updatedConfig);
	monitor = monitorActor(&env, [updatedConfig](auto& env) {
		return env.targetRateIsNear(updatedConfig.getThrottlingId(),
		                            updatedConfig.getTotalQuotaInPages() / updatedConfig.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/true);
	});
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
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
	state TestEnv env = TestEnv::Builder().build();
	state WorkloadConfig config = WorkloadConfig::Builder().build();
	env.setQuota(config);
	state Future<Void> client = env.runClient(config);
	auto const _config = config;
	state Future<Void> monitor = monitorActor(&env, [_config](auto& env) {
		return env.targetRateIsNear(
		    _config.getThrottlingId(), _config.getTotalQuotaInPages() / _config.getPagesPerTransaction(), false);
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
	env.removeQuota(config);
	auto const _config2 = config;
	monitor = monitorActor(&env, [_config2](auto& env) {
		return env.targetRateIsNear(_config2.getThrottlingId(), {}, /*shouldBeBusy=*/false);
	});
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota is initially set to 30 pages/second.
// Client attempts 5 6-page read transactions per second.
// Target limit adjusts to allow 30/6 transactions per second.
// Throttling ID should be reported as busy
// Then Quota is removed.
// Target limit is removed as a result. Throttling id should also be removed from the busy set.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/RemoveQuotaBusyId") {
	state TestEnv env = TestEnv::Builder().build();
	state WorkloadConfig config = WorkloadConfig::Builder().setTotalQuotaInPages(30).build();
	env.setQuota(config);
	state Future<Void> client = env.runClient(config);
	auto const _config = config;
	state Future<Void> monitor = monitorActor(&env, [_config](auto& env) {
		return env.targetRateIsNear(_config.getThrottlingId(),
		                            _config.getTotalQuotaInPages() / _config.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/true);
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
	env.removeQuota(config);
	auto const _config2 = config;
	monitor = monitorActor(&env, [_config2](auto& env) {
		return env.targetRateIsNear(_config2.getThrottlingId(), {}, /*shouldBeBusy=*/false);
	});
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
	return Void();
}

// 10 storage servers can handle 5 pages/second each.
// Total quota is either unset or set to > 50 pages/second.
// Client attempts 10 6-page transactions per second
// Target is adjusted to 50/6 transactions per second, to match the total capacity all storage servers.
// Throttling ID should be reported as busy.
ACTOR static Future<Void> testActiveThrottling(Optional<long> totalQuotaInPages = Optional<long>()) {
	state int numSS = 10;
	state double pagesPerSecondPerSS = 5.0;
	state TestEnv env = TestEnv::Builder().setNumSS(numSS).setPagesPerSecondPerSS(pagesPerSecondPerSS).build();
	auto configBuilder = WorkloadConfig::Builder().setAttemptedTps(10.0);
	if (totalQuotaInPages.present()) {
		configBuilder.setTotalQuotaInPages(totalQuotaInPages.get());
	}
	auto const config = configBuilder.build();
	if (totalQuotaInPages.present()) {
		ASSERT_GE(totalQuotaInPages.get(), config.getAttemptedTps() * pagesPerSecondPerSS);
		env.setQuota(config);
	}
	state Future<Void> client = env.runClient(config);
	auto const totalSSCapacity = numSS * pagesPerSecondPerSS;
	state Future<Void> monitor = monitorActor(&env, [totalSSCapacity, config](auto& env) {
		return env.targetRateIsNear(config.getThrottlingId(),
		                            totalSSCapacity / config.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/true) &&
		       env.throttleCount() == 1;
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
	return Void();
}

TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/ActiveThrottlingOnSQ") {
	wait(testActiveThrottling(/*totalQuotaInPages=*/100));
	return Void();
}

TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/ActiveThrottlingOnSQNoQuota") {
	wait(testActiveThrottling());
	return Void();
}

TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/ActiveThrottlingOnThroughput") {
	state double targetThroughputInPagesPerSecond = 100.0;
	state TestEnv env =
	    TestEnv::Builder()
	        .setMaxFallingBehind(0)
	        .setPagesPerSecondPerSS(100000)
	        .setLimitingThroughputParameters(targetThroughputInPagesPerSecond * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE,
	                                         50.0 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE)
	        .build();
	auto const pagesPerTransaction = 10;
	auto config = WorkloadConfig::Builder().setAttemptedTps(20.0).setPagesPerTransaction(pagesPerTransaction).build();
	state Future<Void> client = env.runClient(config, { 0 });
	auto const targetTps = targetThroughputInPagesPerSecond / pagesPerTransaction;
	state Future<Void> monitor = monitorActor(&env, [config, targetTps](auto& env) {
		return env.targetRateIsNear(config.getThrottlingId(), targetTps, /*shouldBeBusy=*/true) &&
		       env.throttleCount() == 1;
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
	return Void();
}

// 10 storage servers can handle 5 pages/second each.
// Total quota is set to 50 pages/second for one tag, 100 pages/second for another.
// For each tag, a client attempts to execute 10 6-page read transactions per second.
// Target rates are adjusted to utilize the full 50 pages/second capacity of the
// storage servers. The two tags' total target rate will stabilize at 50 / 6.0
// transactions per second.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/MultiTagActiveThrottling1") {
	state int numSS = 10;
	state int pagesPerSecondPerSS = 5;
	state TestEnv env = TestEnv::Builder().setNumSS(numSS).setPagesPerSecondPerSS(pagesPerSecondPerSS).build();
	auto const config1 = WorkloadConfig::Builder()
	                         .setThrottlingId(ThrottlingIdRef::fromTag("sampleTag1"_sr))
	                         .setTotalQuotaInPages(50)
	                         .setAttemptedTps(10.0)
	                         .build();
	auto const config2 = WorkloadConfig::Builder()
	                         .setThrottlingId(ThrottlingIdRef::fromTag("sampleTag2"_sr))
	                         .setTotalQuotaInPages(100)
	                         .setAttemptedTps(10.0)
	                         .build();
	env.setQuota(config1);
	env.setQuota(config2);
	std::vector<Future<Void>> futures;
	futures.push_back(env.runClient(config1));
	futures.push_back(env.runClient(config2));
	ASSERT_EQ(config1.getPagesPerTransaction(), config2.getPagesPerTransaction());
	auto const totalSSCapacity = numSS * pagesPerSecondPerSS;
	state Future<Void> monitor = monitorActor(&env, [totalSSCapacity, config1, config2](auto& env) {
		return env.totalTargetRateIsNear(totalSSCapacity / config1.getPagesPerTransaction()) &&
		       env.throttleCount() == 2 &&
		       // Both tags should be busy
		       (!isBusyThrottlingIdsTrackingEnabled() || (env.isThrottlingIdBusy(config1.getThrottlingId()) &&
		                                                  env.isThrottlingIdBusy(config2.getThrottlingId())));
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
	state int pagesPerSecondPerSS = 50;
	state TestEnv env = TestEnv::Builder().setNumSS(3).setPagesPerSecondPerSS(pagesPerSecondPerSS).build();
	auto config1 = WorkloadConfig::Builder()
	                   .setThrottlingId(ThrottlingIdRef::fromTag("sampleTag1"_sr))
	                   .setAttemptedTps(10.0)
	                   .build();
	auto config2 = WorkloadConfig::Builder()
	                   .setThrottlingId(ThrottlingIdRef::fromTag("sampleTag2"_sr))
	                   .setAttemptedTps(10.0)
	                   .build();
	env.setQuota(config1);
	env.setQuota(config2);
	std::vector<Future<Void>> futures;
	futures.push_back(env.runClient(config1, { 0, 1 }));
	futures.push_back(env.runClient(config2, { 1, 2 }));
	ASSERT_EQ(config1.getPagesPerTransaction(), config2.getPagesPerTransaction());
	auto const totalTargetRatePagesPerSecond = 2 * pagesPerSecondPerSS;
	state Future<Void> monitor = monitorActor(&env, [totalTargetRatePagesPerSecond, config1, config2](auto& env) {
		return env.totalTargetRateIsNear(totalTargetRatePagesPerSecond / config1.getPagesPerTransaction()) &&
		       env.throttleCount() == 2 &&
		       // Both tags should be busy
		       (!isBusyThrottlingIdsTrackingEnabled() || (env.isThrottlingIdBusy(config1.getThrottlingId()) &&
		                                                  env.isThrottlingIdBusy(config2.getThrottlingId())));
	});
	futures.push_back(env.update());
	wait(timeoutError(waitForAny(futures) || monitor, TestEnv::getTimeoutDuration()));
	return Void();
}

// 3 storage servers can handle 50 pages/second each.
// Total quota is set to 100 pages/second for each tag.
// One client attempts 10 6-page read transactions per second, all directed towards a single storage server.
// Another client, using a different tag, attempts 10 6-page read transactions split across the other two storage
// servers. Target rates adjust to 50/6 and 100/6 transactions per second for the two clients, based on the capacities
// of the storage servers being accessed.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/MultiTagActiveThrottling3") {
	state int pagesPerSecondPerSS = 50;
	state TestEnv env = TestEnv::Builder().setNumSS(3).setPagesPerSecondPerSS(pagesPerSecondPerSS).build();
	auto config1 = WorkloadConfig::Builder()
	                   .setThrottlingId(ThrottlingIdRef::fromTag("sampleTag1"_sr))
	                   .setAttemptedTps(10.0)
	                   .build();
	auto config2 = WorkloadConfig::Builder()
	                   .setThrottlingId(ThrottlingIdRef::fromTag("sampleTag2"_sr))
	                   .setAttemptedTps(10.0)
	                   .build();
	env.setQuota(config1);
	env.setQuota(config2);
	std::vector<Future<Void>> futures;
	futures.push_back(env.runClient(config1, { 0 }));
	futures.push_back(env.runClient(config2, { 1, 2 }));
	auto const _pagesPerSecondPerSS = pagesPerSecondPerSS;
	state Future<Void> monitor = monitorActor(&env, [_pagesPerSecondPerSS, config1, config2](auto& env) {
		return env.targetRateIsNear(config1.getThrottlingId(),
		                            _pagesPerSecondPerSS / config1.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/true) &&
		       env.targetRateIsNear(config2.getThrottlingId(),
		                            2 * _pagesPerSecondPerSS / config2.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/false) &&
		       env.throttleCount() == 1;
	});
	futures.push_back(env.update());
	wait(timeoutError(waitForAny(futures) || monitor, TestEnv::getTimeoutDuration()));
	return Void();
}

// 10 storage servers can serve 5 pages/second each.
// Total quota is set to 100 pages/second.
// Reserved quota is set to 70 pages/second.
// A client attempts to execute 10 6-page read transactions per second.
// Despite the storage server only having capacity to serve 50/6 transactions per second,
// the reserved quota will ensure the target rate adjusts to 70/6 transactions per second.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/ReservedQuota") {
	state TestEnv env = TestEnv::Builder().setPagesPerSecondPerSS(5).build();
	auto const config = WorkloadConfig::Builder().setReservedQuotaInPages(70).setAttemptedTps(10.0).build();
	env.setQuota(config);
	state Future<Void> client = env.runClient(config);
	state Future<Void> monitor = monitorActor(&env, [config](auto& env) {
		return env.targetRateIsNear(config.getThrottlingId(),
		                            config.getReservedQuotaInPages() / config.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/true);
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
	return Void();
}

// Test that tags are expired iff a sufficient amount of time has passed since the
// last transaction with that tag, tag should also be removed from the busy set.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/ExpireTags") {
	state TestEnv env = TestEnv::Builder().setPagesPerSecondPerSS(5).build();
	state WorkloadConfig config = WorkloadConfig::Builder().setTotalQuotaInPages(30).setAttemptedTps(10.0).build();

	env.setQuota(config);

	state Future<Void> client = timeout(env.runClient(config), 60.0, Void());
	state Future<Void> updater = timeout(env.update(), 60.0, Void());
	wait(client && updater);
	client.cancel();
	updater.cancel();
	ASSERT_EQ(env.throttlingIdsTracked(), 1);
	if (isBusyThrottlingIdsTrackingEnabled()) {
		ASSERT(env.isThrottlingIdBusy(config.getThrottlingId()));
	}

	env.removeExpiredThrottlingIds();
	ASSERT_EQ(env.throttlingIdsTracked(), 1);
	if (isBusyThrottlingIdsTrackingEnabled()) {
		ASSERT(env.isThrottlingIdBusy(config.getThrottlingId()));
	}

	wait(delay(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER + 1.0));
	ASSERT_EQ(env.throttlingIdsTracked(), 1);
	if (isBusyThrottlingIdsTrackingEnabled()) {
		// Client has been stopped, no throughput for a while. ID should no longer be busy.
		ASSERT(!env.isThrottlingIdBusy(config.getThrottlingId()));
	}

	env.removeExpiredThrottlingIds();
	ASSERT_EQ(env.throttlingIdsTracked(), 0);
	if (isBusyThrottlingIdsTrackingEnabled()) {
		ASSERT(!env.isThrottlingIdBusy(config.getThrottlingId()));
	}
	return Void();
}

// Test that the number of tags tracked does not grow beyond SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/TagLimit") {
	state TestEnv env = TestEnv::Builder().setPagesPerSecondPerSS(5).build();
	std::vector<Future<Void>> futures;
	for (int i = 0; i < 2 * SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED; ++i) {
		Arena arena;
		TransactionTag tag = makeString(8, arena);
		deterministicRandom()->randomBytes(mutateString(tag), tag.size());
		auto const config = WorkloadConfig::Builder().setThrottlingId(ThrottlingIdRef::fromTag(tag)).build();
		futures.push_back(env.runClient(config));
	}
	wait(timeout(waitForAll(futures), 60.0, Void()));
	ASSERT_EQ(env.throttlingIdsTracked(), SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED);
	return Void();
}

// 9 storage servers can handle 100 pages/second each.
// 1 unhealthy storage server can only handle 5 pages/second.
// Total quota is set to 100 pages/second.
// Client attempts 10 6-page transactions per second.
// Target rate adjusts to 100/6 transactions per second, ignoring the worst storage server.
// Then, a second storage server becomes unhealthy and can only handle 5 pages/second.
// Target rate adjusts down to 50/6 transactions per second, because only one bad zone can be ignored.
TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/IgnoreWorstZone") {
	state int numSS = 10;
	state TestEnv env = TestEnv::Builder().setNumSS(numSS).setMaxFallingBehind(1).build();
	state int limitingPagesPerSecond = 5;
	state WorkloadConfig config = WorkloadConfig::Builder().setAttemptedTps(10.0).build();
	env.setCapacity(/*index=*/0, limitingPagesPerSecond);
	env.setQuota(config);
	state Future<Void> client = env.runClient(config);
	auto const _config = config;
	state Future<Void> monitor = monitorActor(&env, [_config](auto& env) {
		return env.targetRateIsNear(_config.getThrottlingId(),
		                            _config.getTotalQuotaInPages() / _config.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/false);
	});
	state Future<Void> updater = env.update();
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
	env.setCapacity(/*index=*/1, limitingPagesPerSecond);
	double const _numSS = numSS;
	double const _limitingPagesPerSecond = limitingPagesPerSecond;
	auto const _config2 = config;
	monitor = monitorActor(&env, [_numSS, _limitingPagesPerSecond, _config2](auto& env) {
		return env.targetRateIsNear(_config2.getThrottlingId(),
		                            (_numSS * _limitingPagesPerSecond) / _config2.getPagesPerTransaction(),
		                            /*shouldBeBusy=*/true);
	});
	wait(timeoutError(monitor || client || updater, TestEnv::getTimeoutDuration()));
	return Void();
}

TEST_CASE("/fdbserver/ratekeeper/QuotaThrottler/TrackBusyIDs") {
	state TestEnv env = TestEnv::Builder().build();
	state int totalQuotaInPages = 300;
	auto const initialConfig =
	    WorkloadConfig::Builder().setTotalQuotaInPages(totalQuotaInPages).setAttemptedTps(5.0).build();

	// Only run this unit test if busy throttling id tracking is enabled.
	if (!isBusyThrottlingIdsTrackingEnabled()) {
		return Void();
	}

	env.setQuota(initialConfig); // 50 TPS effective quota
	state Future<Void> updater = env.update();

	// 10% < Forget threshold, tag should not be tracked as busy.
	wait(timeoutError(env.runClient(initialConfig) ||
	                      monitorActor(&env,
	                                   [initialConfig](auto& env) {
		                                   return env.targetRateIsNear(
		                                       initialConfig.getThrottlingId(), {}, /*shouldBeBusy=*/false);
	                                   }),
	                  TestEnv::getTimeoutDuration()));

	// Forget threshold < 60% < Busy threshold, tag should not be tracked as busy.
	auto const lowWorkloadConfig = WorkloadConfig::Builder().setAttemptedTps(30.0).build();
	wait(timeoutError(env.runClient(lowWorkloadConfig) ||
	                      monitorActor(&env,
	                                   [lowWorkloadConfig](auto& env) {
		                                   return env.targetRateIsNear(
		                                       lowWorkloadConfig.getThrottlingId(), {}, /*shouldBeBusy=*/false);
	                                   }),
	                  TestEnv::getTimeoutDuration()));

	// Busy threshold < 100%, tag should be tracked as busy
	auto const highWorkloadConfig = WorkloadConfig::Builder().setAttemptedTps(50.0).build();
	int const _totalQuotaInPages = totalQuotaInPages;
	wait(timeoutError(env.runClient(highWorkloadConfig) ||
	                      monitorActor(&env,
	                                   [_totalQuotaInPages, highWorkloadConfig](auto& env) {
		                                   return env.targetRateIsNear(highWorkloadConfig.getThrottlingId(),
		                                                               _totalQuotaInPages /
		                                                                   highWorkloadConfig.getPagesPerTransaction(),
		                                                               /*shouldBeBusy=*/true);
	                                   }),
	                  TestEnv::getTimeoutDuration()));

	// Forget threshold < 70% < Busy threshold, tag below busy threshold, but above forget threshold
	auto const stillRememberedConfig = WorkloadConfig::Builder().setAttemptedTps(35.0).build();
	int const _totalQuotaInPages2 = totalQuotaInPages;
	wait(timeoutError(env.runClient(stillRememberedConfig) ||
	                      monitorActor(&env,
	                                   [_totalQuotaInPages2, stillRememberedConfig](auto& env) {
		                                   return env.targetRateIsNear(
		                                       stillRememberedConfig.getThrottlingId(),
		                                       _totalQuotaInPages2 / stillRememberedConfig.getPagesPerTransaction(),
		                                       /*shouldBeBusy=*/true);
	                                   }),
	                  TestEnv::getTimeoutDuration()));

	// 40% < Forget threshold, less than forget threshold, should be no longer be tracked as busy
	auto const forgottenConfig = WorkloadConfig::Builder().setAttemptedTps(20.0).build();
	wait(timeoutError(env.runClient(forgottenConfig) ||
	                      monitorActor(&env,
	                                   [forgottenConfig](auto& env) {
		                                   return env.targetRateIsNear(
		                                       forgottenConfig.getThrottlingId(), {}, /*shouldBeBusy=*/false);
	                                   }),
	                  TestEnv::getTimeoutDuration()));

	return Void();
}
