/**
 * ExpectStableThroughput.actor.cpp
 */

#include "fdbclient/Knobs.h"
#include "fdbclient/ThroughputTracker.h"
#include "fdbserver/workloads/ThroughputQuotaUtil.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/actorcompiler.h" // This must be the last include

// This workload is meant to be run with the ThroughputQuotaWorklaod.
// The ThroughputQuotaWorkload sets a total quota, and then this workload runs
// with tagged transactions for a long duration, attempting to achieve a higher
// throughput than the specified quota. The check phase of this workload then
// verifies that the achieved throughput is near the total quota.
//
// TODO:
//   - Test multi-page operations
//   - Test using tenant groups rather than tags
class ExpectStableThroughputWorkload : public TestWorkload {
	// Metrics:
	struct ThrottlingMetrics {
		HoltLinearSmoother totalCost;
		HoltLinearSmoother throttledDuration;
		HoltLinearSmoother tagThrottledErrors;
		ThrottlingMetrics() : totalCost(1.0, 5.0), throttledDuration(1.0, 5.0), tagThrottledErrors(1.0, 5.0) {}
	};
	class PrioritizedThrottlingMetrics {
		ThrottlingMetrics defaultPriorityMetrics, lowPriorityMetrics;

	public:
		ThrottlingMetrics& getMetrics(TransactionPriority priority) & {
			bool const defaultPriority = (priority == TransactionPriority::DEFAULT);
			return defaultPriority ? defaultPriorityMetrics : lowPriorityMetrics;
		}
		ThrottlingMetrics const& getMetrics(TransactionPriority priority) const& {
			bool const defaultPriority = (priority == TransactionPriority::DEFAULT);
			return defaultPriority ? defaultPriorityMetrics : lowPriorityMetrics;
		}
		uint64_t getTotalCost() const {
			return defaultPriorityMetrics.totalCost.getTotal() + lowPriorityMetrics.totalCost.getTotal();
		}
	};
	boost::unordered_flat_map<TransactionTag, PrioritizedThrottlingMetrics> metricsMap;

	// Parameters:
	double testDuration;
	uint64_t expectedThroughputPagesRate;
	uint64_t attemptedThroughputPagesRate;
	TransactionTag tagPrefix;
	int numTags;
	double errorTolerance;
	Key keyPrefix;
	int defaultPriorityActors;
	int lowPriorityActors;
	double warmupTime;
	int opsPerTransaction;
	bool provideExpectedCost;
	bool useGrvCache;
	enum OpType {
		READ,
		SET,
		CLEAR,
	} opType;

	double startTime;

	std::string getOpTypeStr() const {
		switch (opType) {
		case OpType::READ:
			return "READ";
		case OpType::SET:
			return "SET";
		case OpType::CLEAR:
			return "CLEAR";
		default:
			ASSERT(false);
			return "";
		}
	}

	void finishTransaction(Transaction const& tr,
	                       TransactionTag const& tag,
	                       TransactionPriority priority,
	                       bool isTagThrottled = false) {
		if (now() > startTime + warmupTime) {
			auto& metrics = metricsMap[tag].getMetrics(priority);
			metrics.totalCost.addDelta(tr.getTotalCost());
			metrics.throttledDuration.addDelta(tr.getTagThrottledDuration());
			if (isTagThrottled) {
				metrics.tagThrottledErrors.addDelta(1.0);
			}
		}
	}

	void traceParameters(TraceEvent& te) {
		te.detail("TestDuration", testDuration)
		    .detail("TagPrefix", tagPrefix)
		    .detail("NumTags", numTags)
		    .detail("ExpectedThroughputPagesRate", expectedThroughputPagesRate)
		    .detail("AttemptedThroughputPagesRate", attemptedThroughputPagesRate)
		    .detail("ErrorTolerance", errorTolerance)
		    .detail("KeyPrefix", keyPrefix)
		    .detail("DefaultPriorityActors", defaultPriorityActors)
		    .detail("LowPriorityActors", lowPriorityActors)
		    .detail("WarmupTime", warmupTime)
		    .detail("OpsPerTranaction", opsPerTransaction)
		    .detail("ProvideExpectedCost", provideExpectedCost)
		    .detail("OpType", getOpTypeStr())
		    .detail("UseGrvCache", useGrvCache);
	}

	ACTOR static Future<Void> loggerActor(ExpectStableThroughputWorkload* self) {
		loop {
			wait(delay(5.0));
			for (auto const& [tag, prioritizedMetrics] : self->metricsMap) {
				auto const& lowPriorityMetrics = prioritizedMetrics.getMetrics(TransactionPriority::LOW);
				auto const& defaultPriorityMetrics = prioritizedMetrics.getMetrics(TransactionPriority::DEFAULT);
				TraceEvent te("ExpectStableThroughputWorkloadMetrics");
				te.detail("TransactionTag", tag)
				    .detail("DefaultPriorityThroughputRate", defaultPriorityMetrics.totalCost.smoothRate())
				    .detail("LowPriorityThroughputRate", lowPriorityMetrics.totalCost.smoothRate())
				    .detail("DefaultPriorityThrottledDurationRate",
				            defaultPriorityMetrics.throttledDuration.smoothRate())
				    .detail("LowPriorityThrottledDurationRate", lowPriorityMetrics.throttledDuration.smoothRate())
				    .detail("DefaultPriorityTagThrottledErrorRate",
				            defaultPriorityMetrics.tagThrottledErrors.smoothRate())
				    .detail("LowPriorityTagThrottledErrorRate", lowPriorityMetrics.tagThrottledErrors.smoothRate());
			}
		}
	}

	Key getKey(int index) const { return Key(format("%06d", index)).withPrefix(keyPrefix); }

	ACTOR static Future<Void> runReadTransaction(ExpectStableThroughputWorkload* self,
	                                             Database cx,
	                                             int tagIndex,
	                                             TransactionPriority priority) {
		state Transaction tr(cx);
		state TransactionTag throttlingTag = ThroughputQuotaUtil::getTransactionTag(self->tagPrefix, tagIndex);
		state std::vector<Future<Void>> futures;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::AUTO_THROTTLE_TAG, throttlingTag);
				if (priority == TransactionPriority::LOW) {
					tr.setOption(FDBTransactionOptions::PRIORITY_LOW);
				}
				if (self->useGrvCache && isBusyThrottlingIdsTrackingEnabled()) {
					tr.setOption(FDBTransactionOptions::USE_GRV_CACHE);
				}

				if (self->provideExpectedCost) {
					tr.setExpectedCost(self->opsPerTransaction * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
				}

				futures.clear();
				futures.reserve(self->opsPerTransaction);
				for (int i = 0; i < self->opsPerTransaction; ++i) {
					futures.push_back(success(tr.get(self->getKey(i))));
				}
				wait(waitForAll(futures));
				self->finishTransaction(tr, throttlingTag, priority);
				return Void();
			} catch (Error& e) {
				self->finishTransaction(tr, throttlingTag, priority, e.code() == error_code_proxy_tag_throttled);
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> runWriteTransaction(ExpectStableThroughputWorkload* self,
	                                              Database cx,
	                                              int tagIndex,
	                                              TransactionPriority priority,
	                                              bool isClear) {
		state Transaction tr(cx);
		state TransactionTag throttlingTag = ThroughputQuotaUtil::getTransactionTag(self->tagPrefix, tagIndex);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::AUTO_THROTTLE_TAG, throttlingTag);
				if (priority == TransactionPriority::LOW) {
					tr.setOption(FDBTransactionOptions::PRIORITY_LOW);
				}

				if (self->useGrvCache && isBusyThrottlingIdsTrackingEnabled()) {
					tr.setOption(FDBTransactionOptions::USE_GRV_CACHE);
				}
				if (self->provideExpectedCost) {
					tr.setExpectedCost(self->opsPerTransaction * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE *
					                   CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO);
				}
				// Get read version in order to enforce throttling
				wait(success(tr.getReadVersion()));
				for (int i = 0; i < self->opsPerTransaction; ++i) {
					if (isClear) {
						tr.clear(self->getKey(i));
					} else {
						tr.set(self->getKey(i), ""_sr);
					}
				}
				wait(tr.commit());
				self->finishTransaction(tr, throttlingTag, priority);
				return Void();
			} catch (Error& e) {
				self->finishTransaction(tr, throttlingTag, priority, e.code() == error_code_proxy_tag_throttled);
				wait(tr.onError(e));
			}
		}
	}

	Future<Void> runTransaction(Database const& cx, int tagIndex, TransactionPriority priority) {
		switch (opType) {
		case OpType::READ:
			return runReadTransaction(this, cx, tagIndex, priority);
		case OpType::SET:
			return runWriteTransaction(this, cx, tagIndex, priority, /*isClear=*/false);
		case OpType::CLEAR:
			return runWriteTransaction(this, cx, tagIndex, priority, /*isClear=*/true);
		default:
			ASSERT(false);
			return Void();
		}
	}

	ACTOR static Future<Void> runClient(ExpectStableThroughputWorkload* self,
	                                    Database cx,
	                                    int tagIndex,
	                                    TransactionPriority priority) {
		loop {
			wait(delay(1 / self->getAttemptedTransactionRatePerActor()) &&
			     self->runTransaction(cx, tagIndex, priority));
		}
	}

	double getAttemptedTransactionRatePerActor() const {
		double transactionCostInPages = opsPerTransaction;
		if (opType != OpType::READ) {
			transactionCostInPages *= CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO;
		}
		auto const numActors = defaultPriorityActors + lowPriorityActors;
		return attemptedThroughputPagesRate / (transactionCostInPages * numActors);
	}

public:
	static constexpr auto NAME = "ExpectStableThroughput";
	explicit ExpectStableThroughputWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 1200.0);
		tagPrefix = getOption(options, "tagPrefix"_sr, "testTag"_sr);
		numTags = getOption(options, "numTags"_sr, 1);
		expectedThroughputPagesRate = getOption(options, "expectedThroughputPagesRate"_sr, 1);
		attemptedThroughputPagesRate = getOption(options, "attemptedThroughputPagesRate"_sr, 2);
		errorTolerance = getOption(options, "errorTolerance"_sr, 0.2);
		keyPrefix = getOption(options, "keyPrefix"_sr, "testKey"_sr);
		defaultPriorityActors = getOption(options, "defaultPriorityActors"_sr, 100);
		lowPriorityActors = getOption(options, "lowPriorityActors"_sr, 0);
		// When simulation is not sped up, there are known, unrelated, rare availability issues that cause
		// throughput to be significantly lower than the total quota. To avoid these failure cases, we
		// don't measure throughput until after simulation has been sped up.
		warmupTime = getOption(options, "warmupTime"_sr, FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS);
		// Even with sped-up simulation, ratekeeper can drop to a limit of 100 TPS with enough buggification.
		// To avoid this, the number of operations per transaction should be high enough to keep the
		// total target TPS at or below 100 TPS.
		opsPerTransaction = getOption(options, "opsPerTransaction"_sr, deterministicRandom()->randomInt(5, 10));
		provideExpectedCost = getOption(options, "provideExpectedCost"_sr, deterministicRandom()->coinflip());
		useGrvCache = getOption(options, "useGrvCache"_sr, deterministicRandom()->coinflip());
		opType = static_cast<OpType>(getOption(options, "opType"_sr, deterministicRandom()->randomInt(0, 3)));

		// To avoid timeouts:
#if defined(USE_SANITIZER) || defined(VALGRIND)
		testDuration = 60.0;
		warmupTime = 10.0;
#endif
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}

		// Required for GRV Cache to work in simulation.
		// Normally, MVC would set the shared state and it is verified upon setting the
		// transaction option for GRV Cache.
		// In simulation, explicitly initialize it when used for tests.
		cx->initSharedState();
		TraceEvent te("StartingExpectStableThroughputWorkload");
		traceParameters(te);
		te.log();
		startTime = now();
		std::vector<Future<Void>> clients;
		for (int tagIndex = 0; tagIndex < numTags; ++tagIndex) {
			for (int actorIndex = 0; actorIndex < defaultPriorityActors; ++actorIndex) {
				clients.push_back(runClient(this, cx, tagIndex, TransactionPriority::DEFAULT));
			}
			for (int actorIndex = 0; actorIndex < lowPriorityActors; ++actorIndex) {
				clients.push_back(runClient(this, cx, tagIndex, TransactionPriority::LOW));
			}
		}
		return loggerActor(this) || success(timeout(waitForAll(clients), warmupTime + testDuration));
	}

	Future<bool> check(Database const&) override {
		// When running with sanitizers, we purposefully shorten the duration to avoid timeouts.
		// Therefore, we cannot make strong assumptions about the achieved throughput, so
		// we skip the check phase
#if defined(USE_SANITIZER) || defined(VALGRIND)
		return true;
#else
		if (clientId != 0) {
			return true;
		}

		auto const expectedTotalCost =
		    testDuration * expectedThroughputPagesRate * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;

		bool passed = true;
		for (auto const& [tag, prioritizedMetrics] : metricsMap) {
			auto const totalCost = prioritizedMetrics.getTotalCost();

			bool tagPassed = (totalCost <= static_cast<double>(expectedTotalCost) * (1.0 + errorTolerance));

			// Global ratekeeper throttling is known to restrict throughput independent of quota-based throttling
			// if certain knobs are buggified and the workload performs writes. We work around this by only enforcing a
			// throughput lower bound if the workload is only performing reads.
			if (opType == OpType::READ) {
				tagPassed = tagPassed && (totalCost >= static_cast<double>(expectedTotalCost) * (1.0 - errorTolerance));
			}

			auto const severity = tagPassed ? SevInfo : SevError;
			TraceEvent te(severity, "CheckingStableThroughput");
			te.detail("Tag", tag).detail("ExpectedTotalCost", expectedTotalCost).detail("TotalCost", totalCost);
			traceParameters(te);
			passed = passed && tagPassed;
		}
		return passed;
#endif // #if defined(USE_SANITIZER) || defined(VALGRIND)
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		double totalDefaultPriorityCost{ 0.0 };
		double totalLowPriorityCost{ 0.0 };
		double defaultPriorityTagThrottledErrors{ 0.0 };
		double lowPriorityTagThrottledErrors{ 0.0 };
		double defaultPriorityThrottledDuration{ 0.0 };
		double lowPriorityThrottledDuration{ 0.0 };
		for (auto const& [tag, prioritizedMetrics] : metricsMap) {
			auto const& lowPriorityMetrics = prioritizedMetrics.getMetrics(TransactionPriority::LOW);
			auto const& defaultPriorityMetrics = prioritizedMetrics.getMetrics(TransactionPriority::DEFAULT);
			totalDefaultPriorityCost += defaultPriorityMetrics.totalCost.getTotal();
			totalLowPriorityCost += lowPriorityMetrics.totalCost.getTotal();
			defaultPriorityTagThrottledErrors += defaultPriorityMetrics.tagThrottledErrors.getTotal();
			lowPriorityTagThrottledErrors += lowPriorityMetrics.tagThrottledErrors.getTotal();
			defaultPriorityThrottledDuration += defaultPriorityMetrics.throttledDuration.getTotal();
			lowPriorityThrottledDuration += lowPriorityMetrics.throttledDuration.getTotal();
		}
		m.emplace_back("Total Default Priority Cost", totalDefaultPriorityCost, Averaged::False);
		m.emplace_back("Total Low Priority Cost", totalLowPriorityCost, Averaged::False);
		m.emplace_back("Default Priority TagThrottled Errors", defaultPriorityTagThrottledErrors, Averaged::False);
		m.emplace_back("Low Priority TagThrottled Errors", lowPriorityTagThrottledErrors, Averaged::False);
		m.emplace_back("Default Priority Throttling Duration", defaultPriorityThrottledDuration, Averaged::False);
		m.emplace_back("Low Priority Throttling Duration", lowPriorityThrottledDuration, Averaged::False);
	}
};

WorkloadFactory<ExpectStableThroughputWorkload> ExpectStableThroughputWorkloadFactory;
