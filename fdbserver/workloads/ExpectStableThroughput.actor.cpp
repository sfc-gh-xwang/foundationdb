/**
 * ExpectStableThroughput.actor.cpp
 */

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
	struct TagMetrics {
		HoltLinearSmoother totalCost;
		HoltLinearSmoother throttledDuration;
		HoltLinearSmoother tagThrottledErrors;
		TagMetrics() : totalCost(1.0, 5.0), throttledDuration(1.0, 5.0), tagThrottledErrors(1.0, 5.0) {}
	};
	boost::unordered_flat_map<TransactionTag, TagMetrics> metricsMap;

	// Parameters:
	double testDuration;
	uint64_t expectedThroughputPagesRate;
	uint64_t attemptedThroughputPagesRate;
	TransactionTag tagPrefix;
	int numTags;
	double errorTolerance;
	Key keyPrefix;
	int numActors;
	double warmupTime;
	int opsPerTransaction;
	bool provideExpectedCost;
	bool performReads;

	double startTime;

	void finishTransaction(Transaction const& tr, TransactionTag const& tag, bool isTagThrottled = false) {
		if (now() > startTime + warmupTime) {
			auto& metrics = metricsMap[tag];
			metrics.totalCost.addDelta(tr.getTotalCost());
			metrics.throttledDuration.addDelta(tr.getTagThrottledDuration());
			if (isTagThrottled) {
				metrics.tagThrottledErrors.addDelta(1.0);
			}
		}
	}

	ACTOR static Future<Void> loggerActor(ExpectStableThroughputWorkload* self) {
		loop {
			wait(delay(5.0));
			for (auto const& [tag, metrics] : self->metricsMap) {
				TraceEvent("ExpectStableThroughputWorkloadMetrics")
				    .detail("TransactionTag", tag)
				    .detail("ThroughputRate", metrics.totalCost.smoothRate())
				    .detail("ThrottledDurationRate", metrics.throttledDuration.smoothRate())
				    .detail("TagThrottledErrorRate", metrics.tagThrottledErrors.smoothRate());
			}
		}
	}

	Key getKey(int index) const { return Key(format("%06d", index)).withPrefix(keyPrefix); }

	ACTOR static Future<Void> runReadTransaction(ExpectStableThroughputWorkload* self, Database cx, int tagIndex) {
		state Transaction tr(cx);
		state TransactionTag throttlingTag = ThroughputQuotaUtil::getTransactionTag(self->tagPrefix, tagIndex);
		state std::vector<Future<Void>> futures;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::AUTO_THROTTLE_TAG, throttlingTag);
				if (self->provideExpectedCost) {
					tr.setExpectedCost(self->opsPerTransaction * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
				}
				futures.clear();
				futures.reserve(self->opsPerTransaction);
				for (int i = 0; i < self->opsPerTransaction; ++i) {
					futures.push_back(success(tr.get(self->getKey(i))));
				}
				wait(waitForAll(futures));
				self->finishTransaction(tr, throttlingTag);
				return Void();
			} catch (Error& e) {
				self->finishTransaction(tr, throttlingTag, e.code() == error_code_proxy_tag_throttled);
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> runWriteTransaction(ExpectStableThroughputWorkload* self, Database cx, int tagIndex) {
		state Transaction tr(cx);
		state TransactionTag throttlingTag = ThroughputQuotaUtil::getTransactionTag(self->tagPrefix, tagIndex);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::AUTO_THROTTLE_TAG, throttlingTag);
				if (self->provideExpectedCost) {
					tr.setExpectedCost(self->opsPerTransaction * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE *
					                   CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO);
				}
				// Get read version in order to enforce throttling
				wait(success(tr.getReadVersion()));
				for (int i = 0; i < self->opsPerTransaction; ++i) {
					tr.set(self->getKey(i), ""_sr);
				}
				wait(tr.commit());
				self->finishTransaction(tr, throttlingTag);
				return Void();
			} catch (Error& e) {
				self->finishTransaction(tr, throttlingTag, e.code() == error_code_proxy_tag_throttled);
				wait(tr.onError(e));
			}
		}
	}

	Future<Void> runTransaction(Database const& cx, int tagIndex) {
		if (performReads) {
			return runReadTransaction(this, cx, tagIndex);
		} else {
			return runWriteTransaction(this, cx, tagIndex);
		}
	}

	ACTOR static Future<Void> runClient(ExpectStableThroughputWorkload* self, Database cx, int tagIndex) {
		loop {
			wait(delay(1 / self->getAttemptedTransactionRatePerActor()) && self->runTransaction(cx, tagIndex));
		}
	}

	double getAttemptedTransactionRatePerActor() const {
		double transactionCostInPages = opsPerTransaction;
		if (!performReads) {
			transactionCostInPages *= CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO;
		}
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
		numActors = getOption(options, "numActors"_sr, 100);
		// When simulation is not sped up, there are known, unrelated, rare availability issues that cause
		// throughput to be significantly lower than the total quota. To avoid these failure cases, we
		// don't measure throughput until after simulation has been sped up.
		warmupTime = getOption(options, "warmupTime"_sr, FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS);
		// Even with sped-up simulation, ratekeeper can drop to a limit of 100 TPS with enough buggification.
		// To avoid this, the number of operations per transaction should be high enough to keep the
		// total target TPS at or below 100 TPS.
		opsPerTransaction = getOption(options, "opsPerTransaction"_sr, deterministicRandom()->randomInt(5, 10));
		provideExpectedCost = getOption(options, "provideExpectedCost"_sr, deterministicRandom()->coinflip());
		performReads = getOption(options, "performReads"_sr, deterministicRandom()->coinflip());
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		TraceEvent("StartingExpectStableThroughputWorkload")
		    .detail("TestDuration", testDuration)
		    .detail("TagPrefix", tagPrefix)
		    .detail("NumTags", numTags)
		    .detail("ExpectedThroughputPagesRate", expectedThroughputPagesRate)
		    .detail("AttemptedThroughputPagesRate", attemptedThroughputPagesRate)
		    .detail("ErrorTolerance", errorTolerance)
		    .detail("KeyPrefix", keyPrefix)
		    .detail("NumActors", numActors)
		    .detail("WarmupTime", warmupTime)
		    .detail("OpsPerTranaction", opsPerTransaction)
		    .detail("ProvideExpectedCost", provideExpectedCost)
		    .detail("PerformReads", performReads);
		startTime = now();
		std::vector<Future<Void>> clients;
		for (int actorIndex = 0; actorIndex < numActors; ++actorIndex) {
			for (int tagIndex = 0; tagIndex < numTags; ++tagIndex) {
				clients.push_back(runClient(this, cx, tagIndex));
			}
		}
		return loggerActor(this) || success(timeout(waitForAll(clients), warmupTime + testDuration));
	}

	Future<bool> check(Database const&) override {
		if (clientId != 0) {
			return true;
		}

		auto const expectedTotalCost =
		    testDuration * expectedThroughputPagesRate * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;

		bool passed = true;
		for (auto const& [tag, metrics] : metricsMap) {
			bool tagPassed =
			    (metrics.totalCost.getTotal() <= static_cast<double>(expectedTotalCost) * (1.0 + errorTolerance));

			// Global ratekeeper throttling is known to restrict throughput independent of quota-based throttling
			// if certain knobs are buggified and the workload performs writes. We work around this by only enforcing a
			// throughput lower bound if the workload is only performing reads.
			if (performReads) {
				tagPassed = tagPassed && (metrics.totalCost.getTotal() >=
				                          static_cast<double>(expectedTotalCost) * (1.0 - errorTolerance));
			}

			auto const severity = tagPassed ? SevInfo : SevError;
			TraceEvent(severity, "CheckingStableThroughput")
			    .detail("Tag", tag)
			    .detail("ExpectedTotalCost", expectedTotalCost)
			    .detail("ErrorTolerance", errorTolerance)
			    .detail("TotalCost", metrics.totalCost.getTotal());
			passed = passed && tagPassed;
		}
		return passed;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		double totalCost{ 0.0 };
		double tagThrottledErrors{ 0.0 };
		double throttledDuration{ 0.0 };
		for (auto const& [tag, metrics] : metricsMap) {
			totalCost += metrics.totalCost.getTotal();
			tagThrottledErrors += metrics.tagThrottledErrors.getTotal();
			throttledDuration += metrics.throttledDuration.getTotal();
		}
		m.emplace_back("Total Cost", totalCost, Averaged::False);
		m.emplace_back("TagThrottled Errors", tagThrottledErrors, Averaged::False);
		m.emplace_back("Total Throttling Duration", throttledDuration, Averaged::False);
	}
};

WorkloadFactory<ExpectStableThroughputWorkload> ExpectStableThroughputWorkloadFactory;
