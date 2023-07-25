/**
 * ExpectStableThroughput.actor.cpp
 */

#include "fdbserver/workloads/workloads.actor.h"

#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last include

// This workload is meant to be run with the ThroughputQuotaWorklaod.
// The ThroughputQuotaWorkload sets a total quota, and then this workload runs
// with tagged transactions for a long duration, attempting to achieve a higher
// throughput than the specified quota. The check phase of this workload then
// verifies that the achieved throughput is near the total quota.
//
// TODO:
//   - Randomize the number of operations per transaction
//   - Test multi-page operations
//   - Test using tenant groups rather than tags
class ExpectStableThroughputWorkload : public TestWorkload {
	// Metrics:
	HoltLinearSmoother totalCost;
	HoltLinearSmoother throttledDuration;
	HoltLinearSmoother tagThrottledErrors;

	// Parameters:
	double testDuration;
	uint64_t expectedThroughputPagesRate;
	uint64_t attemptedThroughputPagesRate;
	TransactionTag throttlingTag;
	double errorTolerance;
	Key keyPrefix;
	int numActors;
	double warmupTime;
	int opsPerTransaction;
	bool provideExpectedCost;
	bool performReads;

	double startTime;

	void finishTransaction(Transaction const& tr) {
		if (now() > startTime + warmupTime) {
			totalCost.addDelta(tr.getTotalCost());
			throttledDuration.addDelta(tr.getTagThrottledDuration());
		}
	}

	ACTOR static Future<Void> loggerActor(ExpectStableThroughputWorkload* self) {
		loop {
			wait(delay(5.0));
			TraceEvent("ExpectStableThroughputWorkloadMetrics")
			    .detail("ThroughputRate", self->totalCost.smoothRate())
			    .detail("ThrottledDurationRate", self->throttledDuration.smoothRate())
			    .detail("TagThrottledErrorRate", self->tagThrottledErrors.smoothRate());
		}
	}

	Key getKey(int index) const { return Key(format("%06d", index)).withPrefix(keyPrefix); }

	ACTOR static Future<Void> runReadTransaction(ExpectStableThroughputWorkload* self, Database cx) {
		state Transaction tr(cx);
		state std::vector<Future<Void>> futures;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::AUTO_THROTTLE_TAG, self->throttlingTag);
				if (self->provideExpectedCost) {
					tr.setExpectedCost(self->opsPerTransaction * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
				}
				futures.clear();
				futures.reserve(self->opsPerTransaction);
				for (int i = 0; i < self->opsPerTransaction; ++i) {
					futures.push_back(success(tr.get(self->getKey(i))));
				}
				wait(waitForAll(futures));
				self->finishTransaction(tr);
				return Void();
			} catch (Error& e) {
				self->finishTransaction(tr);
				if (e.code() == error_code_proxy_tag_throttled && now() > self->startTime + self->warmupTime) {
					self->tagThrottledErrors.addDelta(1.0);
				}
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> runWriteTransaction(ExpectStableThroughputWorkload* self, Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::AUTO_THROTTLE_TAG, self->throttlingTag);
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
				self->finishTransaction(tr);
				return Void();
			} catch (Error& e) {
				self->finishTransaction(tr);
				if (e.code() == error_code_proxy_tag_throttled && now() > self->startTime + self->warmupTime) {
					self->tagThrottledErrors.addDelta(1.0);
				}
				wait(tr.onError(e));
			}
		}
	}

	Future<Void> runTransaction(Database const& cx) {
		if (performReads) {
			return runReadTransaction(this, cx);
		} else {
			return runWriteTransaction(this, cx);
		}
	}

	ACTOR static Future<Void> runClient(ExpectStableThroughputWorkload* self, Database cx) {
		loop {
			wait(delay(1 / self->getAttemptedTransactionRatePerActor()) && self->runTransaction(cx));
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
	explicit ExpectStableThroughputWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx),
	    totalCost(1.0, 5.0),
	    throttledDuration(1.0, 5.0),
	    tagThrottledErrors(1.0, 5.0) {
		testDuration = getOption(options, "testDuration"_sr, 1200.0);
		throttlingTag = getOption(options, "throttlingTag"_sr, "testTag"_sr);
		expectedThroughputPagesRate = getOption(options, "expectedThroughputPagesRate"_sr, 1);
		attemptedThroughputPagesRate = getOption(options, "attemptedThroughputPagesRate"_sr, 2);
		errorTolerance = getOption(options, "errorTolerance"_sr, 0.2);
		keyPrefix = getOption(options, "keyPrefix"_sr, "testKey"_sr);
		numActors = getOption(options, "numActors"_sr, 100);
		warmupTime = getOption(options, "warmupTime"_sr, 10.0);
		opsPerTransaction = getOption(options, "opsPerTransaction"_sr, 5);
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
		    .detail("ThrottlingTag", throttlingTag)
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
		for (int i = 0; i < numActors; ++i) {
			clients.push_back(runClient(this, cx));
		}
		return loggerActor(this) || success(timeout(waitForAll(clients), warmupTime + testDuration));
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0) {
			return true;
		}
		auto const expectedTotalCost =
		    testDuration * expectedThroughputPagesRate * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;

		// Unrelated rare availability issues are known to cause throughput to be significantly lower
		// the total quota. Therefore only enforce an upper bound on write throughput:
		bool passed = (totalCost.getTotal() <= static_cast<double>(expectedTotalCost) * (1.0 + errorTolerance));

		auto const severity = passed ? SevInfo : SevError;
		TraceEvent(severity, "CheckingStableThroughput")
		    .detail("ExpectedTotalCost", expectedTotalCost)
		    .detail("ErrorTolerance", errorTolerance)
		    .detail("TotalCost", totalCost.getTotal());
		return passed;
	}
	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("Total Cost", totalCost.getTotal(), Averaged::False);
		m.emplace_back("TagThrottled Errors", tagThrottledErrors.getTotal(), Averaged::False);
		m.emplace_back("Total Throttling Duration", throttledDuration.getTotal(), Averaged::False);
	}
};

WorkloadFactory<ExpectStableThroughputWorkload> ExpectStableThroughputWorkloadFactory;
