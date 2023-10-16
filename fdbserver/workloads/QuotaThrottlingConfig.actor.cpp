/**
 * QuotaThrottlingConfig.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/IQuotaCostCalculator.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"

class QuotaThrottlingConfigWorkload : public TestWorkload {
	int numIterations;

	ACTOR template <class T>
	static Future<Void> setGlobalConfigKey(Database cx, Key key, T value) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				tr->setOption(FDBTransactionOptions::RAW_ACCESS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				auto const tuple = Tuple::makeTuple(value);
				tr->set(key.withPrefix("\xff\xff/global_config/"_sr), tuple.pack());
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> updateReadSampleRate(Database cx) {
		state double readSampleRate = deterministicRandom()->random01();
		wait(setGlobalConfigKey(cx, quotaThrottlingReadSampleRate, readSampleRate));
		loop {
			if (cx->getReadSampleRate() == readSampleRate) {
				return Void();
			}
			wait(delay(1.0));
		}
	}

	ACTOR static Future<Void> updateCommitSampleCost(Database cx) {
		state int64_t sampleCommitCost = deterministicRandom()->randomInt64(1, 100e3);
		wait(setGlobalConfigKey(cx, quotaThrottlingCommitSampleCost, sampleCommitCost));
		loop {
			if (cx->getCommitSampleCost() == sampleCommitCost) {
				return Void();
			}
			wait(delay(1.0));
		}
	}

	static Value getRandomCostFormula() {
		auto const rand = deterministicRandom()->randomInt(0, 3);
		switch (rand) {
		case 0:
			return "add"_sr;
		case 1:
			return "minimum"_sr;
		case 2:
		default:
			return "round"_sr;
		}
	}

	ACTOR static Future<Void> updateCostCalculation(Database cx) {
		state int64_t pageSize = deterministicRandom()->randomInt64(1, 100e3);
		state double fungibilityRatio = deterministicRandom()->random01() * 10;
		state Value costFormula = getRandomCostFormula();
		state uint64_t bytes = deterministicRandom()->randomInt64(1, 100e3);
		wait(setGlobalConfigKey(cx, quotaThrottlingPageSize, pageSize));
		wait(setGlobalConfigKey(cx, quotaThrottlingFungibilityRatio, fungibilityRatio));
		wait(setGlobalConfigKey(cx, quotaThrottlingCostFormula, Tuple::UnicodeStr(costFormula)));
		loop {
			auto const oracle = IQuotaCostCalculator::create(costFormula.toString(), fungibilityRatio, pageSize);
			if (cx->getWriteOperationCost(bytes) == oracle->getWriteCost(bytes)) {
				return Void();
			}
			wait(delay(1.0));
		}
	}

	ACTOR static Future<Void> _start(Database cx, int numIterations) {
		state int i;
		for (i = 0; i < numIterations; ++i) {
			std::vector<Future<Void>> futures;
			futures.push_back(updateReadSampleRate(cx));
			futures.push_back(updateCommitSampleCost(cx));
			futures.push_back(updateCostCalculation(cx));
			wait(waitForAll(futures));
		}
		return Void();
	}

public:
	QuotaThrottlingConfigWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		numIterations = getOption(options, "numIterations"_sr, 100);
	}

	static constexpr auto NAME = "QuotaThrottlingConfig";

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx, numIterations); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<QuotaThrottlingConfigWorkload> quotaThrottlingConfigWorkloadFactory;
