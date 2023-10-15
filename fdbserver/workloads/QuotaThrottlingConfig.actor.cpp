/**
 * QuotaThrottlingConfig.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/QuotaThrottlingConfig.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"

class QuotaThrottlingConfigWorkload : public TestWorkload {
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

	ACTOR static Future<Void> _start(Database cx) {
		state QuotaThrottlingConfig quotaThrottlingConfig(cx->globalConfig);
		loop {
			if (quotaThrottlingConfig.getPageSize() == CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE) {
				break;
			}
			wait(delay(1.0));
		}
		wait(setGlobalConfigKey(cx, quotaThrottlingPageSize, 4096));
		loop {
			if (quotaThrottlingConfig.getPageSize() == 4096) {
				break;
			}
			wait(delay(1.0));
		}
		return Void();
	}

public:
	QuotaThrottlingConfigWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	static constexpr auto NAME = "QuotaThrottlingConfig";

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<QuotaThrottlingConfigWorkload> quotaThrottlingConfigWorkloadFactory;
