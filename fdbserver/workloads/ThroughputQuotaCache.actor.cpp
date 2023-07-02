/**
 * ThroughputQuotaCache.actor.cpp
 */

#include "fdbclient/TagThrottle.h"
#include "fdbclient/UpdateStream.h"
#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // must be the last include

// Tests the functionality of the RKSimpleThroughputQuotaCache class
class ThroughputQuotaCacheWorkload : public TestWorkload {
	static int64_t testReservedQuota() { return 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }

	static int64_t testTotalQuota() { return 1000 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }

	ACTOR static Future<Void> setTagQuota(Database cx, TransactionTag tag, int64_t reservedQuota, int64_t totalQuota) {
		state Reference<ReadYourWritesTransaction> tr = cx->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				ThrottleApi::ThroughputQuotaValue tagQuotaValue;
				tagQuotaValue.reservedQuota = reservedQuota;
				tagQuotaValue.totalQuota = totalQuota;
				if (!tagQuotaValue.isValid()) {
					throw invalid_throttle_quota_value();
				}
				wait(UpdateStream::update(
				    tr, ThrottleApi::getTagQuotaKey(tag), tagQuotaValue.pack().pack(), throughputQuotaUpdatePrefix));
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> removeTagQuota(Database cx, TransactionTag tag) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				wait(UpdateStream::update(
				    tr, ThrottleApi::getTagQuotaKey(tag), Optional<Value>(), throughputQuotaUpdatePrefix));
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> setTenantGroupQuota(Database cx,
	                                              TenantGroupName tenantGroup,
	                                              int64_t reservedQuota,
	                                              int64_t totalQuota) {
		state Reference<ReadYourWritesTransaction> tr = cx->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				ThrottleApi::ThroughputQuotaValue throughputQuotaValue;
				throughputQuotaValue.reservedQuota = reservedQuota;
				throughputQuotaValue.totalQuota = totalQuota;
				if (!throughputQuotaValue.isValid()) {
					throw invalid_throttle_quota_value();
				}
				wait(UpdateStream::update(tr,
				                          TenantMetadata::throughputQuota().packKey(tenantGroup),
				                          throughputQuotaValue.pack().pack(),
				                          throughputQuotaUpdatePrefix));
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> removeTenantGroupQuota(Database cx, TenantGroupName tenantGroup) {
		state Reference<ReadYourWritesTransaction> tr = cx->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				wait(UpdateStream::update(tr,
				                          TenantMetadata::throughputQuota().packKey(tenantGroup),
				                          Optional<Value>(),
				                          throughputQuotaUpdatePrefix));
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> testTagQuota(Database cx, TransactionTag tag, IRKThroughputQuotaCache const* quotaCache) {
		state ThrottlingId throttlingId = ThrottlingIdRef::fromTag(tag);
		ASSERT_EQ(quotaCache->size(), 0);
		wait(setTagQuota(cx, tag, testReservedQuota(), testTotalQuota()));
		while (quotaCache->size() != 1) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->getReservedQuota(throttlingId).get(), testReservedQuota());
		ASSERT_EQ(quotaCache->getTotalQuota(throttlingId).get(), testTotalQuota());
		wait(removeTagQuota(cx, tag));
		while (quotaCache->size() != 0) {
			wait(delay(1.0));
		}
		ASSERT(!quotaCache->getReservedQuota(throttlingId).present());
		ASSERT(!quotaCache->getTotalQuota(throttlingId).present());
		return Void();
	}

	ACTOR static Future<Void> testTenantGroupQuota(Database cx,
	                                               TenantGroupName tenantGroup,
	                                               IRKThroughputQuotaCache const* quotaCache) {
		state ThrottlingId throttlingId = ThrottlingIdRef::fromTenantGroup(tenantGroup);
		ASSERT_EQ(quotaCache->size(), 0);
		wait(setTenantGroupQuota(cx, tenantGroup, testReservedQuota(), testTotalQuota()));
		while (quotaCache->size() != 1) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->getReservedQuota(throttlingId).get(), testReservedQuota());
		ASSERT_EQ(quotaCache->getTotalQuota(throttlingId).get(), testTotalQuota());
		wait(removeTenantGroupQuota(cx, tenantGroup));
		while (quotaCache->size() != 0) {
			wait(delay(1.0));
		}
		ASSERT(!quotaCache->getReservedQuota(throttlingId).present());
		ASSERT(!quotaCache->getTotalQuota(throttlingId).present());
		return Void();
	}

	// When tenant groups and tags have conflicting names, both can still be stored
	// together in the quota database, without conflicting.
	ACTOR static Future<Void> testConflictingNames(Database cx,
	                                               Standalone<StringRef> sharedName,
	                                               IRKThroughputQuotaCache const* quotaCache) {
		ASSERT_EQ(quotaCache->size(), 0);
		wait(setTenantGroupQuota(cx, sharedName, testReservedQuota(), testTotalQuota()));
		wait(setTagQuota(cx, sharedName, testReservedQuota() * 2, testTotalQuota() * 2));
		while (quotaCache->size() != 2) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->size(), 2);
		ASSERT_EQ(quotaCache->getReservedQuota(ThrottlingIdRef::fromTenantGroup(sharedName)).get(),
		          testReservedQuota());
		ASSERT_EQ(quotaCache->getTotalQuota(ThrottlingIdRef::fromTenantGroup(sharedName)).get(), testTotalQuota());
		ASSERT_EQ(quotaCache->getReservedQuota(ThrottlingIdRef::fromTag(sharedName)).get(), testReservedQuota() * 2);
		ASSERT_EQ(quotaCache->getTotalQuota(ThrottlingIdRef::fromTag(sharedName)).get(), testTotalQuota() * 2);
		return Void();
	}

	static void updateThroughput(IRKThroughputQuotaCache& quotaCache) {
		MockThroughputTracker throughputTracker;
		double const throughput = 100.0;
		throughputTracker.update(ThrottlingIdRef::fromTag("testTag"_sr), throughput);
		throughputTracker.update(ThrottlingIdRef::fromTenantGroup("testTenantGroup"_sr), throughput);
		throughputTracker.update(ThrottlingIdRef::fromTag("testConflictingName"_sr), throughput);
		throughputTracker.update(ThrottlingIdRef::fromTenantGroup("testConflictingName"_sr), throughput);
		quotaCache.updateThroughput(throughputTracker);
	}

	static std::unique_ptr<IRKThroughputQuotaCache> getQuotaCache(Database cx) {
		if (deterministicRandom()->coinflip()) {
			return std::make_unique<RKSimpleThroughputQuotaCache>(deterministicRandom()->randomUniqueID(), cx);
		} else {
			return std::make_unique<RKThroughputQuotaCache>(deterministicRandom()->randomUniqueID(), cx);
		}
	}

	ACTOR static Future<Void> _start(Database cx) {
		state std::unique_ptr<IRKThroughputQuotaCache> quotaCache = getQuotaCache(cx);
		state Future<Void> runFuture = quotaCache->run();
		updateThroughput(*quotaCache);
		wait(testTagQuota(cx, "testTag"_sr, quotaCache.get()));
		wait(testTenantGroupQuota(cx, "testTenantGroup"_sr, quotaCache.get()));
		wait(testConflictingNames(cx, "testConflictingName"_sr, quotaCache.get()));
		return Void();
	}

public:
	static constexpr auto NAME = "ThroughputQuotaCache";
	explicit ThroughputQuotaCacheWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ThroughputQuotaCacheWorkload> ThroughputQuotaCacheWorkloadFactory;
