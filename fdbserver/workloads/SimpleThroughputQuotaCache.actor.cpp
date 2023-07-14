/**
 * SimpleThroughputQuotaCache.actor.cpp
 */

#include "fdbclient/UpdateStream.h"
#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/ThroughputQuotaUtil.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // must be the last include

// Simple workload to test the functionality of RKSimpleThroughputQuotaCache
// and RKThroughputQuotaCache.
class SimpleThroughputQuotaCacheWorkload : public TestWorkload {
	static int64_t testReservedQuota() { return 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }

	static int64_t testTotalQuota() { return 1000 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }

	ACTOR static Future<Void> testTagQuota(Database cx, TransactionTag tag, IRKThroughputQuotaCache const* quotaCache) {
		state ThrottlingId throttlingId = ThrottlingIdRef::fromTag(tag);
		ASSERT_EQ(quotaCache->size(), 0);
		wait(ThroughputQuotaUtil::setTagQuota(cx, tag, testReservedQuota(), testTotalQuota()));
		while (quotaCache->size() != 1) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->getReservedQuota(throttlingId).get(), testReservedQuota());
		ASSERT_EQ(quotaCache->getTotalQuota(throttlingId).get(), testTotalQuota());
		wait(ThroughputQuotaUtil::removeTagQuota(cx, tag));
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
		wait(ThroughputQuotaUtil::setTenantGroupQuota(cx, tenantGroup, testReservedQuota(), testTotalQuota()));
		while (quotaCache->size() != 1) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->getReservedQuota(throttlingId).get(), testReservedQuota());
		ASSERT_EQ(quotaCache->getTotalQuota(throttlingId).get(), testTotalQuota());
		wait(ThroughputQuotaUtil::removeTenantGroupQuota(cx, tenantGroup));
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
		wait(ThroughputQuotaUtil::setTenantGroupQuota(cx, sharedName, testReservedQuota(), testTotalQuota()));
		wait(ThroughputQuotaUtil::setTagQuota(cx, sharedName, testReservedQuota() * 2, testTotalQuota() * 2));
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

	// This has no effect for an RKSimpleThroughputQuotaCache object, but for
	// RKThroughputQuotaCache, it's necessary to report sufficient throughput,
	// or the throttlingIds will be ignored.
	ACTOR static Future<Void> keepActive(IRKThroughputQuotaCache* quotaCache) {
		state MockThroughputTracker throughputTracker;
		double const throughput = 100.0;
		throughputTracker.update(ThrottlingIdRef::fromTag("testTag"_sr), throughput);
		throughputTracker.update(ThrottlingIdRef::fromTenantGroup("testTenantGroup"_sr), throughput);
		throughputTracker.update(ThrottlingIdRef::fromTag("testConflictingName"_sr), throughput);
		throughputTracker.update(ThrottlingIdRef::fromTenantGroup("testConflictingName"_sr), throughput);
		loop {
			wait(delay(1.0));
			quotaCache->updateThroughput(throughputTracker);
		}
	}

	static std::unique_ptr<IRKThroughputQuotaCache> getQuotaCache(Database cx) {
		if (deterministicRandom()->coinflip()) {
			return std::make_unique<RKSimpleThroughputQuotaCache>(deterministicRandom()->randomUniqueID(), cx);
		} else {
			return std::make_unique<RKThroughputQuotaCache>(
			    deterministicRandom()->randomUniqueID(),
			    cx,
			    /*throughputThreshold=*/10.0,
			    SERVER_KNOBS->THROUGHPUT_QUOTA_CACHE_EXPIRATION_CHECK_INTERVAL,
			    SERVER_KNOBS->THROUGHPUT_QUOTA_CACHE_EXPIRATION_THRESHOLD);
		}
	}

	ACTOR static Future<Void> _start(Database cx) {
		state std::unique_ptr<IRKThroughputQuotaCache> quotaCache = getQuotaCache(cx);
		state Future<Void> runFuture = quotaCache->run();
		state Future<Void> keepActiveFuture = keepActive(quotaCache.get());
		wait(testTagQuota(cx, "testTag"_sr, quotaCache.get()));
		wait(testTenantGroupQuota(cx, "testTenantGroup"_sr, quotaCache.get()));
		wait(testConflictingNames(cx, "testConflictingName"_sr, quotaCache.get()));
		return Void();
	}

public:
	static constexpr auto NAME = "SimpleThroughputQuotaCache";
	explicit SimpleThroughputQuotaCacheWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SimpleThroughputQuotaCacheWorkload> SimpleThroughputQuotaCacheWorkloadFactory;
