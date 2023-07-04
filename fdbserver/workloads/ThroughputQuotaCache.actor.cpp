/**
 * ThroughputQuotaCache.actor.cpp
 */

#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbserver/workloads/ThroughputQuotaUtil.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // must be the last include

// This workload tests to functionality of RKThroughputQuotaCache
class ThroughputQuotaCacheWorkload : public TestWorkload {
	static int64_t testReservedQuota() { return 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }
	static int64_t testTotalQuota() { return 1000 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }
	static double const throughputThreshold;
	static double const activeThroughput;

	// Simulates continuous active througput using the specified throttlingId
	ACTOR static Future<Void> keepActive(ThrottlingId throttlingId, RKThroughputQuotaCache* quotaCache) {
		state MockThroughputTracker throughputTracker;
		throughputTracker.update(throttlingId, activeThroughput);
		loop {
			quotaCache->updateThroughput(throughputTracker);
			wait(delay(1.0));
		}
	}

	// Tests tracking throughput before setting quota
	ACTOR static Future<Void> testTrackThroughputFirst(Database db, RKThroughputQuotaCache* quotaCache) {
		state TransactionTag tag = "testTag"_sr;
		state ThrottlingId throttlingId = ThrottlingIdRef::fromTag(tag);
		ASSERT_EQ(quotaCache->size(), 0);
		ASSERT_EQ(quotaCache->activeThrottlingIds(), 0);

		// First, track active throughput, without setting any quota.
		state Future<Void> keepActiveFuture = keepActive(throttlingId, quotaCache);
		while (quotaCache->activeThrottlingIds() != 1) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->size(), 0);

		// Next, report set a quota for the tag. The tag is already being
		// tracked by the quota cache, so the quota cache should pick up the
		// newly set quota.
		wait(ThroughputQuotaUtil::setTagQuota(db, tag, testReservedQuota(), testTotalQuota()));
		while (quotaCache->size() != 1) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->getReservedQuota(throttlingId).get(), testReservedQuota());
		ASSERT_EQ(quotaCache->getTotalQuota(throttlingId).get(), testTotalQuota());

		// Cleanup
		wait(ThroughputQuotaUtil::removeTagQuota(db, tag));
		while (quotaCache->size() != 0) {
			wait(delay(1.0));
		}
		keepActiveFuture.cancel();
		while (quotaCache->activeThrottlingIds() != 0) {
			wait(delay(1.0));
		}
		return Void();
	}

	// Tests setting quota before tracking throughput
	ACTOR static Future<Void> testSetQuotaFirst(Database db, RKThroughputQuotaCache* quotaCache) {
		state TransactionTag tag = "testTag"_sr;
		state ThrottlingId throttlingId = ThrottlingIdRef::fromTag(tag);
		state Future<Void> keepActiveFuture;
		ASSERT_EQ(quotaCache->size(), 0);
		ASSERT_EQ(quotaCache->activeThrottlingIds(), 0);

		// First, only set the quota, without reporting any throughput.
		wait(ThroughputQuotaUtil::setTagQuota(db, tag, testReservedQuota(), testTotalQuota()));
		wait(delay(10.0));
		ASSERT_EQ(quotaCache->size(), 0);
		ASSERT_EQ(quotaCache->activeThrottlingIds(), 0);

		// Next, report active throughput from the tag. Eventually, quota
		// should be tracked.
		keepActiveFuture = keepActive(throttlingId, quotaCache);
		while (quotaCache->size() != 1 || quotaCache->activeThrottlingIds() != 1) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->getReservedQuota(throttlingId).get(), testReservedQuota());
		ASSERT_EQ(quotaCache->getTotalQuota(throttlingId).get(), testTotalQuota());

		// Cleanup
		wait(ThroughputQuotaUtil::removeTagQuota(db, tag));
		while (quotaCache->size() != 0) {
			wait(delay(1.0));
		}
		keepActiveFuture.cancel();
		while (quotaCache->activeThrottlingIds() != 0) {
			wait(delay(1.0));
		}
		return Void();
	}

	ACTOR static Future<Void> _start(Database db) {
		state RKThroughputQuotaCache quotaCache(deterministicRandom()->randomUniqueID(),
		                                        db,
		                                        throughputThreshold,
		                                        /*expirationCheckInterval=*/5.0,
		                                        /*expirationThreshold=*/60.0);
		state Future<Void> quotaCacheFuture = quotaCache.run();
		wait(testSetQuotaFirst(db, &quotaCache));
		wait(testTrackThroughputFirst(db, &quotaCache));
		return Void();
	}

public:
	static constexpr auto NAME = "ThroughputQuotaCache";
	explicit ThroughputQuotaCacheWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}
	Future<Void> setup(Database const&) override { return Void(); }
	Future<Void> start(Database const& db) override { return clientId ? Void() : _start(db); }
	Future<bool> check(Database const&) override { return true; }
	void getMetrics(std::vector<PerfMetric>&) override {}
};

double const ThroughputQuotaCacheWorkload::throughputThreshold = 10.0;
double const ThroughputQuotaCacheWorkload::activeThroughput = 100.0;

WorkloadFactory<ThroughputQuotaCacheWorkload> ThroughputQuotaCacheWorkloadFactory;
