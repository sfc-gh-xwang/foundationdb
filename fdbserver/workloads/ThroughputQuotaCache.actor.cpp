/**
 * ThroughputQuotaCache.actor.cpp
 */

#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbserver/workloads/ThroughputQuotaUtil.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // must be the last include

// This workload tests to functionality of RKThroughputQuotaCache
class ThroughputQuotaCacheWorkload : public TestWorkload {
	int numTags;

	static int64_t testReservedQuota() { return 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }
	static int64_t testTotalQuota() { return 1000 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }
	static double const throughputThreshold;
	static double const activeThroughput;

	static TransactionTag getTransactionTag(uint64_t index) {
		return BinaryWriter::toValue(index, Unversioned()).withPrefix("testTag/"_sr);
	}

	static ThrottlingId getThrottlingId(uint64_t index) { return ThrottlingIdRef::fromTag(getTransactionTag(index)); }

	// Simulates continuous active througput using the specified throttlingId
	ACTOR static Future<Void> keepActive(ThroughputQuotaCacheWorkload const* self, RKThroughputQuotaCache* quotaCache) {
		state MockThroughputTracker throughputTracker;
		for (uint64_t index = 0; index < self->numTags; ++index) {
			throughputTracker.update(getThrottlingId(index), activeThroughput);
		}
		loop {
			quotaCache->updateThroughput(throughputTracker);
			wait(delay(1.0));
		}
	}

	ACTOR static Future<Void> setTagQuotas(ThroughputQuotaCacheWorkload const* self, Database db) {
		state uint64_t index = 0;
		while (index < self->numTags) {
			wait(ThroughputQuotaUtil::setTagQuota(db, getTransactionTag(index), testReservedQuota(), testTotalQuota()));
			++index;
		}
		return Void();
	}

	ACTOR static Future<Void> removeTagQuotas(ThroughputQuotaCacheWorkload const* self, Database db) {
		state uint64_t index = 0;
		while (index < self->numTags) {
			wait(ThroughputQuotaUtil::removeTagQuota(db, getTransactionTag(index)));
			++index;
		}
		return Void();
	}

	// Tests tracking throughput before setting quota
	ACTOR static Future<Void> testTrackThroughputFirst(ThroughputQuotaCacheWorkload const* self,
	                                                   Database db,
	                                                   RKThroughputQuotaCache* quotaCache) {
		ASSERT_EQ(quotaCache->size(), 0);
		ASSERT_EQ(quotaCache->activeThrottlingIds(), 0);

		// First, track active throughput, without setting any quota.
		state Future<Void> keepActiveFuture = keepActive(self, quotaCache);
		while (quotaCache->activeThrottlingIds() != self->numTags) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->size(), 0);

		// Next, report set a quota for the tag. The tag is already being
		// tracked by the quota cache, so the quota cache should pick up the
		// newly set quota.
		wait(setTagQuotas(self, db));
		while (quotaCache->size() != self->numTags) {
			wait(delay(1.0));
		}

		// Cleanup
		wait(removeTagQuotas(self, db));
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
	ACTOR static Future<Void> testSetQuotaFirst(ThroughputQuotaCacheWorkload const* self,
	                                            Database db,
	                                            RKThroughputQuotaCache* quotaCache) {
		state Future<Void> keepActiveFuture;
		ASSERT_EQ(quotaCache->size(), 0);
		ASSERT_EQ(quotaCache->activeThrottlingIds(), 0);

		// First, only set the quota, without reporting any throughput.
		wait(setTagQuotas(self, db));
		wait(delay(10.0));
		ASSERT_EQ(quotaCache->size(), 0);
		ASSERT_EQ(quotaCache->activeThrottlingIds(), 0);

		// Next, report active throughput from the tag. Eventually, quota
		// should be tracked.
		keepActiveFuture = keepActive(self, quotaCache);
		while (quotaCache->size() != self->numTags || quotaCache->activeThrottlingIds() != self->numTags) {
			wait(delay(1.0));
		}

		// Cleanup
		wait(removeTagQuotas(self, db));
		while (quotaCache->size() != 0) {
			wait(delay(1.0));
		}
		keepActiveFuture.cancel();
		while (quotaCache->activeThrottlingIds() != 0) {
			wait(delay(1.0));
		}
		return Void();
	}

	ACTOR static Future<Void> _start(ThroughputQuotaCacheWorkload const* self, Database db) {
		state RKThroughputQuotaCache quotaCache(deterministicRandom()->randomUniqueID(),
		                                        db,
		                                        throughputThreshold,
		                                        /*expirationCheckInterval=*/5.0,
		                                        /*expirationThreshold=*/100.0);
		TraceEvent("ThroughputQuotaCacheWorkloadStart").detail("NumTags", self->numTags);
		state Future<Void> quotaCacheFuture = quotaCache.run();
		wait(testSetQuotaFirst(self, db, &quotaCache));
		wait(testTrackThroughputFirst(self, db, &quotaCache));
		return Void();
	}

public:
	static constexpr auto NAME = "ThroughputQuotaCache";
	explicit ThroughputQuotaCacheWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		numTags = getOption(options, "numTags"_sr, deterministicRandom()->randomInt(1, 101));
	}
	Future<Void> setup(Database const&) override { return Void(); }
	Future<Void> start(Database const& db) override { return clientId ? Void() : _start(this, db); }
	Future<bool> check(Database const&) override { return true; }
	void getMetrics(std::vector<PerfMetric>&) override {}
};

double const ThroughputQuotaCacheWorkload::throughputThreshold = 10.0;
double const ThroughputQuotaCacheWorkload::activeThroughput = 100.0;

WorkloadFactory<ThroughputQuotaCacheWorkload> ThroughputQuotaCacheWorkloadFactory;
