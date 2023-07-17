/**
 * IRKThrougputQuotaCache.h
 */

#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/IRKThroughputTracker.h"
#include "flow/flow.h"
#include "flow/Optional.h"

// Responsible for maintaining a cache of per-throttlingId throughput quotas
class IRKThroughputQuotaCache {
public:
	virtual ~IRKThroughputQuotaCache() = default;

	// Returns the cached value for the total throughput quota for
	// the provided throttling ID (in bytes/second)
	virtual Optional<int64_t> getTotalQuota(ThrottlingId const&) const = 0;

	// Returns the cached value for the reserved throughput quota
	// for the provided throttling ID (in bytes/second)
	virtual Optional<int64_t> getReservedQuota(ThrottlingId const&) const = 0;

	// Returns the number of quotas currently cached
	virtual int size() const = 0;

	// Responsible for updating the quota cache. The returned future
	// should never be ready, but can be used for propagating errors.
	virtual Future<Void> run() = 0;

	// Update per-throttlingId throughput statistics. Depending on the
	// implementation, this may cause quotas to be tracked or forgotten.
	virtual void updateThroughput(IRKThroughputTracker const&) = 0;
};

// Uses UpdateStream to monitor the throughput quotas of sufficiently busy throttlingIds
class RKThroughputQuotaCache : public IRKThroughputQuotaCache {
	friend class RKThroughputQuotaCacheImpl;

	struct QuotaData {
		double lastActiveTime;
		Optional<ThrottleApi::ThroughputQuotaValue> quota;
		QuotaData() : lastActiveTime(now()) {}
	};

	ThrottlingIdMap<QuotaData> quotas;
	UID id;
	Database db;
	double throughputThreshold;
	double expirationCheckInterval;
	double expirationThreshold;
	PromiseStream<ThrottlingId> activeThrottlingIdStream;

	void applyUpdate(Key const&, Optional<Value> const&);

public:
	RKThroughputQuotaCache(UID id,
	                       Database db,
	                       double throughputThreshold,
	                       double expirationCheckInterval,
	                       double expirationThreshold);
	~RKThroughputQuotaCache();
	Optional<int64_t> getTotalQuota(ThrottlingId const&) const override;
	Optional<int64_t> getReservedQuota(ThrottlingId const&) const override;
	int size() const override;
	Future<Void> run() override;
	void updateThroughput(IRKThroughputTracker const&) override;

	// Returns the number of active throttling IDs with quotas
	// currently being tracked. These quotas may or may not
	// be set.
	int activeThrottlingIds() const;
};

// Periodically polls the system keyspace to populate a cache of per-throttlingId throughput quotas
class RKSimpleThroughputQuotaCache : public IRKThroughputQuotaCache {
	friend class RKSimpleThroughputQuotaCacheImpl;
	ThrottlingIdMap<ThrottleApi::ThroughputQuotaValue> quotas;
	UID id;
	Database db;

public:
	RKSimpleThroughputQuotaCache(UID id, Database db);
	~RKSimpleThroughputQuotaCache();
	Optional<int64_t> getTotalQuota(ThrottlingId const&) const override;
	Optional<int64_t> getReservedQuota(ThrottlingId const&) const override;
	int size() const override;
	Future<Void> run() override;
	void updateThroughput(IRKThroughputTracker const&) override {}
};

// Cache is updated by a test client that manually sets and removes quotas
class MockRKThroughputQuotaCache : public IRKThroughputQuotaCache {
	ThrottlingIdMap<ThrottleApi::ThroughputQuotaValue> quotas;

public:
	~MockRKThroughputQuotaCache();
	Optional<int64_t> getTotalQuota(ThrottlingId const&) const override;
	Optional<int64_t> getReservedQuota(ThrottlingId const&) const override;
	int size() const override;
	Future<Void> run() override;
	void updateThroughput(IRKThroughputTracker const&) override {}

	void setQuota(ThrottlingId const&, int64_t totalQuota, int64_t reservedQuota);
	void removeQuota(ThrottlingId const&);
};
