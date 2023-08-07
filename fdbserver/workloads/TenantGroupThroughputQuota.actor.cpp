/*
 * TenantGroupThroughputQuota.actor.cpp
 */

#include "fdbclient/ThrottleApi.h"
#include "fdbserver/workloads/ThroughputQuotaUtil.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include

class TenantGroupThroughputQuotaWorkload : public TestWorkload {
	Standalone<StringRef> name;
	int64_t reservedQuotaInPages{ 0 };
	int64_t totalQuotaInPages{ 0 };

	int64_t getReservedQuota() const { return reservedQuotaInPages * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }
	int64_t getTotalQuota() const { return totalQuotaInPages * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }

public:
	static constexpr auto NAME = "TenantGroupThroughputQuota";

	explicit TenantGroupThroughputQuotaWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		name = getOption(options, "name"_sr, "sampleTenantGroup"_sr);
		reservedQuotaInPages = getOption(options, "reservedQuotaInPages"_sr, 0);
		totalQuotaInPages = getOption(options, "totalQuotaInPages"_sr, 0);
	}

	Future<Void> setup(Database const& db) override {
		if (clientId != 0) {
			return Void();
		}
		TraceEvent("TenantGroupThroughputQuotaWorkload_SettingQuota")
		    .detail("TenantGroup", printable(name))
		    .detail("ReservedQuota", getReservedQuota())
		    .detail("TotalQuota", getTotalQuota());
		return ThroughputQuotaUtil::setTenantGroupQuota(db, name, getReservedQuota(), getTotalQuota());
	}
	Future<Void> start(Database const& cx) override { return Void(); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TenantGroupThroughputQuotaWorkload> TenantGroupThroughputQuotaWorkloadFactory;
