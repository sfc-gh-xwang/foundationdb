/*
 * ThroughputQuota.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/ThrottleApi.h"
#include "fdbserver/workloads/ThroughputQuotaUtil.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// This workload sets the throughput quota of a tag during the setup phase
class ThroughputQuotaWorkload : public TestWorkload {
	bool nameRepresentsTenantGroup{ false };
	Standalone<StringRef> name;
	int64_t reservedQuotaInPages{ 0 };
	int64_t totalQuotaInPages{ 0 };

	int64_t getReservedQuota() const { return reservedQuotaInPages * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }

	int64_t getTotalQuota() const { return totalQuotaInPages * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }

	Future<Void> setTagQuota(Database const& db) {
		TraceEvent("ThroughputQuotaWorkload_SettingTagQuota")
		    .detail("Tag", printable(name))
		    .detail("ReservedQuota", getReservedQuota())
		    .detail("TotalQuota", getTotalQuota());
		return ThroughputQuotaUtil::setTagQuota(db, name, getReservedQuota(), getTotalQuota());
	}

	Future<Void> setTenantGroupQuota(Database const& db) {
		TraceEvent("ThroughputQuotaWorkload_SettingTenantGroupQuota")
		    .detail("TenantGroup", printable(name))
		    .detail("ReservedQuota", getReservedQuota())
		    .detail("TotalQuota", getTotalQuota());
		return ThroughputQuotaUtil::setTenantGroupQuota(db, name, getReservedQuota(), getTotalQuota());
	}

public:
	static constexpr auto NAME = "ThroughputQuota";
	explicit ThroughputQuotaWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		nameRepresentsTenantGroup = getOption(options, "nameRepresentsTenantGroup"_sr, false);
		name = getOption(options, "name"_sr, nameRepresentsTenantGroup ? "sampleTenantGroup"_sr : "sampleTag"_sr);
		reservedQuotaInPages = getOption(options, "reservedQuotaInPages"_sr, 0);
		totalQuotaInPages = getOption(options, "totalQuotaInPages"_sr, 0);
	}

	Future<Void> setup(Database const& db) override {
		if (clientId != 0) {
			return Void();
		}
		if (nameRepresentsTenantGroup) {
			return setTenantGroupQuota(db);
		} else {
			return setTagQuota(db);
		}
	}
	Future<Void> start(Database const& cx) override { return Void(); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ThroughputQuotaWorkload> ThroughputQuotaWorkloadFactory;
