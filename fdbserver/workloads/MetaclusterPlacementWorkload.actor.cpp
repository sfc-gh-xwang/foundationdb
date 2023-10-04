/*
 * MetaclusterPlacement.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include <cstdint>
#include <limits>
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/DeterministicRandom.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"

#include "metacluster/CreateTenant.actor.h"
#include "metacluster/Metacluster.h"
#include "metacluster/MetaclusterConsistency.actor.h"
#include "metacluster/MetaclusterTypes.h"
#include "metacluster/MetaclusterUtil.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct MetaclusterPlacementWorkload : TestWorkload {
	static constexpr auto NAME = "MetaclusterPlacement";

	metacluster::util::SimulatedMetacluster simMetacluster;

	int numTenants;
	int numClusters;
	int clusterCapacity;

	MetaclusterPlacementWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(cx, this);
		} else {
			return Void();
		}
	}

	ACTOR static Future<Void> _setup(Database cx, MetaclusterPlacementWorkload* self) {
		ASSERT(g_network->isSimulated());

		self->clusterCapacity = deterministicRandom()->randomInt(1, 11);
		self->numClusters = deterministicRandom()->randomInt(1, g_simulator->extraDatabases.size() + 1);
		self->numTenants = deterministicRandom()->randomInt(1, self->clusterCapacity * self->numClusters + 1);

		TraceEvent("MetaclusterPlacementParameters")
		    .detail("PreferEmptiest", CLIENT_KNOBS->CREATE_TENANT_PREFER_EMPTIEST_CLUSTER)
		    .detail("ClusterCapacity", self->clusterCapacity)
		    .detail("NumClusters", self->numClusters)
		    .detail("NumTenants", self->numTenants);

		metacluster::DataClusterEntry entry;
		entry.capacity.numTenantGroups = self->clusterCapacity;
		wait(
		    store(self->simMetacluster,
		          metacluster::util::createSimulatedMetacluster(cx,
		                                                        0,
		                                                        entry,
		                                                        metacluster::util::SkipMetaclusterCreation::False,
		                                                        metacluster::util::WaitForMetaclusterCreation::True,
		                                                        metacluster::util::WaitForDataClusterRegistration::True,
		                                                        self->numClusters)));
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(cx, this);
		} else {
			return Void();
		}
	}

	ACTOR static Future<Void> _start(Database cx, MetaclusterPlacementWorkload* self) {
		try {
			state int tenantNum;
			for (tenantNum = 0; tenantNum < self->numTenants; ++tenantNum) {
				metacluster::MetaclusterTenantMapEntry entry;
				entry.tenantName = fmt::format("tenant{}", tenantNum);
				wait(metacluster::createTenant(self->simMetacluster.managementDb,
				                               entry,
				                               metacluster::AssignClusterAutomatically::True,
				                               metacluster::IgnoreCapacityLimit::False));
			}
		} catch (Error& e) {
			TraceEvent(SevError, "MetaclusterPlacementCreateTenantFailed").error(e);
			ASSERT(false);
		}

		try {
			KeyBackedRangeResult<std::pair<ClusterName, int64_t>> clusterTenantCounts =
			    wait(metacluster::metadata::management::clusterTenantCount().getRange(
			        self->simMetacluster.managementDb, {}, {}, CLIENT_KNOBS->MAX_DATA_CLUSTERS));

			if (CLIENT_KNOBS->CREATE_TENANT_PREFER_EMPTIEST_CLUSTER) {
				double expectedTenantsPerCluster = self->numTenants / (double)self->numClusters;
				for (auto const& tc : clusterTenantCounts.results) {
					ASSERT_GE(tc.second, (int)expectedTenantsPerCluster);
					ASSERT_LE(tc.second, ceil(expectedTenantsPerCluster));
				}
			} else {
				int fullClusters = 0;
				int partialClusters = 0;
				for (auto const& tc : clusterTenantCounts.results) {
					if (tc.second == self->clusterCapacity) {
						++fullClusters;
					} else if (tc.second != 0) {
						ASSERT_EQ(tc.second, self->numTenants % self->clusterCapacity);
						++partialClusters;
					}
				}

				ASSERT_LE(partialClusters, 1);
				ASSERT_EQ(fullClusters, self->numTenants / self->clusterCapacity);
			}
		} catch (Error& e) {
			TraceEvent(SevError, "MetaclusterPlacementCheckDistributionFailed").error(e);
			ASSERT(false);
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId == 0) {
			return _check(cx, this);
		} else {
			return true;
		}
	}

	ACTOR static Future<bool> _check(Database cx, MetaclusterPlacementWorkload* self) {
		// The metacluster consistency check runs the tenant consistency check for each cluster
		state metacluster::util::MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
		    self->simMetacluster.managementDb, metacluster::util::AllowPartialMetaclusterOperations::False);
		wait(metaclusterConsistencyCheck.run());
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<MetaclusterPlacementWorkload> MetaclusterPlacementWorkloadFactory;
