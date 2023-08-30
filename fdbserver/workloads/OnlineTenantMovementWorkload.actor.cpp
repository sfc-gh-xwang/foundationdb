/*
 *  TenantMovementWorkload.actor.cpp
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/ConfigKnobs.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/flow.h"
#include "flow/IRandom.h"

#include "metacluster/Metacluster.h"
#include "metacluster/MetaclusterConsistency.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct OnlineTenantMovementWorkload : public TestWorkload {
	static constexpr auto NAME = "OnlineTenantMovement";

	metacluster::util::SimulatedMetacluster simMetacluster;

	struct TenantTestData : ReferenceCounted<TenantTestData> {
		ClusterName cluster;
		Optional<TenantGroupName> tenantGroup;

		TenantTestData() {}
		TenantTestData(ClusterName cluster, Optional<TenantGroupName> tenantGroup)
		  : cluster(cluster),
		    tenantGroup(tenantGroup) {}
	};

	struct TenantGroupData : ReferenceCounted<TenantGroupData> {
		ClusterName cluster;
		std::set<TenantName> tenants;

		TenantGroupData() {}
		TenantGroupData(ClusterName cluster) : cluster(cluster) {}
	};

	struct DataClusterData : ReferenceCounted<DataClusterData> {
		Database db;
		int tenantGroupCapacity = 0;

		std::map<TenantName, Reference<TenantTestData>> tenants;
		std::map<TenantGroupName, Reference<TenantGroupData>> tenantGroups;

		DataClusterData() {}
		DataClusterData(Database db) : db(db) {}
	};

	std::map<ClusterName, Reference<DataClusterData>> dataDbs;
	std::map<TenantGroupName, Reference<TenantGroupData>> tenantGroups;
	std::vector<ClusterName> dataDbIndex;

	int64_t totalTenantGroupCapacity = 0;
	std::map<TenantName, Reference<TenantTestData>> createdTenants;

	int maxTenants;
	int maxTenantGroups;
	int64_t tenantIdPrefix;
	int testDuration;

	OnlineTenantMovementWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		maxTenants = std::min<int>(1e8 - 1, getOption(options, "maxTenants"_sr, 1000));
		maxTenantGroups = std::min<int>(2 * maxTenants, getOption(options, "maxTenantGroups"_sr, 20));
		testDuration = getOption(options, "testDuration"_sr, 120.0);
		tenantIdPrefix = getOption(options,
		                           "tenantIdPrefix"_sr,
		                           deterministicRandom()->randomInt(TenantAPI::TENANT_ID_PREFIX_MIN_VALUE,
		                                                            TenantAPI::TENANT_ID_PREFIX_MAX_VALUE + 1));
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("Attrition"); }

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			if (g_network->isSimulated() && BUGGIFY) {
				IKnobCollection::getMutableGlobalKnobCollection().setKnob(
				    "max_tenants_per_cluster", KnobValueRef::create(int{ deterministicRandom()->randomInt(20, 100) }));
			}
			return _setup(cx, this);
		} else {
			return Void();
		}
	}

	ACTOR static Future<Void> _setup(Database cx, OnlineTenantMovementWorkload* self) {
		state metacluster::DataClusterEntry entry;
		entry.capacity.numTenantGroups = 1e9;
		wait(store(self->simMetacluster,
		           metacluster::util::createSimulatedMetacluster(
		               cx,
		               deterministicRandom()->randomInt(TenantAPI::TENANT_ID_PREFIX_MIN_VALUE,
		                                                TenantAPI::TENANT_ID_PREFIX_MAX_VALUE + 1),
		               entry,
		               metacluster::util::SkipMetaclusterCreation(self->clientId != 0))));
		ASSERT_GT(self->simMetacluster.dataDbs.size(), 0);

		for (const auto& [clusterName, db] : self->simMetacluster.dataDbs) {
			self->dataDbs[clusterName] = makeReference<DataClusterData>(db);
			self->dataDbs[clusterName]->tenantGroupCapacity = entry.capacity.numTenantGroups;
			self->dataDbIndex.emplace_back(clusterName);
			self->totalTenantGroupCapacity += entry.capacity.numTenantGroups;
		}

		return Void();
	}

	ClusterName chooseClusterName() { return dataDbIndex[deterministicRandom()->randomInt(0, dataDbIndex.size())]; }

	TenantName chooseTenantName() {
		TenantName tenant(format("tenant%08d", deterministicRandom()->randomInt(0, maxTenants)));
		return tenant;
	}

	TenantGroupName chooseTenantGroup() {
		TenantGroupName tenantGroup =
		    TenantGroupNameRef(format("tenantgroup%08d", deterministicRandom()->randomInt(0, maxTenantGroups)));

		return tenantGroup;
	}

	ACTOR static Future<Void> moveTenantGroup(OnlineTenantMovementWorkload* self) {
		state TenantGroupName tenantGroup = self->chooseTenantGroup();
		state ClusterName dstClusterName = self->chooseClusterName();
		try {
			wait(metacluster::online_movement::moveTenantGroup(
			    self->simMetacluster.managementDb, tenantGroup, dstClusterName));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
		}
		return Void();
	}

	ACTOR static Future<Void> abortTenantMovement(OnlineTenantMovementWorkload* self) {
		state TenantGroupName tenantGroup = self->chooseTenantGroup();
		try {
			wait(metacluster::online_movement::abortTenantMovement(self->simMetacluster.managementDb, tenantGroup));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
		}
		return Void();
	}

	ACTOR static Future<Void> getTenantMovementStatus(OnlineTenantMovementWorkload* self) {
		state TenantGroupName tenantGroup = self->chooseTenantGroup();
		state TenantMovementStatus status;
		try {
			store(
			    status,
			    metacluster::online_movement::getTenantMovementStatus(self->simMetacluster.managementDb, tenantGroup));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
		}
		return Void();
	}

	ACTOR static Future<Void> listMovements(OnlineTenantMovementWorkload* self) {
		state std::vector<TenantMovementInfo> tenantMovementInfo;
		try {
			store(tenantMovementInfo, metacluster::online_movement::listMovements(self->simMetacluster.managementDb));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(cx, this);
		}
		return Void();
	}

	ACTOR
	static Future<Void> delayedTrace() {
		state double start = now();
		loop {
			wait(delay(1.0));
			TraceEvent("TenantMovementNotFinishedAfter").detail("Duration", now() - start).log();
		}
	}

	ACTOR
	Future<Void> _start(Database cx, OnlineTenantMovementWorkload* self) {
		state double start = now();
		while (now() < start + self->testDuration) {
			state Future<Void> logger = delayedTrace();
			state int operation = deterministicRandom()->randomInt(0, 4);
			if (operation == 0) {
				wait(moveTenantGroup(self));
			} else if (operation == 1) {
				wait(abortTenantMovement(self));
			} else if (operation == 2) {
				wait(getTenantMovementStatus(self));
			} else if (operation == 3) {
				wait(listMovements(self));
			}
			wait(yield());
		}
		return Void();
	}

	// Checks that the data cluster state matches our local state
	ACTOR static Future<Void> checkDataCluster(OnlineTenantMovementWorkload* self,
	                                           ClusterName clusterName,
	                                           Reference<DataClusterData> clusterData) {
		state Optional<MetaclusterRegistrationEntry> metaclusterRegistration;
		state std::vector<std::pair<TenantName, TenantMapEntry>> tenants;
		state Reference<ReadYourWritesTransaction> tr = clusterData->db->createTransaction();

		loop {
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				wait(store(metaclusterRegistration, metacluster::metadata::metaclusterRegistration().get(tr)) &&
				     store(tenants,
				           TenantAPI::listTenantMetadataTransaction(
				               tr, ""_sr, "\xff\xff"_sr, clusterData->tenants.size() + 1)));
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}

		ASSERT(metaclusterRegistration.present() &&
		       metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA);

		ASSERT_EQ(tenants.size(), clusterData->tenants.size());
		for (auto [tenantName, tenantEntry] : tenants) {
			ASSERT(clusterData->tenants.count(tenantName));
			auto tenantData = clusterData->tenants.find(tenantName);
			ASSERT(tenantData != clusterData->tenants.end());
			ASSERT(tenantData->second->cluster == clusterName);
			ASSERT(tenantData->second->tenantGroup == tenantEntry.tenantGroup);

			auto itr = self->createdTenants.find(tenantName);
			ASSERT(itr != self->createdTenants.end());
			ASSERT(itr->second == tenantData->second);
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId == 0) {
			return _check(cx, this);
		}
		return true;
	}

	ACTOR
	static Future<bool> _check(Database cx, OnlineTenantMovementWorkload* self) {
		// The metacluster consistency check runs the tenant consistency check for each cluster
		state metacluster::util::MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
		    self->simMetacluster.managementDb, metacluster::util::AllowPartialMetaclusterOperations::False);
		wait(metaclusterConsistencyCheck.run());

		std::map<ClusterName, metacluster::DataClusterMetadata> dataClusters = wait(metacluster::listClusters(
		    self->simMetacluster.managementDb, ""_sr, "\xff\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS + 1));

		int totalTenantGroupsAllocated = 0;
		std::vector<Future<Void>> dataClusterChecks;
		for (auto [clusterName, dataClusterData] : self->dataDbs) {
			auto dataClusterItr = dataClusters.find(clusterName);
			ASSERT(dataClusterItr != dataClusters.end());
			ASSERT_EQ(dataClusterItr->second.entry.capacity.numTenantGroups, dataClusterData->tenantGroupCapacity);
			totalTenantGroupsAllocated += dataClusterData->tenantGroups.size();

			dataClusterChecks.push_back(checkDataCluster(self, clusterName, dataClusterData));
		}
		auto capacityNumbers = metacluster::util::metaclusterCapacity(dataClusters);
		ASSERT_EQ(capacityNumbers.first.numTenantGroups, self->totalTenantGroupCapacity);
		ASSERT_EQ(capacityNumbers.second.numTenantGroups, totalTenantGroupsAllocated);

		wait(waitForAll(dataClusterChecks));

		std::vector<std::pair<TenantName, int64_t>> tenants =
		    wait(metacluster::listTenants(self->simMetacluster.managementDb, ""_sr, "\xff\xff"_sr, 10e6));
		state std::vector<Future<Void>> deleteTenantFutures;
		for (const auto& [tenantName, tid] : tenants) {
			deleteTenantFutures.push_back(metacluster::deleteTenant(self->simMetacluster.managementDb, tenantName));
		}
		wait(waitForAll(deleteTenantFutures));

		wait(metacluster::util::decommissionSimulatedMetacluster(self->simMetacluster));
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<OnlineTenantMovementWorkload> OnlineTenantMovementWorkload;
