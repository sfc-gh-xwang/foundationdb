/*
 * MetaclusterUtil.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"

#include "metacluster/CreateMetacluster.actor.h"
#include "metacluster/DecommissionMetacluster.actor.h"
#include "metacluster/ListClusters.actor.h"
#include "metacluster/MetaclusterUtil.actor.h"
#include "metacluster/RegisterCluster.actor.h"
#include "metacluster/RemoveCluster.actor.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster::util {

std::pair<ClusterUsage, ClusterUsage> metaclusterCapacity(std::map<ClusterName, DataClusterMetadata> const& clusters) {
	ClusterUsage tenantGroupCapacity;
	ClusterUsage tenantGroupsAllocated;
	for (auto cluster : clusters) {
		tenantGroupCapacity.numTenantGroups +=
		    std::max(cluster.second.entry.capacity.numTenantGroups, cluster.second.entry.allocated.numTenantGroups);
		tenantGroupsAllocated.numTenantGroups += cluster.second.entry.allocated.numTenantGroups;
	}
	return { tenantGroupCapacity, tenantGroupsAllocated };
}

ACTOR Future<Reference<IDatabase>> openDatabase(ClusterConnectionString connectionString) {
	if (g_network->isSimulated()) {
		Reference<IClusterConnectionRecord> clusterFile =
		    makeReference<ClusterConnectionMemoryRecord>(connectionString);
		Database nativeDb = Database::createDatabase(clusterFile, ApiVersion::LATEST_VERSION, IsInternal::False);
		Reference<IDatabase> threadSafeDb =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(nativeDb)));
		MultiVersionApi::api->selectApiVersion(ApiVersion::LATEST_VERSION);
		return MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeDb);
	} else {
		return MultiVersionApi::api->createDatabaseFromConnectionString(connectionString.toString().c_str());
	}
}

ACTOR Future<SimulatedMetacluster> createSimulatedMetacluster(
    Database db,
    Optional<int64_t> tenantIdPrefix,
    Optional<DataClusterEntry> dataClusterConfig,
    SkipMetaclusterCreation skipMetaclusterCreation,
    WaitForMetaclusterCreation waitForMetaclusterCreation,
    WaitForDataClusterRegistration waitForDataClusterRegistration,
    int clustersToRegister) {
	ASSERT(g_network->isSimulated());
	state SimulatedMetacluster simMetacluster;

	Reference<IDatabase> threadSafeHandle =
	    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(db)));

	MultiVersionApi::api->selectApiVersion(db->apiVersion.version());
	simMetacluster.managementDb = MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeHandle);

	if (!skipMetaclusterCreation) {
		if (!tenantIdPrefix.present()) {
			tenantIdPrefix = deterministicRandom()->randomInt(TenantAPI::TENANT_ID_PREFIX_MIN_VALUE,
			                                                  TenantAPI::TENANT_ID_PREFIX_MAX_VALUE + 1);
		}
		wait(success(createMetacluster(db.getReference(), "management_cluster"_sr, tenantIdPrefix.get(), false)));
	} else if (waitForMetaclusterCreation) {
		while (db->clientInfo->get().clusterType != ClusterType::METACLUSTER_MANAGEMENT) {
			wait(db->clientInfo->onChange());
		}
	}

	state int clusterIndex;
	for (clusterIndex = 0; clusterIndex < g_simulator->extraDatabases.size(); ++clusterIndex) {
		state ClusterName clusterName = ClusterNameRef(fmt::format("data_cluster_{}", clusterIndex));

		simMetacluster.dataDbs[clusterName] =
		    Database::createSimulatedExtraDatabase(g_simulator->extraDatabases[clusterIndex], db->defaultTenant);

		if (clustersToRegister < 0 || clusterIndex < clustersToRegister) {
			if (dataClusterConfig.present()) {
				wait(metacluster::registerCluster(simMetacluster.managementDb,
				                                  clusterName,
				                                  g_simulator->extraDatabases[clusterIndex],
				                                  dataClusterConfig.get()));

				// Run a transaction to verify that the data cluster has recovered
				wait(runRYWTransactionVoid(simMetacluster.dataDbs[clusterName],
				                           [](Reference<ReadYourWritesTransaction> tr) {
					                           tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					                           return success(tr->get("\xff"_sr));
				                           }));
			} else if (waitForDataClusterRegistration) {
				loop {
					Optional<DataClusterMetadata> metadata = wait(tryGetCluster(db.getReference(), clusterName));
					if (metadata.present() && metadata.get().entry.clusterState == DataClusterState::READY) {
						break;
					}

					wait(delay(1.0));
				}
			}
		}
	}

	return simMetacluster;
}

// REQUIRES all tenants are deleted from the metacluster
ACTOR Future<Void> decommissionSimulatedMetacluster(SimulatedMetacluster metacluster) {
	ASSERT(g_network->isSimulated());

	std::map<ClusterName, metacluster::DataClusterMetadata> dataClusters = wait(
	    metacluster::listClusters(metacluster.managementDb, ""_sr, "\xff\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS));
	std::vector<Future<Void>> removeClusterFutures;
	for (const auto& [clusterName, clusterMetadata] : dataClusters) {
		removeClusterFutures.emplace_back(success(metacluster::removeCluster(metacluster.managementDb,
		                                                                     clusterName,
		                                                                     ClusterType::METACLUSTER_MANAGEMENT,
		                                                                     metacluster::ForceRemove(false))));
	}
	wait(waitForAll(removeClusterFutures));
	wait(metacluster::decommissionMetacluster(metacluster.managementDb));

	Optional<MetaclusterRegistrationEntry> entry =
	    wait(metacluster::metadata::metaclusterRegistration().get(metacluster.managementDb));
	ASSERT(!entry.present());
	return Void();
}

}; // namespace metacluster::util