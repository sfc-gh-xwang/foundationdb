/*
 * ConfigureCluster.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(METACLUSTER_CONFIGURECLUSTER_ACTOR_G_H)
#define METACLUSTER_CONFIGURECLUSTER_ACTOR_G_H
#include "metacluster/ConfigureCluster.actor.g.h"
#elif !defined(METACLUSTER_CONFIGURECLUSTER_ACTOR_H)
#define METACLUSTER_CONFIGURECLUSTER_ACTOR_H

#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "metacluster/MetaclusterInternal.actor.h"
#include "metacluster/MetaclusterMetadata.h"
#include "metacluster/MetaclusterOperationContext.actor.h"
#include "metacluster/MetaclusterTypes.h"

namespace metacluster {

// This should only be called from a transaction that has already confirmed that the cluster entry
// is present. The updatedEntry should use the existing entry and modify only those fields that need
// to be changed.
template <class Transaction>
void updateClusterMetadata(Transaction tr,
                           ClusterNameRef name,
                           DataClusterEntry const& previousEntry,
                           DataClusterEntry const& updatedEntry,
                           IsRestoring isRestoring = IsRestoring::False) {
	if (previousEntry.clusterState == DataClusterState::REGISTERING &&
	    updatedEntry.clusterState != DataClusterState::READY &&
	    updatedEntry.clusterState != DataClusterState::REMOVING) {
		CODE_PROBE(true, "Updating cluster state from registering to unexpected state");
		throw cluster_not_found();
	} else if (previousEntry.clusterState == DataClusterState::REMOVING) {
		CODE_PROBE(true, "Attempting to configure cluster that is being removed");
		throw cluster_removed();
	} else if (!isRestoring && previousEntry.clusterState == DataClusterState::RESTORING &&
	           (updatedEntry.clusterState != DataClusterState::READY &&
	            updatedEntry.clusterState != DataClusterState::REMOVING)) {
		CODE_PROBE(true, "Attempting to configure cluster that is being restored");
		throw cluster_restoring();
	} else if (isRestoring) {
		ASSERT(previousEntry.clusterState == DataClusterState::RESTORING &&
		       updatedEntry.clusterState == DataClusterState::RESTORING);
	}
	metadata::management::dataClusters().set(tr, name, updatedEntry);
	internal::updateClusterCapacityIndex(tr, name, previousEntry, updatedEntry);
}

namespace internal {
template <class DB>
struct ConfigureClusterImpl {
	MetaclusterOperationContext<DB> ctx;

	// Initialization parameters
	Optional<ClusterConnectionString> connectionString;
	std::map<std::string, DataClusterEntry::ConfigValue> parameters;

	ConfigureClusterImpl(Reference<DB> managementDb,
	                     ClusterName clusterName,
	                     Optional<ClusterConnectionString> connectionString,
	                     std::map<std::string, DataClusterEntry::ConfigValue> parameters)
	  : ctx(managementDb, clusterName),
	    connectionString(connectionString),
	    parameters(parameters) {}

	ACTOR static Future<Void> checkDataClusterMatch(ConfigureClusterImpl* self, Reference<ITransaction> tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		state Optional<MetaclusterRegistrationEntry> metaclusterRegistration =
		    wait(metadata::metaclusterRegistration().get(tr));

		// Check that this is the expected data cluster and is part of the right metacluster
		if (!metaclusterRegistration.present()) {
			CODE_PROBE(true, "Change connection string to a cluster that is not a data cluster");
			throw invalid_metacluster_operation();
		} else if (metaclusterRegistration.get().clusterType != ClusterType::METACLUSTER_DATA) {
			CODE_PROBE(true, "Change connection string to a cluster that is a management cluster");
			throw invalid_metacluster_operation();
		} else if (!self->ctx.metaclusterRegistration.get().matches(metaclusterRegistration.get())) {
			CODE_PROBE(true, "Change connection string to a different data cluster");
			throw metacluster_mismatch();
		}

		return Void();
	}

	ACTOR static Future<Void> run(ConfigureClusterImpl* self) {
		CODE_PROBE(self->connectionString.present() && !self->parameters.empty(),
		           "Changing data cluster connection string and metadata simultaneously");

		if (self->connectionString.present()) {
			CODE_PROBE(true, "Configure connection string");
			wait(self->ctx.initializeContext());

			state Reference<IDatabase> dataDb = wait(util::openDatabase(self->connectionString.get()));
			wait(runTransaction(dataDb,
			                    [self = self](Reference<ITransaction> tr) { return checkDataClusterMatch(self, tr); }));
		}

		wait(self->ctx.runManagementTransaction([self = self](Reference<typename DB::TransactionT> tr) {
			if (self->connectionString.present()) {
				metadata::management::dataClusterConnectionRecords().set(
				    tr, self->ctx.clusterName.get(), self->connectionString.get());
			}
			if (!self->parameters.empty()) {
				DataClusterEntry updatedEntry = self->ctx.dataClusterMetadata.get().entry;
				updatedEntry.configure(self->parameters);
				updateClusterMetadata(
				    tr, self->ctx.clusterName.get(), self->ctx.dataClusterMetadata.get().entry, updatedEntry);
			}
			return Future<Void>(Void());
		}));

		return Void();
	}
	Future<Void> run() { return run(this); }
};
} // namespace internal

ACTOR template <class DB>
Future<Void> configureCluster(Reference<DB> db,
                              ClusterName name,
                              Optional<ClusterConnectionString> connectionString,
                              std::map<std::string, DataClusterEntry::ConfigValue> parameters) {
	state internal::ConfigureClusterImpl<DB> impl(db, name, connectionString, parameters);
	wait(impl.run());
	return Void();
}

} // namespace metacluster

#endif