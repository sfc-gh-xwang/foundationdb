/*
 * MetaclusterTenantEntry.actor.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_METACLUSTERTENANTENTRY_ACTOR_G_H)
#define FDBCLIENT_METACLUSTERTENANTENTRY_ACTOR_G_H
#include "fdbclient/MetaclusterTenantEntry.actor.g.h"
#elif !defined(FDBCLIENT_METACLUSTERTENANTENTRY_ACTOR_H)
#define FDBCLIENT_METACLUSTERTENANTENTRY_ACTOR_H

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/json_spirit/json_spirit_value.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/MetaclusterRegistration.h"
#include "fdbclient/Tenant.h"
#include "flow/flat_buffers.h"

#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {

// Represents the various states that a tenant could be in. Only applies to metacluster, not standalone clusters.
// In a metacluster, a tenant on the management cluster could be in the other states while changes are applied to the
// data cluster.
//
// REGISTERING - the tenant has been created on the management cluster and is being created on the data cluster
// READY - the tenant has been created on both clusters, is active, and is consistent between the two clusters
// REMOVING - the tenant has been marked for removal and is being removed on the data cluster
// UPDATING_CONFIGURATION - the tenant configuration has changed on the management cluster and is being applied to the
//                          data cluster
// RENAMING - the tenant is in the process of being renamed
// ERROR - the tenant is in an error state
//
// A tenant in any configuration is allowed to be removed. Only tenants in the READY or UPDATING_CONFIGURATION phases
// can have their configuration updated. A tenant must not exist or be in the REGISTERING phase to be created. To be
// renamed, a tenant must be in the READY or RENAMING state. In the latter case, the rename destination must match
// the original rename attempt.
//
// If an operation fails and the tenant is left in a non-ready state, re-running the same operation is legal. If
// successful, the tenant will return to the READY state.
enum class TenantState { REGISTERING, READY, REMOVING, UPDATING_CONFIGURATION, RENAMING, ERROR };

std::string tenantStateToString(TenantState tenantState);
TenantState stringToTenantState(std::string stateStr);

struct MetaclusterTenantMapEntry {
	constexpr static FileIdentifier file_identifier = 12247338;

	int64_t id = -1;
	Key prefix;
	TenantName tenantName;
	metacluster::TenantState tenantState = metacluster::TenantState::READY;
	TenantAPI::TenantLockState tenantLockState = TenantAPI::TenantLockState::UNLOCKED;
	Optional<UID> tenantLockId;
	Optional<TenantGroupName> tenantGroup;
	ClusterName assignedCluster;
	int64_t configurationSequenceNum = 0;
	Optional<TenantName> renameDestination;

	// Can be set to an error string if the tenant is in the ERROR state
	std::string error;

	MetaclusterTenantMapEntry();
	MetaclusterTenantMapEntry(int64_t id, TenantName tenantName, metacluster::TenantState tenantState);
	MetaclusterTenantMapEntry(int64_t id,
	                          TenantName tenantName,
	                          metacluster::TenantState tenantState,
	                          Optional<TenantGroupName> tenantGroup);

	static MetaclusterTenantMapEntry fromTenantMapEntry(TenantMapEntry const& source);
	TenantMapEntry toTenantMapEntry() const;

	void setId(int64_t id);
	std::string toJson() const;

	bool matchesConfiguration(MetaclusterTenantMapEntry const& other) const;
	bool matchesConfiguration(TenantMapEntry const& other) const;
	void configure(Standalone<StringRef> parameter, Optional<Value> value);

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static MetaclusterTenantMapEntry decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<MetaclusterTenantMapEntry>(value, IncludeVersion());
	}

	bool operator==(MetaclusterTenantMapEntry const& other) const;
	bool operator!=(MetaclusterTenantMapEntry const& other) const;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           id,
		           tenantName,
		           tenantState,
		           tenantLockState,
		           tenantLockId,
		           tenantGroup,
		           assignedCluster,
		           configurationSequenceNum,
		           renameDestination,
		           error);
		if constexpr (Ar::isDeserializing) {
			if (id >= 0) {
				prefix = TenantAPI::idToPrefix(id);
			}
			ASSERT(tenantState >= metacluster::TenantState::REGISTERING &&
			       tenantState <= metacluster::TenantState::ERROR);
		}
	}
};

struct MetaclusterTenantGroupEntry {
	constexpr static FileIdentifier file_identifier = 1082739;

	ClusterName assignedCluster;

	MetaclusterTenantGroupEntry() = default;
	MetaclusterTenantGroupEntry(ClusterName assignedCluster) : assignedCluster(assignedCluster) {}

	json_spirit::mObject toJson() const;

	Value encode() { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static MetaclusterTenantGroupEntry decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<MetaclusterTenantGroupEntry>(value, IncludeVersion());
	}

	bool operator==(MetaclusterTenantGroupEntry const& other) const;
	bool operator!=(MetaclusterTenantGroupEntry const& other) const;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, assignedCluster);
	}
};

class MetaclusterTenantTypes {
public:
	using TenantMapEntryT = MetaclusterTenantMapEntry;
	using TenantGroupEntryT = MetaclusterTenantGroupEntry;
};

// Metadata used only on the management cluster
namespace metadata::management {
struct ConnectionStringCodec {
	static inline Standalone<StringRef> pack(ClusterConnectionString const& val) { return StringRef(val.toString()); }
	static inline ClusterConnectionString unpack(Standalone<StringRef> const& val) {
		return ClusterConnectionString(val.toString());
	}
};

TenantMetadataSpecification<MetaclusterTenantTypes>& tenantMetadata();

// A map from cluster name to the connection string for the cluster
KeyBackedMap<ClusterName, ClusterConnectionString, TupleCodec<ClusterName>, ConnectionStringCodec>&
dataClusterConnectionRecords();
} // namespace metadata::management

template <class Transaction>
Future<Optional<MetaclusterTenantMapEntry>> tryGetTenantTransaction(Transaction tr, int64_t tenantId) {
	CODE_PROBE(true, "Try get tenant by ID");
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	return metadata::management::tenantMetadata().tenantMap.get(tr, tenantId);
}

ACTOR template <class Transaction>
Future<Optional<MetaclusterTenantMapEntry>> tryGetTenantTransaction(Transaction tr, TenantName name) {
	CODE_PROBE(true, "Try get tenant by name");
	tr->setOption(FDBTransactionOptions::RAW_ACCESS);
	Optional<int64_t> tenantId = wait(metadata::management::tenantMetadata().tenantNameIndex.get(tr, name));
	if (tenantId.present()) {
		Optional<MetaclusterTenantMapEntry> entry =
		    wait(metadata::management::tenantMetadata().tenantMap.get(tr, tenantId.get()));
		return entry;
	} else {
		return Optional<MetaclusterTenantMapEntry>();
	}
}

ACTOR template <class DB, class Tenant>
Future<Optional<MetaclusterTenantMapEntry>> tryGetTenant(Reference<DB> db, Tenant tenant) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			Optional<MetaclusterTenantMapEntry> entry = wait(tryGetTenantTransaction(tr, tenant));
			return entry;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR template <class Transaction, class Tenant>
Future<MetaclusterTenantMapEntry> getTenantTransaction(Transaction tr, Tenant tenant) {
	Optional<MetaclusterTenantMapEntry> entry = wait(tryGetTenantTransaction(tr, tenant));
	if (!entry.present()) {
		throw tenant_not_found();
	}

	return entry.get();
}

ACTOR template <class DB, class Tenant>
Future<MetaclusterTenantMapEntry> getTenant(Reference<DB> db, Tenant tenant) {
	Optional<MetaclusterTenantMapEntry> entry = wait(tryGetTenant(db, tenant));
	if (!entry.present()) {
		throw tenant_not_found();
	}

	return entry.get();
}

} // namespace metacluster

#include "flow/unactorcompiler.h"
#endif
