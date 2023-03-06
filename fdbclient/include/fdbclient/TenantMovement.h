#ifndef FDBCLIENT_TENANT_MOVEMENT_H
#define FDBCLIENT_TENANT_MOVEMENT_H
#pragma once

#include <cassert>

#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbrpc/TenantName.h"

using TenantMovementId = UID;

struct TenantMovementMapEntry {
	TenantMovementId id;
	TenantName tenantName;
	ClusterName destClusterName;
};

struct TenantMovementIdCodec {
	static Standalone<StringRef> pack(TenantMovementId id) {
		// Do we need it the map to be ordered by id? If not, then just convert
		std::array<uint64_t, 2> id_copy;
		id_copy[0] = id.first();
		id_copy[1] = id.second();
		return StringRef(reinterpret_cast<uint8_t*>(id_copy.data()), static_cast<int>(id_copy.size()));
	}

	static TenantMovementId unpack(Standalone<StringRef> val) {
		const auto* addr = reinterpret_cast<const uint64_t*>(val.begin());
		assert(addr);
		TenantMovementId id(addr[0], addr[1]);
		return id;
	}
};

struct TenantMovementMetadata {
	Key subspace;
	KeyBackedObjectMap<TenantMovementId, TenantMovementMapEntry, decltype(IncludeVersion()), TenantMovementIdCodec>
	    tenantMovementMap;

	explicit TenantMovementMetadata(Key prefix)
	  : subspace(prefix.withSuffix("movement/"_sr)),
	    tenantMovementMap(subspace.withSuffix("movementMap/"_sr), IncludeVersion()) {}
};
#endif // FDBCLIENT_TENANT_MOVEMENT_H