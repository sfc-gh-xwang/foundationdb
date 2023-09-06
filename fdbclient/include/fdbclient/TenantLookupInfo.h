/*
 * TenantLookupInfo.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once
#ifndef FDBCLIENT_TENANTLOOKUPINFO_H
#define FDBCLIENT_TENANTLOOKUPINFO_H

#include "fdbrpc/TenantName.h"
#include "fdbrpc/TenantInfo.h"
#include "fdbclient/FDBTypes.h"

struct TenantLookupInfo {
	constexpr static FileIdentifier file_identifier = 8018317;

	int64_t id = TenantInfo::INVALID_TENANT;
	Optional<TenantGroupName> group;
	Optional<ClusterName> assignedCluster;

	explicit TenantLookupInfo() = default;
	explicit TenantLookupInfo(int64_t id,
	                          Optional<TenantGroupName> const& group = {},
	                          Optional<ClusterName> const& cluster = {})
	  : id(id),
	    group(group),
	    assignedCluster(cluster) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, group, assignedCluster);
	}
};

#endif // FDBCLIENT_TENANTLOOKUPINFO_H