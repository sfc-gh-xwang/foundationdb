#ifndef FDBCLIENT_TENANT_MOVEMENT_H
#define FDBCLIENT_TENANT_MOVEMENT_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/TenantName.h"

struct TenantMovementRecord {
	UID movementId;
	TenantName tenantName;
	ClusterName destClusterName;
};
#endif // FDBCLIENT_TENANT_MOVEMENT_H