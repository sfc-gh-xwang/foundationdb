#pragma once

#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/TenantBalancerInterface.h"

struct TenantBalancerMetadata {
	static const Key tenantBalancerInterfKey;
	static KeyBackedObjectProperty<TenantBalancerInterface, decltype(IncludeVersion())> getTenantBalancerInterface();
};