/*
 * TenantBalancer.cpp
 * Copyright (c) 2023 Snowflake Computing
 */
#include "fdbclient/TenantBalancer.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/TenantBalancerInterface.h"

KeyBackedObjectProperty<TenantBalancerInterface, decltype(IncludeVersion())>
TenantBalancerMetadata::getTenantBalancerInterface() {
	static KeyBackedObjectProperty<TenantBalancerInterface, decltype(IncludeVersion())> tenantBalancerKeyBackedProperty(
	    "\xff/TenantBalancerInterf"_sr, IncludeVersion());
	return tenantBalancerKeyBackedProperty;
}
