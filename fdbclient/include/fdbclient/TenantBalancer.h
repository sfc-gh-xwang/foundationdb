/*
 * TenantBalancer.h
 * Copyright (c) 2023 Snowflake Computing
 */
#pragma once

#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/TenantBalancerInterface.h"

struct TenantBalancerMetadata {
	static KeyBackedObjectProperty<TenantBalancerInterface, decltype(IncludeVersion())> getTenantBalancerInterface();
};
