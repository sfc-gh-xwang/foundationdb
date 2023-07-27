/*
 * TenantBalancerInterface.cpp
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/TenantBalancerInterface.h"

#include <string>

std::string TenantMovementStatus::toJson() const {
	json_spirit::mValue statusRootValue;
	JSONDoc statusRoot(statusRootValue);

	// Insert tenantMoveInfo into JSON
	statusRoot.create("movementId") = tenantMovementInfo.movementId.toString();
	statusRoot.create("tenantGroupName") = printable(tenantMovementInfo.tenantGroupName);

	// Insert movement status into JSON
	if (databaseVersionLag.present()) {
		statusRoot.create("destinationDatabaseVersionLag") = databaseVersionLag.get();
	}
	if (mutationLag.present()) {
		statusRoot.create("mutationLag") = mutationLag.get();
	}
	if (switchVersion.present()) {
		statusRoot.create("switchVersion") = switchVersion.get();
	}
	statusRoot.create("abortRequested") = abortRequested;
	return json_spirit::write_string(statusRootValue);
}