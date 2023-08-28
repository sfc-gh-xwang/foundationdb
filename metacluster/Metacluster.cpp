/*
 * Metacluster.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "metacluster/MetaclusterMetadata.h"
#include "metacluster/MetaclusterTypes.h"

#include "libb64/decode.h"
#include "libb64/encode.h"

namespace metacluster {

std::string DataClusterEntry::clusterStateToString(DataClusterState clusterState) {
	switch (clusterState) {
	case DataClusterState::REGISTERING:
		return "registering";
	case DataClusterState::READY:
		return "ready";
	case DataClusterState::REMOVING:
		return "removing";
	case DataClusterState::RESTORING:
		return "restoring";
	default:
		UNREACHABLE();
	}
}

DataClusterState DataClusterEntry::stringToClusterState(std::string stateStr) {
	if (stateStr == "registering") {
		return DataClusterState::REGISTERING;
	} else if (stateStr == "ready") {
		return DataClusterState::READY;
	} else if (stateStr == "removing") {
		return DataClusterState::REMOVING;
	} else if (stateStr == "restoring") {
		return DataClusterState::RESTORING;
	}

	UNREACHABLE();
}

std::string DataClusterEntry::autoTenantAssignmentToString(AutoTenantAssignment autoTenantAssignment) {
	switch (autoTenantAssignment) {
	case AutoTenantAssignment::DISABLED:
		return "disabled";
	case AutoTenantAssignment::ENABLED:
		return "enabled";
	default:
		UNREACHABLE();
	}
}

void DataClusterEntry::configure(std::string const& parameter, ConfigValue const& value) {
	if (parameter == "max_tenant_groups") {
		CODE_PROBE(true, "Configure max_tenant_groups");
		ASSERT(std::holds_alternative<int>(value));
		capacity.numTenantGroups = std::get<int>(value);
	} else if (parameter == "auto_tenant_assignment") {
		CODE_PROBE(true, "Configure auto_tenant_assignment");
		ASSERT(std::holds_alternative<int>(value));
		int val = std::get<int>(value);
		ASSERT(val == 0 || val == 1);
		autoTenantAssignment = (AutoTenantAssignment)val;
	} else {
		CODE_PROBE(true, "Invalid data cluster configuration parameter");
		TraceEvent(SevWarnAlways, "UnknownClusterConfigurationParameter").detail("Parameter", parameter);
		throw invalid_cluster_configuration();
	}
}

void DataClusterEntry::configure(std::map<std::string, ConfigValue> const& parameters) {
	for (auto const& [p, v] : parameters) {
		configure(p, v);
	}
}

json_spirit::mObject DataClusterEntry::toJson() const {
	json_spirit::mObject obj;
	obj["id"] = id.toString();
	obj["capacity"] = capacity.toJson();
	obj["allocated"] = allocated.toJson();
	obj["cluster_state"] = DataClusterEntry::clusterStateToString(clusterState);
	obj["auto_tenant_assignment"] = autoTenantAssignmentToString(autoTenantAssignment);
	return obj;
}

json_spirit::mObject ClusterUsage::toJson() const {
	json_spirit::mObject obj;
	obj["num_tenant_groups"] = numTenantGroups;
	return obj;
}

} // namespace metacluster