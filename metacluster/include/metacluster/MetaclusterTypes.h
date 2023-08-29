/*
 * MetaclusterTypes.h
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

#ifndef METACLUSTER_METACLUSTERTYPES_H
#define METACLUSTER_METACLUSTERTYPES_H
#pragma once

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/json_spirit/json_spirit_value.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/MetaclusterRegistration.h"
#include "fdbclient/MetaclusterTenantEntry.actor.h"
#include "flow/flat_buffers.h"

namespace metacluster {

FDB_BOOLEAN_PARAM(IgnoreCapacityLimit);
FDB_BOOLEAN_PARAM(IsRestoring);

struct ClusterUsage {
	int numTenantGroups = 0;

	ClusterUsage() = default;
	ClusterUsage(int numTenantGroups) : numTenantGroups(numTenantGroups) {}

	json_spirit::mObject toJson() const;

	bool operator==(const ClusterUsage& other) const noexcept { return numTenantGroups == other.numTenantGroups; }
	bool operator!=(const ClusterUsage& other) const noexcept { return !(*this == other); }
	bool operator<(const ClusterUsage& other) const noexcept { return numTenantGroups < other.numTenantGroups; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, numTenantGroups);
	}
};

// Represents the various states that a data cluster could be in.
//
// REGISTERING - the data cluster is being registered with the metacluster
// READY - the data cluster is active
// REMOVING - the data cluster is being removed and cannot have its configuration changed or any tenants created
// RESTORING - the data cluster is being restored and cannot have its configuration changed or any tenants
//             created/updated/deleted.
enum class DataClusterState { REGISTERING, READY, REMOVING, RESTORING };

enum class AutoTenantAssignment { ENABLED, DISABLED };

struct DataClusterEntry {
	using ConfigValue = std::variant<int, AutoTenantAssignment>;

	constexpr static FileIdentifier file_identifier = 929511;

	static std::string clusterStateToString(DataClusterState clusterState);
	static DataClusterState stringToClusterState(std::string stateStr);
	static std::string autoTenantAssignmentToString(AutoTenantAssignment autoTenantAssignment);

	UID id;
	ClusterUsage capacity;
	ClusterUsage allocated;

	DataClusterState clusterState = DataClusterState::READY;

	AutoTenantAssignment autoTenantAssignment = AutoTenantAssignment::ENABLED;

	DataClusterEntry() = default;
	DataClusterEntry(ClusterUsage capacity) : capacity(capacity) {}
	DataClusterEntry(UID id, ClusterUsage capacity, ClusterUsage allocated)
	  : id(id),
	    capacity(capacity),
	    allocated(allocated) {}

	void configure(std::string const& parameter, ConfigValue const& value);
	void configure(std::map<std::string, ConfigValue> const& parameters);

	// Returns true if all configurable properties match
	bool matchesConfiguration(DataClusterEntry const& other) const { return capacity == other.capacity; }

	bool hasCapacity() const { return allocated < capacity; }

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static DataClusterEntry decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<DataClusterEntry>(value, IncludeVersion());
	}

	json_spirit::mObject toJson() const;

	bool operator==(DataClusterEntry const& other) const {
		return id == other.id && capacity == other.capacity && allocated == other.allocated &&
		       clusterState == other.clusterState;
	}

	bool operator!=(DataClusterEntry const& other) const { return !(*this == other); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, capacity, allocated, clusterState, autoTenantAssignment);
	}
};

struct DataClusterMetadata {
	constexpr static FileIdentifier file_identifier = 5573993;

	DataClusterEntry entry;
	ClusterConnectionString connectionString;

	DataClusterMetadata() = default;
	DataClusterMetadata(DataClusterEntry const& entry, ClusterConnectionString const& connectionString)
	  : entry(entry),
	    connectionString(connectionString) {}

	bool matchesConfiguration(DataClusterMetadata const& other) const {
		return entry.matchesConfiguration(other.entry) && connectionString == other.connectionString;
	}

	Value encode() const { return ObjectWriter::toValue(*this, IncludeVersion()); }
	static DataClusterMetadata decode(ValueRef const& value) {
		return ObjectReader::fromStringRef<DataClusterMetadata>(value, IncludeVersion());
	}

	json_spirit::mValue toJson() const {
		json_spirit::mObject obj = entry.toJson();
		obj["connection_string"] = connectionString.toString();
		return obj;
	}

	bool operator==(DataClusterMetadata const& other) const {
		return entry == other.entry && connectionString == other.connectionString;
	}

	bool operator!=(DataClusterMetadata const& other) const { return !(*this == other); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, connectionString, entry);
	}
};

} // namespace metacluster

template <>
struct Traceable<metacluster::ClusterUsage> : std::true_type {
	static std::string toString(const metacluster::ClusterUsage& value) {
		return format("NumTenantGroups: %d", value.numTenantGroups);
	}
};

#endif
