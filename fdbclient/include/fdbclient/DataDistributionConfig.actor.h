/*
 * DataDistributionConfig.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_DATA_DISTRIBUTION_CONFIG_ACTOR_G_H)
#define FDBCLIENT_DATA_DISTRIBUTION_CONFIG_ACTOR_G_H
#include "fdbclient/DataDistributionConfig.actor.g.h"
#elif !defined(FDBCLIENT_DATA_DISTRIBUTION_CONFIG_ACTOR_H)
#define FDBCLIENT_DATA_DISTRIBUTION_CONFIG_ACTOR_H

#include "flow/serialize.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/KeyBackedRangeMap.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/json_spirit/json_spirit_value.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Give a sorted boundaries and a range. Split the range into multiple ranges aligned with boundaries
//
std::vector<KeyRangeRef> splitRangeByBoundaries(KeyRangeRef keys, const std::vector<Key>& boundaries);

template <class ForwardIt, class Func>
std::vector<KeyRangeRef> splitRangeByBoundaries(KeyRangeRef keys,
                                                ForwardIt boundaryBegin,
                                                ForwardIt boundaryEnd,
                                                Func keyAccess) {
	std::vector<KeyRangeRef> ranges;
	if (boundaryBegin == boundaryEnd) {
		ranges.push_back(keys);
		return ranges;
	}
	KeyRef beginKey = keys.begin;

	while (boundaryBegin != boundaryEnd && keys.contains(keyAccess(boundaryBegin))) {
		// fmt::print("splitBeginRangeByBoundaries Range 1: begin: {}, boundaryBegin {} \n", beginKey, *boundaryBegin);
		ranges.emplace_back(beginKey, keyAccess(boundaryBegin));
		beginKey = KeyRef(keyAccess(boundaryBegin));
		boundaryBegin++;
	}
	ranges.emplace_back(beginKey, keys.end);
	return ranges;
}

// DD Configuration for a key range range.
// There are many properties here, all Optional, because for a given key range, a property
// can be unspecified which means it continues the setting of the preceding range.
struct DDRangeConfig {
	constexpr static FileIdentifier file_identifier = 9193856;

	DDRangeConfig(Optional<int> replicationFactor = {}, Optional<int> teamID = {}, Optional<UID> bulkLoadID = {})
	  : replicationFactor(replicationFactor),
	    teamID(teamID),
	    bulkLoadID(bulkLoadID) {}

	Optional<int> replicationFactor;
	// Ranges with different team IDs should be assigned to different storage teams
	// Shards with the same team ID can be assigned to the same storage team but nothing enforces/prefers this.
	Optional<int> teamID;

	Optional<UID> bulkLoadID;

	DDRangeConfig apply(DDRangeConfig const& update) const {
		DDRangeConfig result = *this;
		if (update.replicationFactor.present()) {
			result.replicationFactor = update.replicationFactor;
		}
		if (update.teamID.present()) {
			result.teamID = update.teamID;
		}
		if (update.bulkLoadID.present()) {
			result.bulkLoadID = update.bulkLoadID;
		}

		return result;
	}

	bool operator==(DDRangeConfig const& rhs) const = default;

	// String description of the range config
	std::string toString() const {
		return fmt::format("replication={} teamID={} bulkLoadID={}", replicationFactor, teamID, bulkLoadID);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, replicationFactor, teamID, bulkLoadID);
	}

	json_spirit::mObject toJSON() const {
		json_spirit::mObject doc;
		if (teamID.present()) {
			doc["teamID"] = *teamID;
		}
		if (replicationFactor.present()) {
			doc["replicationFactor"] = *replicationFactor;
		}
		if (bulkLoadID.present()) {
			doc["bulkLoadID"] = bulkLoadID.get().toString();
		}
		return doc;
	}
};

template <>
struct Traceable<DDRangeConfig> : std::true_type {
	static std::string toString(const DDRangeConfig& c) { return c.toString(); }
};

template <>
struct fmt::formatter<DDRangeConfig> : FormatUsingTraceable<DDRangeConfig> {};

struct SpecialBoundaryInfo;

struct DDConfiguration : public KeyBackedClass {
	DDConfiguration(KeyRef prefix = SystemKey("\xff\x02/ddconfig/"_sr)) : KeyBackedClass(prefix) {}

	// RangeConfigMap is a  KeyBackedRangeMap of DDRangeConfig values describing various option overrides for key ranges
	typedef KeyBackedRangeMap<Key,
	                          DDRangeConfig,
	                          TupleCodec<Key>,
	                          ObjectCodec<DDRangeConfig, decltype(IncludeVersion())>>
	    RangeConfigMap;

	typedef RangeConfigMap::LocalSnapshot RangeConfigMapSnapshot;

	// Range configuration options set by Users
	RangeConfigMap userRangeConfig() const { return { subspace.pack(__FUNCTION__sr), trigger, IncludeVersion() }; }

	static json_spirit::mValue toJSON(RangeConfigMapSnapshot const& snapshot, bool includeDefaultRanges = false);
};

struct SpecialBoundaryInfo : public ReferenceCounted<SpecialBoundaryInfo> {
	bool violatesBoundary(KeyRangeRef range);
	std::vector<KeyRangeRef> splitRangeBySpecialBoundaries(KeyRangeRef keys);
	const std::vector<Key>& getSpecialBoundaries();
	Reference<const DDConfiguration::RangeConfigMapSnapshot> getSnapshot() const { return snapshot; }

	explicit SpecialBoundaryInfo(Reference<DDConfiguration::RangeConfigMapSnapshot> snapshot);
	SpecialBoundaryInfo()
	  : SpecialBoundaryInfo(makeReference<DDConfiguration::RangeConfigMapSnapshot>(allKeys.begin, allKeys.end)) {}

	void reset(Reference<DDConfiguration::RangeConfigMapSnapshot> snapshot);

private:
	Reference<DDConfiguration::RangeConfigMapSnapshot> snapshot;
	// lazy build
	bool boundaryLoaded = false;
	std::vector<Key> boundaries;
};

#include "flow/unactorcompiler.h"
#endif
