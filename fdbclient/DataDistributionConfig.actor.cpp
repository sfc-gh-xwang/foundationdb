/*
 * DataDistributionConfig.actor.cpp
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

#include "fdbclient/DataDistributionConfig.actor.h"
#include "fdbclient/json_spirit/json_spirit_value.h"
#include "flow/actorcompiler.h" // This must be the last #include.

json_spirit::mValue DDConfiguration::toJSON(RangeConfigMapSnapshot const& config, bool includeDefaultRanges) {
	json_spirit::mObject doc;
	json_spirit::mArray& ranges = (doc["ranges"] = json_spirit::mArray()).get_array();
	int defaultRanges = 0;
	int configuredRanges = 0;

	// Range config with no options set
	DDRangeConfig defaultRangeConfig;

	// Add each non-default configured range to the output ranges doc
	for (auto const& rv : config.ranges()) {
		bool configured = rv.value() != defaultRangeConfig;
		if (configured) {
			++configuredRanges;
		} else {
			++defaultRanges;
		}

		if (includeDefaultRanges || configured) {
			json_spirit::mObject range;
			range["begin"] = rv.range().begin.toString();
			range["end"] = rv.range().end.toString();
			range["configuration"] = rv.value().toJSON();
			ranges.push_back(std::move(range));
		}
	}

	doc["numConfiguredRanges"] = configuredRanges;
	doc["numDefaultRanges"] = defaultRanges;
	doc["numBoundaries"] = (int)config.map.size();

	return doc;
}

std::vector<KeyRangeRef> splitRangeByBoundaries(KeyRangeRef keys, const std::vector<Key>& boundaries) {
	auto it = std::upper_bound(boundaries.begin(), boundaries.end(), keys.begin);
	return splitRangeByBoundaries(keys, it, boundaries.end(), [](decltype(it) it) { return *it; });
}

SpecialBoundaryInfo::SpecialBoundaryInfo(Reference<DDConfiguration::RangeConfigMapSnapshot> snapshot)
  : snapshot(snapshot),
    boundaryLoaded(false) {}

void SpecialBoundaryInfo::reset(Reference<DDConfiguration::RangeConfigMapSnapshot> newSnapshot) {
	snapshot = newSnapshot;
	boundaryLoaded = false;
	boundaries.clear();
}

const std::vector<Key>& SpecialBoundaryInfo::getSpecialBoundaries() {
	if (boundaryLoaded)
		return boundaries;

	boundaries.clear();
	for (const auto it : snapshot->ranges()) {
		if (boundaries.empty())
			boundaries.push_back(it->range().begin);
		boundaries.push_back(it->range().end);
	}
	boundaryLoaded = true;
	return boundaries;
}

// whether range go across a boundary point
bool SpecialBoundaryInfo::violatesBoundary(KeyRangeRef range) {
	getSpecialBoundaries();
	auto it = std::upper_bound(boundaries.begin(), boundaries.end(), range.begin);
	if (it == boundaries.end()) {
		return false;
	}
	return range.contains(*it);
}

std::vector<KeyRangeRef> SpecialBoundaryInfo::splitRangeBySpecialBoundaries(KeyRangeRef keys) {
	getSpecialBoundaries();
	auto it = std::upper_bound(boundaries.begin(), boundaries.end(), keys.begin);
	return splitRangeByBoundaries(keys, it, boundaries.end(), [](decltype(it) it) { return *it; });
}

TEST_CASE("/Shard/SplitBoundary") {
	KeyRange range(KeyRangeRef("a"_sr, "o"_sr));
	Reference<DDConfiguration::RangeConfigMapSnapshot> snapshot(
	    new DDConfiguration::RangeConfigMapSnapshot(""_sr, "\xff"_sr));
	snapshot->map["b"_sr] = {};
	snapshot->map["c"_sr] = {};
	snapshot->map["z"_sr] = {};
	std::vector<Key> expBoundaries{ ""_sr, "b"_sr, "c"_sr, "z"_sr, "\xff"_sr };

	SpecialBoundaryInfo info(snapshot);
	auto& boundaries = info.getSpecialBoundaries();
	ASSERT(boundaries == expBoundaries);
	auto ranges = info.splitRangeBySpecialBoundaries(range);
	// a, b, c, o
	ASSERT_EQ(ranges.size(), 3);
	ASSERT_EQ(KeyRangeRef("a"_sr, "b"_sr), ranges[0]);
	ASSERT_EQ(KeyRangeRef("b"_sr, "c"_sr), ranges[1]);
	ASSERT_EQ(KeyRangeRef("c"_sr, "o"_sr), ranges[2]);
	KeyRange r1(KeyRangeRef("a"_sr, "o"_sr));
	ASSERT(info.violatesBoundary(r1));
	r1 = KeyRangeRef("z"_sr, "\xff"_sr);
	ASSERT(!info.violatesBoundary(r1));
	r1 = KeyRangeRef("d"_sr, "f"_sr);
	ASSERT(!info.violatesBoundary(r1));

	expBoundaries = std::vector<Key>{ ""_sr, "a"_sr, "c"_sr, "\xff"_sr };
	ranges = splitRangeByBoundaries(range, expBoundaries);
	// a, c, o
	ASSERT_EQ(ranges.size(), 2);
	ASSERT_EQ(KeyRangeRef("a"_sr, "c"_sr), ranges[0]);
	ASSERT_EQ(KeyRangeRef("c"_sr, "o"_sr), ranges[1]);

	expBoundaries = std::vector<Key>{ ""_sr, "a"_sr, "o"_sr, "\xff"_sr };
	ranges = splitRangeByBoundaries(range, expBoundaries);
	// a, o
	ASSERT_EQ(ranges.size(), 1);
	ASSERT_EQ(range, ranges[0]);

	return Void();
}
