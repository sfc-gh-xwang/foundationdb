/*
 * BulkLoadMetadata.h
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

#pragma once
#ifndef FDBCLIENT_BULKLOADMETADATA_H
#define FDBCLIENT_BULKLOADMETADATA_H

#import "fdbclient/BulkLoad.h"
#import "fdbclient/KeyBackedTypes.actor.h"
#import "fdbclient/KeyBackedRangeMap.actor.h"

namespace BulkLoadMetadata {

// for use in range map. Can't just be BulkLoadID because it needs apply() function
// FIXME: should we just make a default implementation of apply() instead?
struct IDValue {
	constexpr static FileIdentifier file_identifier = 2178156;
	BulkLoadID id;

	IDValue() {}
	IDValue(BulkLoadID id) : id(id) {}

	IDValue apply(IDValue const& update) const {
		IDValue result = *this;
		result.id = update.id;
		return result;
	}

	bool operator==(const IDValue& that) const { return id == that.id; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id);
	}

	std::string toString() const { return id.toString(); }

	std::string printable() const { return id.printable(); }
};

// 0x02 - not in transaction state store
// bulk load status keys - updated every time the state of a bulk load changes
extern const KeyRangeRef bulkLoadStatusKeys;
typedef KeyBackedObjectMap<BulkLoadID, BulkLoadStatus, _IncludeVersion, NullCodec> StatusMap;

// 0x02 - not in transaction state store
// metadata for active (not completed or aborted) bulk loads
// FIXME: make these versionstamped!
extern const KeyRangeRef bulkLoadInfoKeys;
typedef KeyBackedObjectMap<BulkLoadID, BulkLoadInfo, _IncludeVersion, NullCodec> InfoMap;

// set whenever a new bulk load is added, or an old one is cleaned up (changed by writes to bulkLoadInfoKeys)
extern const StringRef bulkLoadChangeKey;

// no \x02 -> in transaction state store (so proxies can check it for locking)
// ranges for active (not completed or aborted) bulk loads
extern const KeyRangeRef bulkLoadRangeKeys;
typedef KeyBackedRangeMap<Standalone<StringRef>, IDValue, NullCodec, ObjectCodec<IDValue, _IncludeVersion>> RangeMap;
typedef RangeMap::LocalSnapshot RangeMapSnapshot;

WatchableTrigger& onChange();
StatusMap& statusMap();
InfoMap& infoMap();
RangeMap& rangeMap();

}; // namespace BulkLoadMetadata

bool checkValidBulkLoadStateTransition(BulkLoadState current, BulkLoadState desired);

template <>
struct Traceable<BulkLoadMetadata::IDValue> : std::true_type {
	static std::string toString(const BulkLoadMetadata::IDValue& idv) { return idv.toString(); }
};

template <>
struct fmt::formatter<BulkLoadMetadata::IDValue> : FormatUsingTraceable<BulkLoadMetadata::IDValue> {};

#endif