/*
 * BulkLoadMetadata.h
 *
 * Copyright (c) 2023 Snowflake Computing
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

// 0x02 -> not in transaction state store
// versionstamp of start for active bulk loads
extern const KeyRangeRef bulkLoadVersionKeys;
typedef KeyBackedMap<BulkLoadID, Versionstamp, NullCodec, BinaryCodec<Versionstamp>> VersionMap;

// ranges for active (not completed or aborted) bulk loads
extern const KeyRangeRef bulkLoadRangeKeys;
typedef KeyBackedRangeMap<Standalone<StringRef>, IDValue, NullCodec, ObjectCodec<IDValue, _IncludeVersion>> RangeMap;
typedef RangeMap::LocalSnapshot RangeMapSnapshot;

WatchableTrigger& onChange();
StatusMap& statusMap();
InfoMap& infoMap();
VersionMap& versionMap();
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