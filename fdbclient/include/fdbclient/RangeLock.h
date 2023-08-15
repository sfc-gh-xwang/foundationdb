
/*
 * RangeLock.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once
#ifndef FDBCLIENT_RANGELOCK_H
#define FDBCLIENT_RANGELOCK_H

#include "flow/Arena.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/KeyBackedRangeMap.actor.h"
#include "fdbclient/SystemData.h"

struct RangeLockInfo {
	constexpr static FileIdentifier file_identifier = 7289603;

	UID lockId; // For idempotent check
	enum Status { UNLOCKED = 0, LOCKED } status = UNLOCKED;

	RangeLockInfo() = default;
	RangeLockInfo(Status status, UID id) : lockId(id), status(status) {}

	static std::string statusString(const Status& status) {
		switch (status) {
		case UNLOCKED:
			return "unlocked";
		case LOCKED:
			return "locked";
		default:
			ASSERT(false);
		}
		return "";
	}

	std::string toString() const;
	json_spirit::mObject toJson() const;

	RangeLockInfo apply(const RangeLockInfo& info) const { return info; }
	bool operator==(RangeLockInfo const& rhs) const = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, lockId, status);
	}
};

template <>
struct Traceable<RangeLockInfo> : std::true_type {
	static std::string toString(const RangeLockInfo& info) { return info.toString(); }
};

template <>
struct fmt::formatter<RangeLockInfo> : FormatUsingTraceable<RangeLockInfo> {};

namespace RangeLockAPI {

bool isRangeLockError(const Error& e);

// Whether allow the range to be read / written. This range is in txn state store.
// \xff/rangeLock/[beginKey] = [RangeLockInfo]
extern const KeyRangeRef rangeLockKeys;

// \xff\x02/rangeLockMetadata/
extern const KeyRef rangeLockMetadataPrefix;

typedef KeyBackedRangeMap<Key, RangeLockInfo, NullCodec, ObjectCodec<RangeLockInfo, decltype(IncludeVersion())>>
    RangeLockMap;
typedef RangeLockMap::LocalSnapshot RangeLockMapSnapshot;
typedef RangeLockMap::RangeValue RangeLockRangeValue;
RangeLockMap& rangeLockMap();

// The rawKey is the binary string we write directly to kv store.
// Basically, the rangeLock map will encode the KeyType object A into string B, then prepend map prefix (aka.
// B.withPrefix(mapPrefix)) to get the rawKey. The return value is the map key A.
inline Key unpackRangeLockMapKey(Key rawKey) {
	return rangeLockMap().unpack(rawKey);
}

inline bool rangeLockChanging(MutationRef const& mutation) {
	if (isSingleKeyMutation((MutationRef::Type)mutation.type) && mutation.param1.startsWith(rangeLockKeys.begin)) {
		return true;
	} else if (mutation.type == MutationRef::ClearRange &&
	           rangeLockKeys.intersects(KeyRangeRef(mutation.param1, mutation.param2))) {
		return true;
	}
	return false;
}

// give an in-memory keyRangeMap of range locks. return whether there's no locked ranges
inline bool noLockedRange(KeyRangeMap<RangeLockInfo>* keyRangeMap) {
	return keyRangeMap->size() == 1 && keyRangeMap->nthRange(0).cvalue().status == RangeLockInfo::UNLOCKED;
}

struct RangeLockMetadata : public KeyBackedClass {

	RangeLockMetadata() : KeyBackedClass(SystemKey(rangeLockMetadataPrefix)){};

	KeyBackedBinaryValue<int> rangeLockCount() const;
};

RangeLockMetadata& rangeLockMetadata();

} // namespace RangeLockAPI

#endif