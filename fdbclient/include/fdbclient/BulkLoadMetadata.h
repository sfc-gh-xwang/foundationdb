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

// This file (to distinguish from BulkLoad.h) contains the system keyspace data and namespaces and methods for
// interacting with them.

namespace BulkLoadMetadata {

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
typedef KeyBackedRangeMap<Standalone<StringRef>, BulkLoadID, NullCodec, ObjectCodec<BulkLoadID, _IncludeVersion>>
    RangeMap;
typedef RangeMap::LocalSnapshot RangeMapSnapshot;

WatchableTrigger& onChange();
StatusMap& statusMap();
InfoMap& infoMap();
VersionMap& versionMap();
RangeMap& rangeMap();

}; // namespace BulkLoadMetadata

bool checkValidBulkLoadStateTransition(BulkLoadStatus::State current, BulkLoadStatus::State desired);

#endif