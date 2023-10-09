/*
 * BulkLoadMetadata.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once
#ifndef FDBCLIENT_BULKLOADMETADATA_H
#define FDBCLIENT_BULKLOADMETADATA_H

#import "fdbclient/BulkLoad.h"
#import "fdbclient/BulkLoaderInterface.h"
#import "fdbclient/KeyBackedTypes.actor.h"
#import "fdbclient/KeyBackedRangeMap.actor.h"

// This file (to distinguish from BulkLoad.h) contains the system keyspace data and namespaces and methods for
// interacting with them.

namespace BulkLoadMetadata {

// bulk load status keys - updated every time the state of a bulk load changes
extern const KeyRangeRef bulkLoadStatusKeys;
typedef KeyBackedObjectMap<BulkLoadIDKey, BulkLoadStatus, _IncludeVersion, TupleCodec<BulkLoadIDKey>> StatusMap;

// metadata for active (not completed or aborted) bulk loads
extern const KeyRangeRef bulkLoadInfoKeys;
typedef KeyBackedObjectMap<BulkLoadIDKey, BulkLoadInfo, _IncludeVersion, TupleCodec<BulkLoadIDKey>> InfoMap;

// set whenever a new bulk load is added, or an old one is cleaned up (changed by writes to bulkLoadInfoKeys)
extern const StringRef bulkLoadChangeKey;

// versionstamp of start for active bulk loads
extern const KeyRangeRef bulkLoadVersionKeys;
typedef KeyBackedMap<BulkLoadIDKey, Versionstamp, TupleCodec<BulkLoadIDKey>, BinaryCodec<Versionstamp>> VersionMap;

// ranges for active (not completed or aborted) bulk loads
extern const KeyRangeRef bulkLoadRangeKeys;
typedef KeyBackedRangeMap<Standalone<StringRef>, BulkLoadID, NullCodec, ObjectCodec<BulkLoadID, _IncludeVersion>>
    RangeMap;
typedef RangeMap::LocalSnapshot RangeMapSnapshot;

// reservations for active bulk loads
extern const KeyRangeRef bulkLoadReservationKeys;
typedef KeyBackedMap<Standalone<StringRef>, int64_t, NullCodec, BinaryCodec<int64_t>> ReservationMap;

// interfaces for active bulk loader endpoints
extern const KeyRangeRef bulkLoaderListKeys;
typedef KeyBackedObjectMap<UID, BulkLoaderInterface, _IncludeVersion, BinaryCodec<UID>> LoaderListMap;

// structs stored in the database for created + written file pointers

// essentially pointer logged in db that will be cleaned up if client didn't use it
struct CreatedFilePointerRef {
	constexpr static FileIdentifier file_identifier = 1207542;
	StringRef filename;
	BulkLoadFileType type;
	// encryption metadata later to be stored in bgFileKeys
	Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta;

	int expectedSize() const {
		// FIXME: doesn't have cipherKeys
		return filename.size();
	}

	CreatedFilePointerRef() {}
	CreatedFilePointerRef(Arena& to, CreatedFilePointerRef other)
	  : filename(to, other.filename),
	    type(other.type),
	    cipherKeysMeta(other.cipherKeysMeta) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, filename, type, cipherKeysMeta);
	}
};

// superset of created pointer that also includes post-write metadata
struct WrittenFilePointerRef {
	constexpr static FileIdentifier file_identifier = 8942399;
	CreatedFilePointerRef createdPointer;

	// metadata provided after write
	KeyRangeRef keyRange;
	int64_t logicalSize;
	int64_t physicalSize;

	WrittenFilePointerRef() {}
	WrittenFilePointerRef(Arena& to, WrittenFilePointerRef other)
	  : createdPointer(to, other.createdPointer),
	    logicalSize(other.logicalSize),
	    physicalSize(other.physicalSize) {}

	int expectedSize() const { return createdPointer.expectedSize() + keyRange.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, createdPointer, keyRange, logicalSize, physicalSize);
	}
};

// created file pointers
typedef std::pair<BulkLoadID, BulkLoadFileID> FilePointerKey;

extern const KeyRangeRef bulkLoadCreatedFilePointersKeys;
typedef KeyBackedObjectMap<FilePointerKey, Standalone<CreatedFilePointerRef>, _IncludeVersion> CreatedFilePointersMap;

// written file pointers
extern const KeyRangeRef bulkLoadWrittenFilePointersKeys;
typedef KeyBackedObjectMap<FilePointerKey, Standalone<WrittenFilePointerRef>, _IncludeVersion> WrittenFilePointersMap;

WatchableTrigger& onChange();
StatusMap& statusMap();
InfoMap& infoMap();
VersionMap& versionMap();
RangeMap& rangeMap();
ReservationMap& reservationMap();
LoaderListMap& loaderListMap();
CreatedFilePointersMap& createdFilePointersMap();
WrittenFilePointersMap& writtenFilePointersMap();

std::pair<FilePointerKey, FilePointerKey> allFilePointersForLoadRange(const BulkLoadID& id);

}; // namespace BulkLoadMetadata

bool checkValidBulkLoadStateTransition(BulkLoadStatus::State current, BulkLoadStatus::State desired);

#endif