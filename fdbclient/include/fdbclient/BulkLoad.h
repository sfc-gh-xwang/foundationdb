
/*
 * BulkLoad.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once
#ifndef FDBCLIENT_BULKLOAD_H
#define FDBCLIENT_BULKLOAD_H

#include "flow/Arena.h"
#include "flow/CompressionUtils.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/Tenant.h"

// This file (to distinguish from BulkLoadMetadata.h) only contains struct definitions and primitives for bulk loading.

using BulkLoadID = Standalone<StringRef>;
using BulkLoadIDRef = StringRef;
using BulkLoadFileID = Standalone<StringRef>;
using BulkLoadFileIDRef = StringRef;

// key is logical tuple of tenantid, bulk load id. Tenant id is -1 when not set.
using BulkLoadIDKey = std::pair<int64_t, BulkLoadID>;

// Generally ordered such that valid state transitions only increase the number
// leaving space for future states with int gaps
// Everything after STAGING are internal states of staging that are controlled by FDB. STARTED -> STAGING is done by the
// client once they're done staging data.

// defined as struct and serialized with flatbuffers to be evolvable
class BulkLoadStatus {
public:
	constexpr static FileIdentifier file_identifier = 1934635;
	enum class State : uint8_t {
		STARTED = 0,
		STAGING = 10,
		STAGING_BLOB_DONE = 20,
		STAGING_DDPREPARED = 30,
		DONE = 90,
		ABORTED = 99,
		ABORTED_COMPLETE = 109,
		CLEANUP = 250, // never persisted in DB (row deleted instead), just used for state transition logic
		UNKNOWN = 255 // dummy type used for noargs constructor, should never be seen in practice
	};

	State state;

	// noarg for serialization
	BulkLoadStatus() : state(State::UNKNOWN) {}

	BulkLoadStatus(State state) : state(state) { ASSERT(state != State::UNKNOWN); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, state);
	}

	std::string toStateString() const {
		switch (state) {
		case State::STARTED:
			return "Started";
		case State::STAGING:
			return "Staging";
		case State::STAGING_BLOB_DONE:
			return "Staging blob done";
		case State::STAGING_DDPREPARED:
			return "Staging Prepared";
		case State::DONE:
			return "Done";
		case State::ABORTED:
			return "Aborted";
		case State::ABORTED_COMPLETE:
			return "Aborted Complete";
		case State::CLEANUP:
			return "Cleanup";
		default:
			ASSERT(true);
			return "Unknown";
		}
	}
};

struct BulkLoadInfo {
	constexpr static FileIdentifier file_identifier = 3119155;
	KeyRange keyRange;

	// noarg for serialization
	BulkLoadInfo() {}

	BulkLoadInfo(KeyRange keyRange) : keyRange(keyRange) { ASSERT(!keyRange.empty()); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange);
	}
};

enum class BulkLoadFileType : uint8_t { SNAPSHOT = 0, DELTA = 1 };
// file format (only row_v1 currently supported)
enum class BulkLoadFileFormat : uint8_t { ROW_V1 = 0, COLUMNAR_V1 = 1 };

struct BulkLoadFileToWriteRef {
	constexpr static FileIdentifier file_identifier = 5109804;
	// file id - pass back to fdb along with file metadata when file is successfully written to mark as registered
	BulkLoadFileIDRef fileId;

	// location to write file to
	StringRef fileName;

	// target size in bytes of file to be written
	int64_t targetSize;

	// The following are just config pass-throughs to the fdb client library on how the file should be written
	// Encryption keys to write file with (if enabled)
	Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx;

	BulkLoadFileType type;
	BulkLoadFileFormat format;
	// only matters for ROW_V1 format
	CompressionFilter compression;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, fileId, fileName, targetSize, cipherKeysCtx, type, format, compression);
	}
};

struct BulkLoadWrittenFileRef {
	constexpr static FileIdentifier file_identifier = 3671325;
	// id to register as written
	BulkLoadFileIDRef fileId;

	// post-write metadata about file
	KeyRangeRef keyRange;
	int64_t logicalSize;
	int64_t physicalSize;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, fileId, keyRange, logicalSize, physicalSize);
	}
};

template <>
struct Traceable<BulkLoadStatus::State> : std::true_type {
	// FIXME: more human-readable string for debugging
	static std::string toString(const BulkLoadStatus::State& st) { return std::to_string(static_cast<int>(st)); }
};

template <>
struct fmt::formatter<BulkLoadStatus::State> : FormatUsingTraceable<BulkLoadStatus::State> {};

#endif
