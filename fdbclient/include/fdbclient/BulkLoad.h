
/*
 * BulkLoad.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once
#ifndef FDBCLIENT_BULKLOAD_H
#define FDBCLIENT_BULKLOAD_H

#include "flow/Arena.h"
#include "fdbclient/FDBTypes.h"

using BulkLoadID = Standalone<StringRef>;

// Generally ordered such that valid state transitions only increase the number
// leaving space for future states with int gaps
// Everything after STAGING are internal states of staging that are controlled by FDB. STARTED -> STAGING is done by the
// client once they're done staging data.
enum class BulkLoadState : uint8_t {
	STARTED = 0,
	STAGING = 10,
	STAGING_BLOB_DONE = 20,
	STAGING_DDPREPARED = 30,
	DONE = 90,
	ABORTED = 99,
	ABORTED_COMPLETE = 109,
	CLEANUP = 254, // never persisted in DB (row deleted instead), just used for state transition logic
	UNKNOWN = 255 // dummy type used for noargs constructor, should never be seen in practice
};

// defined as struct and serialized with flatbuffers to be evolvable
struct BulkLoadStatus {
	constexpr static FileIdentifier file_identifier = 1934635;
	BulkLoadState state;

	// noarg for serialization
	BulkLoadStatus() : state(BulkLoadState::UNKNOWN) {}

	BulkLoadStatus(BulkLoadState state) : state(state) { ASSERT(state != BulkLoadState::UNKNOWN); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, state);
	}
};

struct BulkLoadInfo {
	constexpr static FileIdentifier file_identifier = 3119155;
	int64_t tenantId;
	KeyRange keyRange;
	// FIXME: make versionstamp and fill it somehow?
	Version createVersion;

	// noarg for serialization
	BulkLoadInfo() {}

	BulkLoadInfo(int64_t tenantId, KeyRange keyRange) : tenantId(tenantId), keyRange(keyRange), createVersion(0) {
		ASSERT(!keyRange.empty());
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantId, keyRange, createVersion);
	}
};

template <>
struct Traceable<BulkLoadState> : std::true_type {
	static std::string toString(const BulkLoadState& idv) { return std::to_string(static_cast<int>(idv)); }
};

template <>
struct fmt::formatter<BulkLoadState> : FormatUsingTraceable<BulkLoadState> {};

#endif