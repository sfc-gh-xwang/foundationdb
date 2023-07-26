
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

// This file (to distinguish from BulkLoadMetadata.h) only contains struct definitions and primitives for bulk loading.

using BulkLoadID = Standalone<StringRef>;

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
};

struct BulkLoadInfo {
	constexpr static FileIdentifier file_identifier = 3119155;
	int64_t tenantId;
	KeyRange keyRange;

	// noarg for serialization
	BulkLoadInfo() {}

	BulkLoadInfo(int64_t tenantId, KeyRange keyRange) : tenantId(tenantId), keyRange(keyRange) {
		ASSERT(!keyRange.empty());
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantId, keyRange);
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