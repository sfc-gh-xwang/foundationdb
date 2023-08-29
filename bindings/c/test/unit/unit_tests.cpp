/*
 * unit_tests.cpp
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

// Unit tests for the FoundationDB C API.

#include "fdb_c_options.g.h"
#define FDB_USE_LATEST_API_VERSION
#include <assert.h>
#include <string.h>

#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <semaphore>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>
#include <random>
#include <chrono>

#define DOCTEST_CONFIG_IMPLEMENT
#include <rapidjson/document.h>
#include "doctest.h"
#include "fdbclient/Tracing.h"
#include "fdbclient/Tuple.h"

#include "flow/config.h"
#include "flow/DeterministicRandom.h"
#include "flow/IRandom.h"

#include "test/fdb_api.hpp"

using namespace fdb::literals; // use "[key]"_br shorthand for fdb::BytesRef

namespace {

void fdbCheck(const fdb::Error& err) {
	if (err) {
		std::cerr << err.what() << std::endl;
		std::abort();
	}
}

fdb::Error waitFuture(fdb::Future& f) {
	fdbCheck(f.blockUntilReady());
	return f.error();
}

static fdb::Database db;
static std::string prefix;
static std::string clusterFilePath = "";

std::string key(const std::string& key) {
	return prefix + key;
}

// Given a string s, returns the "lowest" string greater than any string that
// starts with s. Taken from
// https://github.com/apple/foundationdb/blob/e7d72f458c6a985fdfa677ae021f357d6f49945b/flow/flow.cpp#L223.
std::string strinc_str(const std::string& s) {
	int index = -1;
	for (index = s.size() - 1; index >= 0; --index) {
		if ((uint8_t)s[index] != 255) {
			break;
		}
	}

	assert(index >= 0);

	std::string r = s.substr(0, index + 1);
	char* p = r.data();
	p[r.size() - 1]++;
	return r;
}

// Helper function to add `prefix` to all keys in the given map. Returns a new
// map.
std::map<std::string, std::string> create_data(std::map<std::string, std::string>&& map) {
	std::map<std::string, std::string> out;
	for (const auto& [key, val] : map) {
		out[prefix + key] = val;
	}
	return out;
}

// Clears all data in the database, then inserts the given key value pairs.
void insert_data(fdb::Database db, const std::map<std::string, std::string>& data) {
	auto tr = db.createTransaction();
	auto end_key = strinc_str(prefix);
	while (1) {
		tr.clearRange(fdb::toBytesRef(prefix), fdb::toBytesRef(end_key));
		for (const auto& [key, val] : data) {
			tr.set(fdb::toBytesRef(key), fdb::toBytesRef(val));
		}

		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}
}

// Get the value associated with `key_name` from the database. Accepts a list
// of transaction options to apply (values for options not supported). Returns
// an optional which will be populated with the result if one was found.
std::optional<std::string> getValue(fdb::KeyRef key, bool snapshot, std::vector<FDBTransactionOption> options) {
	auto tr = db.createTransaction();
	while (1) {
		for (auto& option : options) {
			tr.setOption(option);
		}
		auto f1 = tr.get(key, snapshot);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto value = f1.get();
		return value.has_value() ? std::make_optional(std::string(value->begin(), value->end())) : std::nullopt;
	}
}

struct GetMappedRangeResult {
	struct MappedKV {
		MappedKV(const std::string& key,
		         const std::string& value,
		         const std::string& begin,
		         const std::string& end,
		         const std::vector<std::pair<std::string, std::string>>& range_results)
		  : key(key),
		    value(value),
		    begin(begin),
		    end(end),
		    range_results(range_results) {}

		std::string key;
		std::string value;
		std::string begin;
		std::string end;
		std::vector<std::pair<std::string, std::string>> range_results;
	};
	std::vector<MappedKV> mkvs;
	// True if values remain in the key range requested.
	bool more;
	// Set to a non-zero value if an error occurred during the transaction.
	fdb::Error err;
};

static inline std::string extractString(fdb::KeyRef key) {
	return std::string(key.begin(), key.end());
}

GetMappedRangeResult getMappedRange(fdb::Transaction& tr,
                                    fdb::KeySelector begin,
                                    fdb::KeySelector end,
                                    fdb::KeyRef mapperName,
                                    int limit,
                                    int target_bytes,
                                    FDBStreamingMode mode,
                                    int iteration,
                                    int matchIndex,
                                    bool snapshot,
                                    bool reverse) {
	auto f1 =
	    tr.getMappedRange(begin, end, mapperName, limit, target_bytes, mode, iteration, matchIndex, snapshot, reverse);
	auto err = waitFuture(f1);
	if (err) {
		return GetMappedRangeResult{ {}, false, err };
	}

	auto [outMkv, outCount, outMore] = f1.get();

	GetMappedRangeResult result;
	result.more = (outMore != 0);
	result.err = fdb::Error::success();

	//	std::cout << "out_count:" << out_count << " out_more:" << out_more << " out_mkv:" << (void*)out_mkv <<
	// std::endl;

	for (int i = 0; i < outCount; ++i) {
		fdb::MappedKeyValueRef mkv = outMkv[i];
		auto key = extractString(mkv.key());
		auto value = extractString(mkv.value());
		auto begin = extractString(mkv.rangeBeginKey());
		auto end = extractString(mkv.rangeEndKey());
		//		std::cout << "key:" << key << " value:" << value << " begin:" << begin << " end:" << end << std::endl;

		std::vector<std::pair<std::string, std::string>> range_results;
		for (int i = 0; i < mkv.rangeSize(); ++i) {
			const auto& kv = mkv.rangeKeyValue(i);
			std::string k(kv.key().begin(), kv.key().end());
			std::string v(kv.value().begin(), kv.value().end());
			range_results.emplace_back(k, v);
			// std::cout << "[" << i << "]" << k << " -> " << v << std::endl;
		}
		result.mkvs.emplace_back(key, value, begin, end, range_results);
	}
	return result;
}

// Clears all data in the database.
void clear_data(fdb::Database db) {
	insert_data(db, {});
}

enum TenantOp { CREATE, DELETE };

void tenantHelper(fdb::BytesRef name, TenantOp op) {
	// Helper functin to create | delete a tenant
	auto tr = db.createTransaction();
	while (1) {
		switch (op) {
		case TenantOp::CREATE:
			fdb::Tenant::createTenant(tr, name);
			break;
		case TenantOp::DELETE:
			fdb::Tenant::deleteTenant(tr, name);
			break;
		default:
			UNREACHABLE();
		}
		auto f1 = tr.commit();

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}
}

void createTenant(fdb::BytesRef name) {
	tenantHelper(name, TenantOp::CREATE);
}

void deleteTenant(fdb::BytesRef name) {
	tenantHelper(name, TenantOp::DELETE);
}

void checkClientStatusFuture(fdb::TypedFuture<fdb::future_var::KeyRef> f) {
	// Sanity check of the client status.
	fdbCheck(waitFuture(f));
	auto value = f.get();
	std::string statusJsonStr(value.begin(), value.end());
	rapidjson::Document statusJson;
	statusJson.Parse(statusJsonStr.c_str());
	CHECK(statusJson.HasMember("Healthy"));
	CHECK(statusJson["Healthy"].GetBool());
}

} // namespace

TEST_CASE("strinc_str") {
	CHECK(strinc_str("a").compare("b") == 0);
	CHECK(strinc_str("y").compare("z") == 0);
	CHECK(strinc_str("!").compare("\"") == 0);
	CHECK(strinc_str("*").compare("+") == 0);
	CHECK(strinc_str("fdb").compare("fdc") == 0);
	CHECK(strinc_str("foundation database 6").compare("foundation database 7") == 0);

	char terminated[] = { 'a', 'b', '\xff' };
	CHECK(strinc_str(std::string(terminated, 3)).compare("ac") == 0);
}

//
// Setting the transaction size limit as a database option causes issues when
// outside the bounds of acceptable values. TODO: Needs investigating...
// TEST_CASE("FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT too small") {
//   // Size limit must be at least 32 to be valid, so test a smaller size.
//   int64_t size_limit = 31;
//   fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT,
//                                     (const uint8_t *)&size_limit,
//                                     sizeof(size_limit)));
//
//   fdb::Transaction tr(db);
//   tr.set((const uint8_t *)"foo", 3, (const uint8_t *)"bar", 3);
//   fdb::EmptyFuture f1 = tr.commit();
//
//   CHECK(wait_future(f1) == 2006); // invalid_option_value
//
//   // Set size limit back to default.
//   size_limit = 10000000;
//   fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT,
//                                     (const uint8_t *)&size_limit,
//                                     sizeof(size_limit)));
// }

// TEST_CASE("FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT too large") {
//   // Size limit must be less than or equal to 10,000,000.
//   int64_t size_limit = 10000001;
//   fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT,
//                                     (const uint8_t *)&size_limit,
//                                     sizeof(size_limit)));
//
//   fdb::Transaction tr(db);
//   tr.set((const uint8_t *)"foo", 3, (const uint8_t *)"bar", 3);
//   fdb::EmptyFuture f1 = tr.commit();
//
//   CHECK(wait_future(f1) == 2006); // invalid_option_value
//
//   // Set size limit back to default.
//   size_limit = 10000000;
//   fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT,
//                                     (const uint8_t *)&size_limit,
//                                     sizeof(size_limit)));
// }

const std::string EMPTY = Tuple().pack().toString();
const KeyRef RECORD = "RECORD"_sr;
const KeyRef INDEX = "INDEX"_sr;
static Key primaryKey(const int i) {
	return Key(format("primary-key-of-record-%08d", i));
}
static Key indexKey(const int i) {
	return Key(format("index-key-of-record-%08d", i));
}
static Value dataOfRecord(const int i) {
	return Value(format("data-of-record-%08d", i));
}
static std::string indexEntryKey(const int i) {
	return Tuple::makeTuple(prefix, INDEX, indexKey(i), primaryKey(i)).pack().toString();
}
static std::string recordKey(const int i, const int split) {
	return Tuple::makeTuple(prefix, RECORD, primaryKey(i), split).pack().toString();
}
static std::string recordValue(const int i, const int split) {
	return Tuple::makeTuple(dataOfRecord(i), split).pack().toString();
}

const static int SPLIT_SIZE = 3;
std::map<std::string, std::string> fillInRecords(int n) {
	// Note: The user requested `prefix` should be added as the first element of the tuple that forms the key, rather
	// than the prefix of the key. So we don't use key() or create_data() in this test.
	std::map<std::string, std::string> data;
	for (int i = 0; i < n; i++) {
		data[indexEntryKey(i)] = EMPTY;
		for (int split = 0; split < SPLIT_SIZE; split++) {
			data[recordKey(i, split)] = recordValue(i, split);
		}
	}
	insert_data(db, data);
	return data;
}

GetMappedRangeResult getMappedIndexEntries(int beginId,
                                           int endId,
                                           fdb::Transaction& tr,
                                           std::string mapper,
                                           int matchIndex) {
	std::string indexEntryKeyBegin = indexEntryKey(beginId);
	std::string indexEntryKeyEnd = indexEntryKey(endId);

	return getMappedRange(tr,
	                      fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(indexEntryKeyBegin)),
	                      fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(indexEntryKeyEnd)),
	                      fdb::toBytesRef(mapper),
	                      /* limit */ 0,
	                      /* target_bytes */ 0,
	                      /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
	                      /* iteration */ 0,
	                      /* matchIndex */ matchIndex,
	                      /* snapshot */ false,
	                      /* reverse */ 0);
}

GetMappedRangeResult getMappedIndexEntries(int beginId,
                                           int endId,
                                           fdb::Transaction& tr,
                                           int matchIndex,
                                           bool allMissing) {
	std::string mapper =
	    Tuple::makeTuple(prefix, RECORD, (allMissing ? "{K[2]}"_sr : "{K[3]}"_sr), "{...}"_sr).pack().toString();
	return getMappedIndexEntries(beginId, endId, tr, mapper, matchIndex);
}

TEST_CASE("versionstamp_unit_test") {
	// a random 12 bytes long StringRef as a versionstamp
	StringRef str = "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x11\x12"_sr;
	TupleVersionstamp vs(str), vs2(str);
	ASSERT(vs == vs2);
	ASSERT(vs.begin() != vs2.begin());

	int64_t version = vs.getVersion();
	int64_t version2 = vs2.getVersion();
	int64_t versionExpected = ((int64_t)0x01 << 56) + ((int64_t)0x02 << 48) + ((int64_t)0x03 << 40) +
	                          ((int64_t)0x04 << 32) + (0x05 << 24) + (0x06 << 16) + (0x07 << 8) + 0x08;
	ASSERT(version == versionExpected);
	ASSERT(version2 == versionExpected);

	int16_t batch = vs.getBatchNumber();
	int16_t batch2 = vs2.getBatchNumber();
	int16_t batchExpected = (0x09 << 8) + 0x10;
	ASSERT(batch == batchExpected);
	ASSERT(batch2 == batchExpected);

	int16_t user = vs.getUserVersion();
	int16_t user2 = vs2.getUserVersion();
	int16_t userExpected = (0x11 << 8) + 0x12;
	ASSERT(user == userExpected);
	ASSERT(user2 == userExpected);

	ASSERT(vs.size() == VERSIONSTAMP_TUPLE_SIZE);
	ASSERT(vs2.size() == VERSIONSTAMP_TUPLE_SIZE);
}

TEST_CASE("tuple_support_versionstamp") {
	// a random 12 bytes long StringRef as a versionstamp
	StringRef str = "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x11\x12"_sr;
	TupleVersionstamp vs(str);
	const Tuple t = Tuple::makeTuple(prefix, RECORD, vs, "{K[3]}"_sr, "{...}"_sr);
	ASSERT(t.getVersionstamp(2) == vs);

	// verify the round-way pack-unpack path for a Tuple containing a versionstamp
	StringRef result1 = t.pack();
	Tuple t2 = Tuple::unpack(result1);
	StringRef result2 = t2.pack();
	ASSERT(t2.getVersionstamp(2) == vs);
	ASSERT(result1.toString() == result2.toString());
}

TEST_CASE("tuple_fail_to_append_truncated_versionstamp") {
	// a truncated 11 bytes long StringRef as a versionstamp
	StringRef str = "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x11"_sr;
	try {
		TupleVersionstamp truncatedVersionstamp(str);
	} catch (Error& e) {
		return;
	}
	UNREACHABLE();
}

TEST_CASE("tuple_fail_to_append_longer_versionstamp") {
	// a longer than expected 13 bytes long StringRef as a versionstamp
	StringRef str = "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x11"_sr;
	try {
		TupleVersionstamp longerVersionstamp(str);
	} catch (Error& e) {
		return;
	}
	UNREACHABLE();
}

TEST_CASE("fdb_transaction_getMappedRange") {
	const int TOTAL_RECORDS = 20;
	fillInRecords(TOTAL_RECORDS);

	auto tr = db.createTransaction();
	// RYW should be enabled.
	while (1) {
		int beginId = 1;
		int endId = 19;
		const double r = deterministicRandom()->random01();
		int matchIndex = MATCH_INDEX_ALL;
		if (r < 0.25) {
			matchIndex = MATCH_INDEX_NONE;
		} else if (r < 0.5) {
			matchIndex = MATCH_INDEX_MATCHED_ONLY;
		} else if (r < 0.75) {
			matchIndex = MATCH_INDEX_UNMATCHED_ONLY;
		}
		auto result = getMappedIndexEntries(beginId, endId, tr, matchIndex, false);

		if (result.err) {
			auto f1 = tr.onError(result.err);
			fdbCheck(waitFuture(f1));
			continue;
		}

		int expectSize = endId - beginId;
		CHECK(result.mkvs.size() == expectSize);
		CHECK(!result.more);

		int id = beginId;
		for (int i = 0; i < expectSize; i++, id++) {
			const auto& mkv = result.mkvs[i];
			if (matchIndex == MATCH_INDEX_ALL || i == 0 || i == expectSize - 1) {
				CHECK(indexEntryKey(id).compare(mkv.key) == 0);
			} else if (matchIndex == MATCH_INDEX_MATCHED_ONLY) {
				CHECK(indexEntryKey(id).compare(mkv.key) == 0);
			} else if (matchIndex == MATCH_INDEX_UNMATCHED_ONLY) {
				CHECK(EMPTY.compare(mkv.key) == 0);
			} else {
				CHECK(EMPTY.compare(mkv.key) == 0);
			}
			CHECK(EMPTY.compare(mkv.value) == 0);
			CHECK(mkv.range_results.size() == SPLIT_SIZE);
			for (int split = 0; split < SPLIT_SIZE; split++) {
				auto& kv = mkv.range_results[split];
				CHECK(recordKey(id, split).compare(kv.first) == 0);
				CHECK(recordValue(id, split).compare(kv.second) == 0);
			}
		}
		break;
	}
}

TEST_CASE("fdb_transaction_getMappedRange_missing_all_secondary") {
	const int TOTAL_RECORDS = 20;
	fillInRecords(TOTAL_RECORDS);

	auto tr = db.createTransaction();
	// RYW should be enabled.
	while (1) {
		int beginId = 1;
		int endId = 19;
		const double r = deterministicRandom()->random01();
		int matchIndex = MATCH_INDEX_ALL;
		if (r < 0.25) {
			matchIndex = MATCH_INDEX_NONE;
		} else if (r < 0.5) {
			matchIndex = MATCH_INDEX_MATCHED_ONLY;
		} else if (r < 0.75) {
			matchIndex = MATCH_INDEX_UNMATCHED_ONLY;
		}
		auto result = getMappedIndexEntries(beginId, endId, tr, matchIndex, true);

		if (result.err) {
			auto f1 = tr.onError(result.err);
			fdbCheck(waitFuture(f1));
			continue;
		}

		int expectSize = endId - beginId;
		CHECK(result.mkvs.size() == expectSize);
		CHECK(!result.more);

		int id = beginId;
		for (int i = 0; i < expectSize; i++, id++) {
			const auto& mkv = result.mkvs[i];
			if (matchIndex == MATCH_INDEX_ALL || i == 0 || i == expectSize - 1) {
				CHECK(indexEntryKey(id).compare(mkv.key) == 0);
			} else if (matchIndex == MATCH_INDEX_MATCHED_ONLY) {
				CHECK(EMPTY.compare(mkv.key) == 0);
			} else if (matchIndex == MATCH_INDEX_UNMATCHED_ONLY) {
				CHECK(indexEntryKey(id).compare(mkv.key) == 0);
			} else {
				CHECK(EMPTY.compare(mkv.key) == 0);
			}
			CHECK(EMPTY.compare(mkv.value) == 0);
		}
		break;
	}
}

TEST_CASE("fdb_transaction_getMappedRange_restricted_to_serializable") {
	std::string mapper = Tuple::makeTuple(prefix, RECORD, "{K[3]}"_sr).pack().toString();
	auto tr = db.createTransaction();
	auto result = getMappedRange(tr,
	                             fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(indexEntryKey(0))),
	                             fdb::key_select::firstGreaterThan(fdb::toBytesRef(indexEntryKey(1))),
	                             fdb::toBytesRef(mapper),
	                             /* limit */ 0,
	                             /* target_bytes */ 0,
	                             /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
	                             /* iteration */ 0,
	                             /* matchIndex */ MATCH_INDEX_ALL,
	                             /* snapshot */ true, // Set snapshot to true
	                             /* reverse */ 0);
	ASSERT(result.err.code() == error_code_unsupported_operation);
}

TEST_CASE("fdb_transaction_getMappedRange_restricted_to_ryw_enable") {
	std::string mapper = Tuple::makeTuple(prefix, RECORD, "{K[3]}"_sr).pack().toString();
	auto tr = db.createTransaction();
	tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE); // Not disable RYW
	auto result = getMappedRange(tr,
	                             fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(indexEntryKey(0))),
	                             fdb::key_select::firstGreaterThan(fdb::toBytesRef(indexEntryKey(1))),
	                             fdb::toBytesRef(mapper),
	                             /* limit */ 0,
	                             /* target_bytes */ 0,
	                             /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
	                             /* iteration */ 0,
	                             /* matchIndex */ MATCH_INDEX_ALL,
	                             /* snapshot */ false,
	                             /* reverse */ 0);
	ASSERT(result.err.code() == error_code_unsupported_operation);
}

void assertNotTuple(std::string str) {
	try {
		Tuple::unpack(str);
	} catch (Error& e) {
		return;
	}
	UNREACHABLE();
}

TEST_CASE("fdb_transaction_get_mapped_range_fail_on_mapper_not_tuple") {
	// A string that cannot be parsed as tuple.
	//
	// "\x15:\x152\x15E\x15\x09\x15\x02\x02MySimpleRecord$repeater-version\x00\x15\x013\x00\x00\x00\x00\x1aU\x90\xba\x00\x00\x00\x02\x15\x04"
	// should fail at \x35
	std::string mapper = {
		'\x15', ':',    '\x15', '2', '\x15', 'E',    '\x15', '\t',   '\x15', '\x02', '\x02', 'M',
		'y',    'S',    'i',    'm', 'p',    'l',    'e',    'R',    'e',    'c',    'o',    'r',
		'd',    '$',    'r',    'e', 'p',    'e',    'a',    't',    'e',    'r',    '-',    'v',
		'e',    'r',    's',    'i', 'o',    'n',    '\x00', '\x15', '\x01', '\x35', '\x00', '\x00',
		'\x00', '\x00', '\x1a', 'U', '\x90', '\xba', '\x00', '\x00', '\x00', '\x02', '\x15', '\x04'
	};
	assertNotTuple(mapper);
	auto tr = db.createTransaction();
	auto result = getMappedIndexEntries(1, 3, tr, mapper, MATCH_INDEX_ALL);
	ASSERT(result.err.code() == error_code_mapper_not_tuple);
}

TEST_CASE("fdb_transaction_add_conflict_range") {
	bool success = false;

	bool retry = true;
	while (retry) {
		auto tr = db.createTransaction();
		while (1) {
			auto f1 = tr.getReadVersion();
			auto err = waitFuture(f1);
			if (err) {
				auto f2 = tr.onError(err);
				fdbCheck(waitFuture(f2));
				continue;
			}
			break;
		}

		auto tr2 = db.createTransaction();
		while (1) {
			tr2.addConflictRange(
			    fdb::toBytesRef(key("a")), fdb::toBytesRef(strinc_str(key("a"))), FDB_CONFLICT_RANGE_TYPE_WRITE);
			auto f1 = tr2.commit();
			auto err = waitFuture(f1);
			if (err) {
				auto f2 = tr2.onError(err);
				fdbCheck(waitFuture(f2));
				continue;
			}
			break;
		}

		while (1) {
			tr.addConflictRange(
			    fdb::toBytesRef(key("a")), fdb::toBytesRef(strinc_str(key("a"))), FDB_CONFLICT_RANGE_TYPE_READ);
			tr.addConflictRange(
			    fdb::toBytesRef(key("a")), fdb::toBytesRef(strinc_str(key("a"))), FDB_CONFLICT_RANGE_TYPE_WRITE);
			auto f1 = tr.commit();
			auto err = waitFuture(f1);
			if (err.code() == 1020) { // not_committed
				// Test should pass if transactions conflict.
				success = true;
				retry = false;
			} else if (err) {
				auto f2 = tr.onError(err);
				fdbCheck(waitFuture(f2));
				retry = true;
			} else {
				// If the transaction succeeded, something went wrong.
				CHECK(false);
				retry = false;
			}
			break;
		}
	}

	// Double check that failure was achieved and the loop wasn't just broken out
	// of.
	CHECK(success);
}

TEST_CASE("invalid trace parent") {
	auto tr = db.createTransaction();
	std::string invalidTraceParent = "invalid";
	auto err = tr.setOptionNothrow(FDB_TR_OPTION_TRACE_PARENT, invalidTraceParent);
	if (err.code() != 2006) { // invalid_option_value
		auto f1 = tr.commit();
		ASSERT_EQ(waitFuture(f1).code(), 2006);
	}
}

TEST_CASE("trace parent") {
	auto tr = db.createTransaction();
	SpanContext context(
	    deterministicRandom()->randomUniqueID(), deterministicRandom()->randomUInt64(), TraceFlags::unsampled);

	std::string traceParent = context.toString();

	tr.setOption(FDB_TR_OPTION_TRACE_PARENT, traceParent);
	while (1) {
		auto f1 = tr.get("\xff\xff/tracing/transaction_id"_br, /* snapshot */ false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto output = f1.get();
		REQUIRE(output.has_value());
		UID transactionID = UID::fromString(std::string(output->begin(), output->end()));
		ASSERT_EQ(transactionID, context.traceID);
		break;
	}
}

std::string get_valid_status_json() {
	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.get("\xff\xff/status/json"_br, false);
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto value = f1.get();
		assert(value.has_value());
		std::string statusJsonStr(value->begin(), value->end());
		rapidjson::Document statusJson;
		statusJson.Parse(statusJsonStr.c_str());
		// make sure it is available
		bool available = statusJson["client"]["database_status"]["available"].GetBool();
		if (!available)
			continue; // cannot reach to the cluster, retry
		return statusJsonStr;
	}
}

TEST_CASE("fdb_database_reboot_worker") {
#ifdef USE_TSAN
	MESSAGE(
	    "fdb_database_reboot_worker disabled for tsan, since fdbmonitor doesn't seem to restart the killed process");
	return;
#endif
	std::string status_json = get_valid_status_json();
	rapidjson::Document statusJson;
	statusJson.Parse(status_json.c_str());
	CHECK(statusJson.HasMember("cluster"));
	CHECK(statusJson["cluster"].HasMember("generation"));
	int old_generation = statusJson["cluster"]["generation"].GetInt();
	CHECK(statusJson["cluster"].HasMember("processes"));
	// Make sure we only have one process in the cluster
	// Thus, rebooting the worker ensures a recovery
	// Configuration changes may break the contract here
	CHECK(statusJson["cluster"]["processes"].MemberCount() == 1);
	auto processPtr = statusJson["cluster"]["processes"].MemberBegin();
	CHECK(processPtr->value.HasMember("address"));
	std::string networkAddress = processPtr->value["address"].GetString();
	while (1) {
		auto f = db.rebootWorker(fdb::toBytesRef(networkAddress), false, 0);
		fdbCheck(waitFuture(f));
		int64_t successful = f.get();
		if (successful)
			break; // retry rebooting until success
	}
	status_json = get_valid_status_json();
	statusJson.Parse(status_json.c_str());
	CHECK(statusJson.HasMember("cluster"));
	CHECK(statusJson["cluster"].HasMember("generation"));
	int new_generation = statusJson["cluster"]["generation"].GetInt();
	// The generation number should increase after the recovery
	CHECK(new_generation > old_generation);
}

TEST_CASE("fdb_database_force_recovery_with_data_loss") {
	// This command cannot be tested completely in the current unit test configuration
	// For now, we simply call the function to make sure it exist
	// Background:
	// It is also only usable when usable_regions=2, so it requires a fearless configuration
	// In particular, you have two data centers, and the storage servers in one region are allowed to fall behind (async
	// replication) Normally, you would not want to recover to that set of storage servers unless there are tlogs which
	// can let those storage servers catch up However, if all the tlogs are dead and you still want to be able to
	// recover your database even if that means losing recently committed mutation, that's the time this function works

	std::string dcid = "test_id";
	while (1) {
		auto f = db.forceRecoveryWithDataLoss(fdb::toBytesRef(dcid));
		fdbCheck(waitFuture(f));
		break;
	}
}

std::string random_hex_string(size_t length) {
	const char charset[] = "0123456789"
	                       "ABCDEF"
	                       "abcdef";
	// construct a random generator engine from a time-based seed:
	std::default_random_engine generator(time(nullptr));
	std::uniform_int_distribution<int> distribution(0, strlen(charset) - 1);
	auto randchar = [&charset, &generator, &distribution]() -> char { return charset[distribution(generator)]; };
	std::string str(length, 0);
	std::generate_n(str.begin(), length, randchar);
	return str;
}

TEST_CASE("fdb_database_create_snapshot") {
	std::string snapshot_command = "test";
	std::string uid = "invalid_uid";
	bool retry = false;
	while (1) {
		auto f = db.createSnapshot(fdb::toBytesRef(uid), fdb::toBytesRef(snapshot_command));
		auto err = waitFuture(f);
		if (err.code() == 2509) { // expected error code
			CHECK(!retry);
			uid = random_hex_string(32);
			retry = true;
		} else if (err.code() == 2505) {
			CHECK(retry);
			break;
		} else {
			// Otherwise, something went wrong.
			CHECK(false);
		}
	}
}

TEST_CASE("fdb_error_predicate") {
	CHECK(fdb::Error(1007).retryable()); // transaction_too_old
	CHECK(fdb::Error(1020).retryable()); // not_committed
	CHECK(fdb::Error(1038).retryable()); // database_locked

	CHECK(!fdb::Error(1036).retryable()); // accessed_unreadable
	CHECK(!fdb::Error(2000).retryable()); // client_invalid_operation
	CHECK(!fdb::Error(2004).retryable()); // key_outside_legal_range
	CHECK(!fdb::Error(2005).retryable()); // inverted_range
	CHECK(!fdb::Error(2006).retryable()); // invalid_option_value
	CHECK(!fdb::Error(2007).retryable()); // invalid_option
	CHECK(!fdb::Error(2011).retryable()); // version_invalid
	CHECK(!fdb::Error(2020).retryable()); // transaction_invalid_version
	CHECK(!fdb::Error(2023).retryable()); // transaction_read_only
	CHECK(!fdb::Error(2100).retryable()); // incompatible_protocol_version
	CHECK(!fdb::Error(2101).retryable()); // transaction_too_large
	CHECK(!fdb::Error(2102).retryable()); // key_too_large
	CHECK(!fdb::Error(2103).retryable()); // value_too_large
	CHECK(!fdb::Error(2108).retryable()); // unsupported_operation
	CHECK(!fdb::Error(2200).retryable()); // api_version_unset
	CHECK(!fdb::Error(4000).retryable()); // unknown_error
	CHECK(!fdb::Error(4001).retryable()); // internal_error

	CHECK(fdb::Error(1021).maybeCommitted()); // commit_unknown_result

	CHECK(!fdb::Error(1000).maybeCommitted()); // operation_failed
	CHECK(!fdb::Error(1004).maybeCommitted()); // timed_out
	CHECK(!fdb::Error(1025).maybeCommitted()); // transaction_cancelled
	CHECK(!fdb::Error(1038).maybeCommitted()); // database_locked
	CHECK(!fdb::Error(1101).maybeCommitted()); // operation_cancelled
	CHECK(!fdb::Error(2002).maybeCommitted()); // commit_read_incomplete

	CHECK(fdb::Error(1007).retryableNotCommitted()); // transaction_too_old
	CHECK(fdb::Error(1020).retryableNotCommitted()); // not_committed
	CHECK(fdb::Error(1038).retryableNotCommitted()); // database_locked

	CHECK(!fdb::Error(1021).retryableNotCommitted()); // commit_unknown_result
	CHECK(!fdb::Error(1025).retryableNotCommitted()); // transaction_cancelled
	CHECK(!fdb::Error(1031).retryableNotCommitted()); // transaction_timed_out
	CHECK(!fdb::Error(1040).retryableNotCommitted()); // proxy_memory_limit_exceeded
}

// Commit a transaction and confirm it has not been reset
TEST_CASE("commit_does_not_reset") {
	auto tr = db.createTransaction();
	auto tr2 = db.createTransaction();

	// Commit two transactions, one that will fail with conflict and the other
	// that will succeed. Ensure both transactions are not reset at the end.
	while (1) {
		auto tr1GrvFuture = tr.getReadVersion();
		auto err = waitFuture(tr1GrvFuture);
		if (err) {
			auto tr1OnErrorFuture = tr.onError(err);
			fdbCheck(waitFuture(tr1OnErrorFuture));
			continue;
		}

		int64_t tr1StartVersion = tr1GrvFuture.get();

		auto tr2GrvFuture = tr2.getReadVersion();
		err = waitFuture(tr2GrvFuture);
		if (err) {
			auto tr2OnErrorFuture = tr2.onError(err);
			fdbCheck(waitFuture(tr2OnErrorFuture));
			continue;
		}

		int64_t tr2StartVersion = tr2GrvFuture.get();

		tr.set(fdb::toBytesRef(key("foo")), "bar"_br);
		auto tr1CommitFuture = tr.commit();
		err = waitFuture(tr1CommitFuture);
		if (err) {
			auto tr1OnErrorFuture = tr.onError(err);
			fdbCheck(waitFuture(tr1OnErrorFuture));
			continue;
		}

		tr2.addConflictRange(
		    fdb::toBytesRef(key("foo")), fdb::toBytesRef(strinc_str(key("foo"))), FDB_CONFLICT_RANGE_TYPE_READ);
		tr2.set(fdb::toBytesRef(key("foo")), "bar"_br);
		auto tr2CommitFuture = tr2.commit();
		err = waitFuture(tr2CommitFuture);
		CHECK(err.code() == 1020); // not_committed

		auto tr1GrvFuture2 = tr.getReadVersion();
		err = waitFuture(tr1GrvFuture2);
		if (err) {
			auto tr1OnErrorFuture = tr.onError(err);
			fdbCheck(waitFuture(tr1OnErrorFuture));
			continue;
		}

		int64_t tr1EndVersion = tr1GrvFuture2.get();

		auto tr2GrvFuture2 = tr2.getReadVersion();
		err = waitFuture(tr2GrvFuture2);
		if (err) {
			auto tr2OnErrorFuture = tr2.onError(err);
			fdbCheck(waitFuture(tr2OnErrorFuture));
			continue;
		}

		int64_t tr2EndVersion = tr2GrvFuture2.get();

		// If we reset the transaction, then the read version will change
		CHECK(tr1StartVersion == tr1EndVersion);
		CHECK(tr2StartVersion == tr2EndVersion);
		break;
	}
}

TEST_CASE("Fast alloc thread cleanup") {
	// Try to cause an OOM if thread cleanup doesn't work
	for (int i = 0; i < 50000; ++i) {
		auto thread = std::thread([]() {
			auto tr = db.createTransaction();
			for (int s = 0; s < 11; ++s) {
				tr.set(fdb::toBytesRef(key("foo")), fdb::toBytesRef(std::string(8 << s, '\x00')));
			}
		});
		thread.join();
	}
}

TEST_CASE("Tenant create, access, and delete") {
	std::string tenantName = "tenant";
	auto testKey = "foo"_br;
	auto testValue = "bar"_br;

	auto tr = db.createTransaction();
	while (1) {
		tr.setOption(FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES);
		tr.set(fdb::toBytesRef("\xff\xff/management/tenant/map/" + tenantName), ""_br);
		auto commitFuture = tr.commit();
		auto err = waitFuture(commitFuture);
		if (err) {
			auto f = tr.onError(err);
			fdbCheck(waitFuture(f));
			continue;
		}
		tr.reset();
		break;
	}

	while (1) {
		std::string begin = "\xff\xff/management/tenant/map/";
		std::string end = "\xff\xff/management/tenant/map0";

		tr.setOption(FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES);
		auto f = tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(begin)),
		                     fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(end)),
		                     /* limit */ 0,
		                     /* target_bytes */ 0,
		                     /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
		                     /* iteration */ 0,
		                     /* snapshot */ false,
		                     /* reverse */ false);

		auto err = waitFuture(f);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto output = f.get();
		auto outKv = std::get<0>(output);
		auto outCount = std::get<1>(output);

		CHECK(outCount == 1);
		CHECK(StringRef(outKv[0].key().begin(), outKv[0].key().size()) == StringRef(tenantName).withPrefix(begin));

		tr.reset();
		break;
	}

	auto tenant = db.openTenant(fdb::toBytesRef(tenantName));
	auto tr2 = tenant.createTransaction();

	while (1) {
		tr2.set(testKey, testValue);
		auto commitFuture = tr2.commit();
		auto err = waitFuture(commitFuture);
		if (err) {
			auto f = tr2.onError(err);
			fdbCheck(waitFuture(f));
			continue;
		}
		tr2.reset();
		break;
	}

	while (1) {
		auto f1 = tr2.get(testKey, false);
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr2.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto val = f1.get();
		CHECK(val.has_value());
		CHECK(testValue == val.value());

		tr2.clear(testKey);
		auto commitFuture = tr2.commit();
		err = waitFuture(commitFuture);
		if (err) {
			auto f = tr2.onError(err);
			fdbCheck(waitFuture(f));
			continue;
		}
		tr2.reset();
		break;
	}

	while (1) {
		tr.setOption(FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES);
		tr.clear(fdb::toBytesRef("\xff\xff/management/tenant/map/" + tenantName));
		auto commitFuture = tr.commit();
		auto err = waitFuture(commitFuture);
		if (err) {
			auto f = tr.onError(err);
			fdbCheck(waitFuture(f));
			continue;
		}
		tr.reset();
		break;
	}

	while (1) {
		auto f1 = tr2.get(testKey, false);
		auto err = waitFuture(f1);
		if (err.code() == error_code_tenant_not_found) {
			tr2.reset();
			break;
		}
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
	}
}

TEST_CASE("Tenant create twice") {
	auto tenantName = "testTenantNameDup"_br;
	createTenant(tenantName);
	// The second create call is a no-op
	createTenant(tenantName);

	deleteTenant(tenantName);
}

TEST_CASE("Database get client status") {
	auto statusFuture = db.getClientStatus();
	checkClientStatusFuture(statusFuture);
}

TEST_CASE("Tenant get client status") {
	auto tenantName = "testTenantForClientStatus"_br;
	createTenant(tenantName);

	auto tenant = db.openTenant(tenantName);
	auto statusFuture = tenant.getClientStatus();
	checkClientStatusFuture(statusFuture);

	deleteTenant(tenantName);
}

TEST_CASE("Non-existing tenant get client status") {
	auto tenantName = "testTenantNonExist"_br;
	auto tenant = db.openTenant(tenantName);
	// Currently, we are able to open a on-existing tenant and call get client status
	// on the handle. This may change in the future.
	auto statusFuture = tenant.getClientStatus();
	checkClientStatusFuture(statusFuture);
}

int64_t granule_start_load_fail(const char* filename,
                                int filenameLength,
                                int64_t offset,
                                int64_t length,
                                int64_t fullFileLength,
                                void* userContext) {
	CHECK(false);
	return -1;
}

uint8_t* granule_get_load_fail(int64_t loadId, void* userContext) {
	CHECK(false);
	return nullptr;
}

void granule_free_load_fail(int64_t loadId, void* userContext) {
	CHECK(false);
}

TEST_CASE("Blob Granule Functions") {
	auto confValue =
	    getValue("\xff/conf/blob_granules_enabled"_br, /* snapshot */ false, { FDB_TR_OPTION_READ_SYSTEM_KEYS });
	if (!confValue.has_value() || confValue.value() != "1") {
		// std::cout << "skipping blob granule test" << std::endl;
		return;
	}

	// write some data
	insert_data(db, create_data({ { "bg1", "a" }, { "bg2", "b" }, { "bg3", "c" } }));

	// because wiring up files is non-trivial, just test the calls complete with the expected no_materialize error
	fdb::native::FDBReadBlobGranuleContext granuleContext;
	granuleContext.userContext = nullptr;
	granuleContext.start_load_f = &granule_start_load_fail;
	granuleContext.get_load_f = &granule_get_load_fail;
	granuleContext.free_load_f = &granule_free_load_fail;
	granuleContext.debugNoMaterialize = true;
	granuleContext.granuleParallelism = 1;

	auto tr = db.createTransaction();
	int64_t originalReadVersion = -1;

	// test no materialize gets error but completes, save read version
	while (1) {
		tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
		// -2 is latest version
		auto r = tr.readBlobGranules(fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), 0, -2, granuleContext);
		fdb::future_var::KeyValueRefArray::Type out;
		auto err = r.getKeyValueArrayNothrow(out);
		if (err && err.code() != 2037 /* blob_granule_not_materialized */) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		CHECK(err.code() == 2037 /* blob_granule_not_materialized */);

		// If read done, save read version. Should have already used read version so this shouldn't error
		auto grvFuture = tr.getReadVersion();
		auto grvErr = waitFuture(grvFuture);
		CHECK(!grvErr);
		CHECK(!grvFuture.getNothrow(originalReadVersion));
		CHECK(originalReadVersion > 0);

		tr.reset();
		break;
	}

	// test with begin version > 0
	while (1) {
		tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
		// -2 is latest version, read version should be >= originalReadVersion
		auto r = tr.readBlobGranules(
		    fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), originalReadVersion, -2, granuleContext);

		fdb::future_var::KeyValueRefArray::Type out;
		auto err = r.getKeyValueArrayNothrow(out);
		if (err && err.code() != 2037 /* blob_granule_not_materialized */) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		CHECK(err.code() == 2037 /* blob_granule_not_materialized */);
		tr.reset();
		break;
	}

	// test with prior read version completes after delay larger than normal MVC window
	// TODO: should we not do this?
	std::this_thread::sleep_for(std::chrono::milliseconds(6000));
	while (1) {
		tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
		auto r = tr.readBlobGranules(
		    fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), 0, originalReadVersion, granuleContext);

		fdb::future_var::KeyValueRefArray::Type out;
		auto err = r.getKeyValueArrayNothrow(out);
		if (err && err.code() != 2037 /* blob_granule_not_materialized */) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		CHECK(err.code() == 2037 /* blob_granule_not_materialized */);
		tr.reset();
		break;
	}

	// test ranges

	while (1) {
		auto f = tr.getBlobGranuleRanges(fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), 1000);
		auto err = waitFuture(f);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto output = f.get();
		auto outKr = std::get<0>(output);
		auto outCount = std::get<1>(output);

		CHECK(fdb::toCharsRef(outKr[0].beginKey()) <= key("bg"));
		CHECK(fdb::toCharsRef(outKr[outCount - 1].endKey()) >= key("bh"));

		CHECK(outCount >= 1);
		// check key ranges are in order
		for (int i = 0; i < outCount; i++) {
			// key range start < end
			CHECK(fdb::toCharsRef(outKr[i].beginKey()) < fdb::toCharsRef(outKr[i].endKey()));
		}
		// Ranges themselves are sorted and contiguous
		for (int i = 0; i < outCount - 1; i++) {
			CHECK(fdb::toCharsRef(outKr[i].endKey()) == fdb::toCharsRef(outKr[i + 1].beginKey()));
		}

		tr.reset();
		break;
	}

	// do a purge + wait at that version to purge everything before originalReadVersion

	auto purgeKeyFuture =
	    db.purgeBlobGranules(fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), originalReadVersion, false);
	fdbCheck(waitFuture(purgeKeyFuture));

	fdb::KeyRef purgeKey = purgeKeyFuture.get();

	auto waitPurgeFuture = db.waitPurgeGranulesComplete(purgeKey);
	fdbCheck(waitFuture(waitPurgeFuture));

	// re-read again at the purge version to make sure it is still valid
	while (1) {
		tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
		auto r = tr.readBlobGranules(
		    fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), 0, originalReadVersion, granuleContext);
		fdb::future_var::KeyValueRefArray::Type out;
		auto err = r.getKeyValueArrayNothrow(out);
		if (err && err.code() != 2037 /* blob_granule_not_materialized */) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		CHECK(err.code() == 2037 /* blob_granule_not_materialized */);
		tr.reset();
		break;
	}

	// check granule summary
	while (1) {
		auto f =
		    tr.summarizeBlobGranules(fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), originalReadVersion, 100);
		auto err = waitFuture(f);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto out = f.get();
		auto out_summaries = std::get<0>(out);
		auto out_count = std::get<1>(out);

		CHECK(out_count >= 1);
		CHECK(out_count <= 100);

		// check that ranges cover requested range
		CHECK(fdb::toCharsRef(out_summaries[0].beginKey()) <= key("bg"));
		CHECK(fdb::toCharsRef(out_summaries[out_count - 1].endKey()) >= key("bh"));

		// check key ranges are in order
		for (int i = 0; i < out_count; i++) {
			// key range start < end
			CHECK(fdb::toCharsRef(out_summaries[i].beginKey()) < fdb::toCharsRef(out_summaries[i].endKey()));
			//// sanity check versions and sizes
			CHECK(out_summaries[i].snapshot_version <= originalReadVersion);
			CHECK(out_summaries[i].delta_version <= originalReadVersion);
			CHECK(out_summaries[i].snapshot_version <= out_summaries[i].delta_version);
			CHECK(out_summaries[i].snapshot_size > 0);
			CHECK(out_summaries[i].delta_size >= 0);
		}

		// Ranges themselves are sorted and contiguous
		for (int i = 0; i < out_count - 1; i++) {
			CHECK(fdb::toCharsRef(out_summaries[i].endKey()) == fdb::toCharsRef(out_summaries[i + 1].beginKey()));
		}

		tr.reset();
		break;
	}
}

int main(int argc, char** argv) {
	if (argc < 3) {
		std::cout << "Unit tests for the FoundationDB C API.\n"
		          << "Usage: fdb_c_unit_tests /path/to/cluster_file key_prefix [externalClient] [doctest args]"
		          << std::endl;
		return 1;
	}
	fdb::selectApiVersion(FDB_API_VERSION);
	if (argc >= 4) {
		std::string externalClientLibrary = argv[3];
		if (externalClientLibrary.substr(0, 2) != "--") {
			fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_DISABLE_LOCAL_CLIENT);
			fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_EXTERNAL_CLIENT_LIBRARY, externalClientLibrary);
		}
	}

	/* fdb_check(fdb_network_set_option( */
	/*     FDBNetworkOption::FDB_NET_OPTION_CLIENT_BUGGIFY_ENABLE, reinterpret_cast<const uint8_t*>(""), 0)); */

	doctest::Context context;
	context.applyCommandLine(argc, argv);

	fdb::network::setup();
	std::thread network_thread{ [] { fdbCheck(fdb::network::run()); } };

	db = fdb::Database(argv[1]);
	clusterFilePath = std::string(argv[1]);
	prefix = argv[2];
	int res = context.run();

	if (context.shouldExit()) {
		fdbCheck(fdb::network::stop());
		network_thread.join();
		return res;
	}
	fdbCheck(fdb::network::stop());
	network_thread.join();

	return res;
}
