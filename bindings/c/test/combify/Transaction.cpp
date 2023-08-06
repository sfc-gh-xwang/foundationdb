#include <optional>
#include "Combify.hpp"
#include "TestHelpers.hpp"

// Prevent ODR clash between sources
namespace {

using namespace combify;
using namespace fdb::literals;

// Helper function to add `prefix` to all keys in the given map. Returns a new map
auto applyKeyPrefix(const std::string& prefix, std::vector<std::pair<std::string, std::string>>&& data) {
	std::vector<std::pair<std::string, std::string>> out;
	for (const auto& d : data) {
		out.emplace_back(prefix + d.first, d.second);
	}
	return out;
}

AddTest testWriteAndRead("BasicWriteAndReadBack", ALL, [](const TestEnv& env) {
	auto tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr,
	    [](auto tr) {
		    tr.set("abc"_br, "def"_br);
		    return tr.commit();
	    },
	    "WriteSomeValue");

	tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr, [](auto tr) { return tr.get("abc"_br, false); }, "ReadBackValue");
	ASSERT(f.get() == "def"_br, "unmatched read back value: expected=def actual={}", fdb::toCharsRef(f.get()));
});

AddTest testGetReadVersion("BasicGetReadVersion", ALL, [](const TestEnv& env) {
	auto tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr, [](auto tr) { return tr.getReadVersion(); }, "GetReadVersion");

	ASSERT(f.get() > 0, "unmatched read version value: expected > 0");
});

AddTest testGetKey("BasicGetKey", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-basic-get-key-");
	auto tr = env.newNormalOrTenantTransaction();

	populateDBData(tr, applyKeyPrefix(prefix, { { "a", "1" }, { "baz", "2" }, { "bar", "3" } }));

	tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    return tr.getKey(fdb::key_select::firstGreaterThan(fdb::toBytesRef(prefix + "a")), /* snapshot */ false);
	    },
	    "GetKey");

	ASSERT(fdb::toCharsRef(f.get()) == prefix + "bar",
	       "unmatched read value: expected={} actual={}",
	       prefix + "bar",
	       fdb::toCharsRef(f.get()));
});

AddTest testGetValue("BasicGetValue", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-basic-get-value-");
	auto tr = env.newNormalOrTenantTransaction();
	populateDBData(tr, applyKeyPrefix(prefix, { { "foo", "bar" } }));

	tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr, [&prefix](auto tr) { return tr.get(fdb::toBytesRef(prefix + "foo"), false); }, "GetValue");

	ASSERT(f.get() == "bar"_br, "unmatched read value: expected=bar actual={}", fdb::toCharsRef(f.get()));
});

AddTest testGetStringArray("GetKeyAddress", asTrustedClient, [](const TestEnv& env) {
	// Access of key address is protected from the tenant use case. Thus only trusted client case is tested.
	const std::string prefix("combify-get-string-array-");
	auto db = env.newDatabase();
	populateDBData(db, applyKeyPrefix(prefix, { { "foo", "bar" } }));
	auto tr = env.newTransaction(db);
	auto f = runToCompletion(
	    tr, [&prefix](auto tr) { return tr.getAddressForKey(fdb::toBytesRef(prefix + "foo")); }, "GetKeyAddress");

	auto const& [strings, count] = f.get();
	ASSERT(count > 0);
	for (int i = 0; i < count; ++i) {
		ASSERT(strlen(strings[i]) > 0, "unexpected empty string");
	}
});

AddTest testReadSystemKey("ReadSystemKey", asTrustedClient, [](const TestEnv& env) {
	// Access of system key is protected from the tenant use case. Thus only trusted client case is tested.
	auto db = env.newDatabase();
	for (auto const enableReadSystemKeys : { false, true }) {
		auto tr = env.newTransaction(db);
		try {
			auto f = runToCompletion(
			    tr,
			    [enableReadSystemKeys](auto tr) {
				    if (enableReadSystemKeys) {
					    tr.setOption(FDB_TR_OPTION_READ_SYSTEM_KEYS);
				    }

				    return tr.get("\xff/coordinators"_br, false);
			    },
			    "ReadSystemKey");

			ASSERT(enableReadSystemKeys);
			// Only when enableReadSystemKeys is set we hit this line
			ASSERT(f.get().has_value(), "Unexpected null value from read");
		} catch (fdb::Error e) {
			ASSERT(!enableReadSystemKeys, "FDB_TR_OPTION_READ_SYSTEM_KEYS must be disabled");
			ASSERT(e.code() == 2004); // key_outside_legal_range
		}
	}
});

AddTest testWriteSystemKey("WriteSystemKey", asTrustedClient, [](const TestEnv& env) {
	// Access of system key is protected from the tenant use case. Thus only trusted client case is tested.
	auto db = env.newDatabase();
	auto syskey = "\xff\x02"_br;

	for (auto const enableWriteSystemKeys : { false, true }) {
		auto tr = env.newTransaction(db);
		try {
			// Write a system key
			runToCompletion(
			    tr,
			    [enableWriteSystemKeys, syskey](auto tr) {
				    if (enableWriteSystemKeys) {
					    tr.setOption(FDB_TR_OPTION_ACCESS_SYSTEM_KEYS);
				    }

				    tr.set(syskey, "bar"_br);
				    return tr.commit();
			    },
			    "WriteSystemKey");

			// Read and verify the system key value
			tr = env.newTransaction(db);
			auto f = runToCompletion(
			    tr,
			    [syskey](auto tr) {
				    tr.setOption(FDB_TR_OPTION_READ_SYSTEM_KEYS);
				    return tr.get(syskey, false);
			    },
			    "ReadBackSystemKey");

			ASSERT(enableWriteSystemKeys);
			ASSERT(f.get() == "bar"_br, "unexpected read value: expected=bar actual={}", fdb::toCharsRef(f.get()));
		} catch (fdb::Error e) {
			ASSERT(!enableWriteSystemKeys, "FDB_TR_OPTION_ACCESS_SYSTEM_KEYS must be disabled");
			ASSERT(e.code() == 2004); // key_outside_legal_range
		}
	}
});

AddTest testReadYourWrites("ReadYourWrites", ALL, [](const TestEnv& env) {
	for (auto const option : std::vector<std::optional<FDBTransactionOption>>{
	         std::nullopt, FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE }) {
		auto tr = env.newNormalOrTenantTransaction();

		// Read before committing. Herer we test 3 different cases:
		// 1) By default, read the snapshot = false after a write before commit should read the initial write
		// 2) If FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE is set, read with snapshot = false should return null
		// 3) If FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE is set, read with snapshot = true should succeed
		auto snapshot = false;
		if (option == FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE) {
			snapshot = true;
		}

		auto f1 = runToCompletion(
		    tr,
		    [option, snapshot](auto tr) {
			    if (option.has_value()) {
				    tr.setOption(option.value());
			    }
			    tr.set("foo"_br, "bar"_br);
			    return tr.get("foo"_br, snapshot);
		    },
		    "ReadValue");

		if (option == FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE) {
			ASSERT(!f1.get().has_value(), "Expect null value");
			continue;
		}

		ASSERT(f1.get() == "bar"_br, "unmatched read value: expected=bar, actual={}", fdb::toCharsRef(f1.get()));
	}
});

AddTest testSnapshotReadYourWritesDisable("SnapshotReadYourWritesDisable", ALL, [](const TestEnv& env) {
	auto tr = env.newNormalOrTenantTransaction();

	auto f2 = runToCompletion(
	    tr,
	    [](auto tr) {
		    tr.setOption(FDB_TR_OPTION_SNAPSHOT_RYW_DISABLE);
		    tr.set("foo"_br, "bar"_br);
		    auto f1 = tr.get("foo"_br, /*snapshot*/ true);

		    // This allows the runToCompletion to properly retry on transient errors that happen to f1
		    if (f1.blockUntilReady() || f1.error()) {
			    return f1;
		    }

		    ASSERT(!f1.get().has_value(), "Expect null value");
		    auto f2 = tr.get("foo"_br, /*snapshot*/ false);
		    return f2;
	    },
	    "ReadSnapshotYourWrites");

	ASSERT(f2.get() == "bar"_br, "unmatched read value: expected=bar, actual={}", fdb::toCharsRef(f2.get()));
});

AddTest testGetKeyValueArray("GetRange", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-get-key-value-array-");
	auto data = applyKeyPrefix(prefix, { { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
	auto tr = env.newNormalOrTenantTransaction();
	populateDBData(tr, data);

	// The following use cases are tested for getRange
	// 1) FDB_STREAMING_MODE_WANT_ALL + no limit.
	// 2) FDB_STREAMING_MODE_WANT_ALL + no limit + reverse order
	// 3) FDB_STREAMING_MODE_WANT_ALL + limit = 2
	// 4) FDB_STREAMING_MODE_EXACT + limit = 3
	for (auto const& testConfigs : { std::make_tuple(false, FDB_STREAMING_MODE_WANT_ALL, 0),
	                                 std::make_tuple(true, FDB_STREAMING_MODE_WANT_ALL, 0),
	                                 std::make_tuple(false, FDB_STREAMING_MODE_WANT_ALL, 2),
	                                 std::make_tuple(false, FDB_STREAMING_MODE_EXACT, 3) }) {
		tr = env.newNormalOrTenantTransaction();
		auto f = runToCompletion(
		    tr,
		    [&prefix, &testConfigs](auto tr) {
			    auto const [reverse, mode, limit] = testConfigs;
			    return tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(prefix + "a")),
			                       fdb::key_select::lastLessOrEqual(fdb::toBytesRef(prefix + "d"), 1),
			                       /* limit */ limit,
			                       /* target_bytes */ 0,
			                       /* FDBStreamingMode */ mode,
			                       /* iteration */ 0,
			                       /* snapshot */ false,
			                       /* reverse */ reverse);
		    },
		    "GetRange");

		auto const [reverse, mode, limit] = testConfigs;
		auto [kvs, count, more] = f.get();
		ASSERT(count > 0, "count is expected to be larger than zero");

		auto expectedUpperBound = (int)data.size();
		if (limit > 0) {
			expectedUpperBound = std::min(limit, expectedUpperBound);
		}

		if (mode == FDB_STREAMING_MODE_EXACT) {
			ASSERT(count == limit, "unmatched count: expected={}, actual={}", limit, count);
		} else {
			ASSERT(count <= expectedUpperBound, "unmatched count: expected<={}, actual={}", expectedUpperBound, count);
			if (count < expectedUpperBound) {
				ASSERT(more, "more is expected to be true if count < {}", expectedUpperBound);
			}
		}

		auto dataIdx = reverse ? data.size() - 1 : 0;
		for (int i = 0; i < count; i++) {
			auto kv = *kvs++;
			auto key = std::string(kv.key().begin(), kv.key().end());
			auto value = std::string(kv.value().begin(), kv.value().end());
			auto& d = data[dataIdx];
			ASSERT(d.first == key, "unmatched key, expected={}, actual={}", d.first, key);
			ASSERT(d.second == value, "unmatched value, expected={}, actual={}", d.second, value);
			if (reverse) {
				dataIdx--;
			} else {
				dataIdx++;
			}
		}
	}
});

AddTest testClearKey("ClearKey", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-clear-key-");
	auto tr = env.newNormalOrTenantTransaction();

	populateDBData(tr, applyKeyPrefix(prefix, { { "foo", "bar" } }));

	tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    tr.clear(fdb::toBytesRef(prefix + "foo"));
		    return tr.commit();
	    },
	    "ClearKey");

	tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr,
	    [&prefix](auto tr) { return tr.get(fdb::toBytesRef(prefix + "foo"), /* snapshot */ false); },
	    "CheckEmptyKey");
	ASSERT(!f.get().has_value(), "Expect null value");
});

AddTest testCancel("Cancel", asTrustedClient, [](const TestEnv& env) {
	// Cannot use transaction after cancelling
	auto tr = env.newNormalOrTenantTransaction();

	try {
		auto f = runToCompletion(
		    tr,
		    [](auto tr) {
			    tr.cancel();
			    return tr.get("foo"_br, /* snapshot */ false);
		    },
		    "ReadKeyFail");
		ASSERT(false, "Read-key operation should fail after transaction is canceled");
	} catch (fdb::Error e) {
		ASSERT(e.code() == 1025, "unmatched error code: expected=1025, actual={}", e.code()); // transaction_cancelled
	}

	// After the transaction is reset, the transaction can get through
	runToCompletion(
	    tr,
	    [](auto tr) {
		    tr.reset();
		    return tr.get("foo"_br, /* snapshot */ false);
	    },
	    "ReadKeyWithoutException");
});

AddTest testGetCommittedVersionReadOnly("GetCommittedVersionReadOnly", ALL, [](const TestEnv& env) {
	auto tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr, [](auto tr) { return tr.get("foo"_br, /*snapshot*/ false); }, "ReadOnlyTransaction");

	ASSERT(tr.getCommittedVersion() == -1,
	       "Unmatched committed version: expected=-1, actual={}",
	       tr.getCommittedVersion());
});

AddTest testGetCommittedVersion("GetCommittedVersion", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-get-committed-version-");
	auto tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    tr.set(fdb::toBytesRef(prefix + "foo"), "bar"_br);
		    return tr.commit();
	    },
	    "CommitValue");

	ASSERT(
	    tr.getCommittedVersion() >= 0, "Unmatched committed version: expected>=0, actual={}", tr.getCommittedVersion());
});

AddTest testGetTagThrottledDuration("GetTagThrottledDuration", ALL, [](const TestEnv& env) {
	auto tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr, [](auto tr) { return tr.get("foo"_br, /*snapshot*/ false); }, "GetKey");

	auto f = runToCompletion(
	    tr, [](auto tr) { return tr.getTagThrottledDuration(); }, "GetTagThrottledDuration");

	ASSERT(f.get() >= 0.0, "Unmatched tag throttled duration: expected>=0.0, actual={}", f.get());
});

AddTest testGetTotalCost("GetTotalCost", ALL, [](const TestEnv& env) {
	auto tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr, [](auto tr) { return tr.get("foo"_br, /*snapshot*/ false); }, "GetKey");

	auto f = runToCompletion(
	    tr, [](auto tr) { return tr.getTotalCost(); }, "GetTotalCost");

	ASSERT(f.get() > 0.0, "Unmatched tag throttled duration: expected>=0.0, actual={}", f.get());
});

AddTest testSetExpectedCost("SetExpectedCost", ALL, [](const TestEnv& env) {
	auto tr = env.newNormalOrTenantTransaction();
	tr.setExpectedCost(4096);

	runToCompletion(
	    tr, [](auto tr) { return tr.getReadVersion(); }, "GetReadVersion");
});

AddTest testGetApproxSize("GetApproxSize", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-get-approx-size-");
	auto tr = env.newNormalOrTenantTransaction();

	auto f = runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    tr.set(fdb::toBytesRef(prefix + "foo"), "bar"_br);
		    return tr.getApproximateSize();
	    },
	    "GetApproxSize");

	ASSERT(f.get() >= 3, "Unmatched transaction approx size: expected>=3, actual={}", f.get());
});

AddTest testAtomicOpBitAnd("AtomicOpBitAnd", ALL, [](const TestEnv& env) {
	// Test bitwise and on values of same length:
	//   db key = foo
	//   db value = 'a' == 97
	//   param = 'b' == 98
	//
	//   'a' == 97 == 0b01100001
	// & 'b' == 98 == 0b01100010
	//   -----------------------
	//                0b01100000 == 96 == '`'
	//
	// Test bitwise and on extended database value:
	//   db key = bar
	//   db value = 'c' == 99
	//   param = "ad"
	//
	//   'c' == 99 == 0b0110001100000000 (zero extended on right to match length of param)
	// & "ad"   ==    0b0110000101100100
	//   -------------------------------
	//                0b0110000100000000 == 'a' followed by null (0)
	//
	// Test bitwise and on truncated database value:
	//   db key = baz
	//   db value = "abc"
	//   param = 'e' == 101
	//
	//   "abc"  ->  0b01100001 (truncated to "a" to match length of param)
	// & 'e' == 101 0b01100101
	//   ---------------------
	//              0b01100001 == 97 == 'a'
	//
	const std::string prefix("combify-atomic-op-bit-and");
	auto tr = env.newNormalOrTenantTransaction();
	populateDBData(tr, applyKeyPrefix(prefix, { { "foo", "a" }, { "bar", "c" }, { "baz", "abc" } }));
	auto fooKey = prefix + "foo";
	auto barKey = prefix + "bar";
	auto bazKey = prefix + "baz";

	tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr,
	    [&fooKey, &barKey, &bazKey](auto tr) {
		    char param[] = { 'a', 'd' };

		    tr.atomicOp(fdb::toBytesRef(fooKey), "b"_br, FDB_MUTATION_TYPE_BIT_AND);
		    tr.atomicOp(fdb::toBytesRef(barKey),
		                fdb::BytesRef(reinterpret_cast<const uint8_t*>(param), 2),
		                FDB_MUTATION_TYPE_BIT_AND);
		    tr.atomicOp(fdb::toBytesRef(bazKey), "e"_br, FDB_MUTATION_TYPE_BIT_AND);

		    return tr.commit();
	    },
	    "FDBMutationBitAnd");

	// Check results
	for (auto const& keyVal :
	     { std::make_tuple("foo", "`"_br), std::make_tuple("bar", "a\0"_br), std::make_tuple("baz", "a"_br) }) {
		std::string key;
		fdb::BytesRef val;
		std::tie(key, val) = keyVal;

		tr = env.newNormalOrTenantTransaction();
		auto f = runToCompletion(
		    tr, [&prefix, &key](auto tr) { return tr.get(fdb::toBytesRef(prefix + key), false); }, "GetVal");
		ASSERT(f.get() == val,
		       "unmatched val for {}: expected={}, actual={}",
		       key,
		       fdb::toCharsRef(val),
		       fdb::toCharsRef(f.get()));
	}
});

AddTest testAtomicOpBitOr("AtomicOpBitOr", ALL, [](const TestEnv& env) {
	// Test bitwise or on values of same length:
	//   db key = foo
	//   db value = 'a' == 97
	//   param = 'b' == 98
	//
	//   'a' == 97 == 0b01100001
	// | 'b' == 98 == 0b01100010
	//   -----------------------
	//                0b01100011 == 99 == 'c'
	//
	// Test bitwise or on extended database value:
	//   db key = bar
	//   db value = 'b' == 98
	//   param = "ad"
	//
	//   'b' == 98 -> 0b0110001000000000 (zero extended on right to match length of param)
	// | "ad"   ==    0b0110000101100100
	//   -------------------------------
	//                0b0110001101100100 == "cd"
	//
	// Test bitwise or on truncated database value:
	//   db key = baz
	//   db value = "abc"
	//   param = 'd' == 100
	//
	//   "abc"  ->  0b01100001 (truncated to "a" to match length of param)
	// | 'd' == 100 0b01100100
	//   ---------------------
	//              0b01100101 == 101 == 'e'
	//
	const std::string prefix("combify-atomic-op-bit-or");
	auto tr = env.newNormalOrTenantTransaction();
	populateDBData(tr, applyKeyPrefix(prefix, { { "foo", "a" }, { "bar", "b" }, { "baz", "abc" } }));
	auto fooKey = prefix + "foo";
	auto barKey = prefix + "bar";
	auto bazKey = prefix + "baz";

	tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr,
	    [&fooKey, &barKey, &bazKey](auto tr) {
		    char param[] = { 'a', 'd' };

		    tr.atomicOp(fdb::toBytesRef(fooKey), "b"_br, FDB_MUTATION_TYPE_BIT_OR);
		    tr.atomicOp(fdb::toBytesRef(barKey),
		                fdb::BytesRef(reinterpret_cast<const uint8_t*>(param), 2),
		                FDB_MUTATION_TYPE_BIT_OR);
		    tr.atomicOp(fdb::toBytesRef(bazKey), "d"_br, FDB_MUTATION_TYPE_BIT_OR);

		    return tr.commit();
	    },
	    "FDBMutationBitAnd");

	// Check results
	for (auto const& keyVal :
	     { std::make_tuple("foo", "c"_br), std::make_tuple("bar", "cd"_br), std::make_tuple("baz", "e"_br) }) {
		std::string key;
		fdb::BytesRef val;
		std::tie(key, val) = keyVal;

		tr = env.newNormalOrTenantTransaction();
		auto f = runToCompletion(
		    tr, [&prefix, &key](auto tr) { return tr.get(fdb::toBytesRef(prefix + key), false); }, "GetVal");
		ASSERT(f.get() == val,
		       "unmatched val for {}: expected={}, actual={}",
		       key,
		       fdb::toCharsRef(val),
		       fdb::toCharsRef(f.get()));
	}
});

AddTest testAtomicOpCompareAndClear("AtomicOpCompareAndClear", ALL, [](const TestEnv& env) {
	// Atomically remove a key-value pair from the database based on a value
	// comparison.
	const std::string prefix("combify-atomic-op-compare-and-clear-");
	auto tr = env.newNormalOrTenantTransaction();
	populateDBData(tr, applyKeyPrefix(prefix, { { "foo", "bar" }, { "fdb", "foundation" } }));

	tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    tr.atomicOp(fdb::toBytesRef(prefix + "foo"), "bar"_br, FDB_MUTATION_TYPE_COMPARE_AND_CLEAR);
		    return tr.commit();
	    },
	    "CompareAndClear");

	tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr, [&prefix](auto tr) { return tr.get(fdb::toBytesRef(prefix + "foo"), false); }, "GetFoo");
	ASSERT(!f.get().has_value(), "Expect null value");

	tr = env.newNormalOrTenantTransaction();
	f = runToCompletion(
	    tr, [&prefix](auto tr) { return tr.get(fdb::toBytesRef(prefix + "fdb"), false); }, "GetFdb");
	ASSERT(f.get() == "foundation"_br,
	       "unmatched value for fdb key: expected=foundation, actual={}",
	       fdb::toCharsRef(f.get()));
});

AddTest testAtomicOpMutationTypeMax("AtomicOpMutationTypeMax", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-atomic-op-mutation-type-max-");
	auto tr = env.newNormalOrTenantTransaction();
	populateDBData(tr, applyKeyPrefix(prefix, { { "foo", "a" }, { "bar", "b" }, { "baz", "cba" } }));

	tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    tr.atomicOp(fdb::toBytesRef(prefix + "foo"), "b"_br, FDB_MUTATION_TYPE_MAX);
		    // Value in database will be extended with zeros to match length of param.
		    tr.atomicOp(fdb::toBytesRef(prefix + "bar"), "aa"_br, FDB_MUTATION_TYPE_MAX);
		    // Value in database will be truncated to match length of param.
		    tr.atomicOp(fdb::toBytesRef(prefix + "baz"), "b"_br, FDB_MUTATION_TYPE_MAX);
		    return tr.commit();
	    },
	    "MutationTypeMax");

	// Check results
	for (auto const& keyVal :
	     { std::make_tuple("foo", "b"_br), std::make_tuple("bar", "aa"_br), std::make_tuple("baz", "c"_br) }) {
		std::string key;
		fdb::BytesRef val;
		std::tie(key, val) = keyVal;

		tr = env.newNormalOrTenantTransaction();
		auto f = runToCompletion(
		    tr, [&prefix, &key](auto tr) { return tr.get(fdb::toBytesRef(prefix + key), false); }, "GetVal");
		ASSERT(f.get() == val,
		       "unmatched val for {}: expected={}, actual={}",
		       key,
		       fdb::toCharsRef(val),
		       fdb::toCharsRef(f.get()));
	}
});

} // anonymous namespace
