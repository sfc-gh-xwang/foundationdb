#include <optional>
#include <semaphore>
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

fdb::Error waitFuture(fdb::Future& f) {
	if (auto err = f.blockUntilReady()) {
		ASSERT(false, "Unexpected future unready: {}", err.what());
	}
	return f.error();
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

	insertAll(tr, applyKeyPrefix(prefix, { { "a", "1" }, { "baz", "2" }, { "bar", "3" } }));

	tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    return tr.getKey(fdb::key_select::firstGreaterThan(fdb::toBytesRef(prefix + "a")),
		                     /* snapshot */ false);
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
	insertAll(tr, applyKeyPrefix(prefix, { { "foo", "bar" } }));

	tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr, [&prefix](auto tr) { return tr.get(fdb::toBytesRef(prefix + "foo"), false); }, "GetValue");

	ASSERT(f.get() == "bar"_br, "unmatched read value: expected=bar actual={}", fdb::toCharsRef(f.get()));
});

AddTest testGetStringArray("GetKeyAddress", asTrustedClient, [](const TestEnv& env) {
	// Access of key address is protected from the tenant use case. Thus only trusted client case is tested.
	const std::string prefix("combify-get-string-array-");
	auto db = env.newDatabase();
	insertAll(db, applyKeyPrefix(prefix, { { "foo", "bar" } }));
	auto tr = env.newTransaction(db);
	auto f = runToCompletion(
	    tr, [&prefix](auto tr) { return tr.getAddressForKey(fdb::toBytesRef(prefix + "foo")); }, "GetKeyAddress");

	auto const& [strings, count] = f.get();
	ASSERT(count > 0);
	for (int i = 0; i < count; ++i) {
		ASSERT(strlen(strings[i]) > 0, "unexpected empty string");
	}
});

AddTest testReadSystemKeyFail("SystemKeyReadFailsWithoutTransactionOption", asTrustedClient, [](const TestEnv& env) {
	// Access of system key is protected from the tenant use case. Thus only trusted client case is tested.
	auto db = env.newDatabase();
	auto tr = env.newTransaction(db);
	try {
		auto f = runToCompletion(
		    tr, [](auto tr) { return tr.get("\xff/coordinators"_br, false); }, "ReadSystemKeyFail");
		ASSERT(false, "Do not expect transaction to succeed");
	} catch (fdb::Error e) {
		ASSERT(e.code() == 2004); // key_outside_legal_range
	}
});

AddTest testReadSystemKey("SystemKeyReadRequiresTransactionOption", asTrustedClient, [](const TestEnv& env) {
	// Access of system key is protected from the tenant use case. Thus only trusted client case is tested.
	auto db = env.newDatabase();
	auto tr = env.newTransaction(db);
	auto f = runToCompletion(
	    tr,
	    [](auto tr) {
		    tr.setOption(FDB_TR_OPTION_READ_SYSTEM_KEYS);
		    return tr.get("\xff/coordinators"_br, false);
	    },
	    "ReadSystemKey");

	// Only when enableReadSystemKeys is set we hit this line
	ASSERT(f.get().has_value(), "Unexpected null value from read");
});

AddTest testWriteSystemKeyFail("WriteSystemKeyFailWithoutTransactionOption", asTrustedClient, [](const TestEnv& env) {
	// Access of system key is protected from the tenant use case. Thus only trusted client case is tested.
	auto db = env.newDatabase();
	auto syskey = "\xff\x02"_br;

	auto tr = env.newTransaction(db);
	try {
		// Write a system key
		runToCompletion(
		    tr,
		    [syskey](auto tr) {
			    tr.set(syskey, "bar"_br);
			    return tr.commit();
		    },
		    "WriteSystemKeyFail");

		ASSERT(false, "Do not expect the write transaction to succeed");
	} catch (fdb::Error e) {
		ASSERT(e.code() == 2004); // key_outside_legal_range
	}
});

AddTest testWriteSystemKey("WriteSystemKeyRequiresTransactionOption", asTrustedClient, [](const TestEnv& env) {
	// Access of system key is protected from the tenant use case. Thus only trusted client case is tested.
	auto db = env.newDatabase();
	auto syskey = "\xff\x02"_br;

	auto tr = env.newTransaction(db);
	// Write a system key
	runToCompletion(
	    tr,
	    [syskey](auto tr) {
		    tr.setOption(FDB_TR_OPTION_ACCESS_SYSTEM_KEYS);
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

	ASSERT(f.get().value() == "bar"_br, "unexpected read value: expected=bar actual={}", fdb::toCharsRef(f.get()));
});

AddTest testReadYourWrites("ReadYourWrites", ALL, [](const TestEnv& env) {
	// By default, read the snapshot = false after a write before commit should read the
	// initial write
	auto tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr,
	    [](auto tr) {
		    tr.set("foo"_br, "bar"_br);
		    return tr.get("foo"_br, false);
	    },
	    "ReadYourWrites");

	ASSERT(f.get().value() == "bar"_br, "unmatched read value: expected=bar, actual={}", fdb::toCharsRef(f.get()));
});

AddTest testReadYourWritesDisable("ReadYourWritesDisable",
                                  OR(asTrustedClient,
                                     AND(asUntrustedClient, withAuthZ, withTls, withTenantsMoreThan(0))),
                                  [](const TestEnv& env) {
	                                  // If FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE is set, read with snapshot = false
	                                  // should return null
	                                  auto tr = env.newNormalOrTenantTransaction();
	                                  auto f = runToCompletion(
	                                      tr,
	                                      [](auto tr) {
		                                      tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
		                                      tr.set("foo"_br, "bar"_br);
		                                      return tr.get("foo"_br, false);
	                                      },
	                                      "ReadYourWrites");

	                                  ASSERT(!f.get().has_value(), "Expect null value");
                                  });

AddTest testReadYourWritesSnapshotRYW("ReadYourWritesSnapshotRYWEnabled", ALL, [](const TestEnv& env) {
	// If FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE is set, read with snapshot = true
	// should succeed
	auto tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr,
	    [](auto tr) {
		    tr.setOption(FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE);
		    tr.set("foo"_br, "bar"_br);
		    return tr.get("foo"_br, /* snapshot */ true);
	    },
	    "ReadYourWrites");

	ASSERT(f.get().value() == "bar"_br, "unmatched read value: expected=bar, actual={}", fdb::toCharsRef(f.get()));
});

AddTest testSnapshotReadYourWritesDisable("SnapshotReadYourWritesDisable",
                                          OR(asTrustedClient,
                                             AND(asUntrustedClient, withAuthZ, withTls, withTenantsMoreThan(0))),
                                          [](const TestEnv& env) {
	                                          auto tr = env.newNormalOrTenantTransaction();

	                                          while (1) {
		                                          // Disable read your writes for snapshot reads.
		                                          tr.setOption(FDB_TR_OPTION_SNAPSHOT_RYW_DISABLE);
		                                          tr.set("foo"_br, "bar"_br);
		                                          auto f1 = tr.get("foo"_br, /*snapshot*/ true);
		                                          auto f2 = tr.get("foo"_br, /*snapshot*/ false);

		                                          auto err = waitFuture(f1);
		                                          if (err) {
			                                          auto f3 = tr.onError(err);
			                                          if (auto err3 = waitFuture(f3)) {
				                                          ASSERT(false, "Unexpected failure: {}", err3.what());
			                                          }
			                                          continue;
		                                          }

		                                          ASSERT(!f1.get().has_value(), "Expect null value");
		                                          //// Non-snapshot reads should still read writes in the transaction.
		                                          err = waitFuture(f2);
		                                          if (err) {
			                                          auto f3 = tr.onError(err);
			                                          if (auto err3 = waitFuture(f3)) {
				                                          ASSERT(false, "Unexpected failure: {}", err3.what());
			                                          }
			                                          continue;
		                                          }

		                                          ASSERT(f2.get().value() == "bar"_br,
		                                                 "unmatched read value: expected=bar, actual={}",
		                                                 fdb::toCharsRef(f2.get()));
		                                          break;
	                                          }
                                          });

AddTest testGetRangeStreamingModeWantAllNoLimit("GetRangeStreamingModeWantAllNoLimit", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-get-range-streaming-mode-want-all-no-limit-");
	auto data = applyKeyPrefix(prefix, { { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
	auto tr = env.newNormalOrTenantTransaction();
	insertAll(tr, data);

	// FDB_STREAMING_MODE_WANT_ALL + no limit.
	tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    return tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(prefix + "a")),
		                       fdb::key_select::lastLessOrEqual(fdb::toBytesRef(prefix + "d"), 1),
		                       /* limit */ 0,
		                       /* target_bytes */ 0,
		                       /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
		                       /* iteration */ 0,
		                       /* snapshot */ false,
		                       /* reverse */ false);
	    },
	    "GetRange");

	auto [kvs, count, more] = f.get();
	ASSERT(count > 0, "count is expected to be larger than zero");

	auto expectedUpperBound = (int)data.size();
	ASSERT(count <= expectedUpperBound, "unmatched count: expected<={}, actual={}", expectedUpperBound, count);
	if (count < expectedUpperBound) {
		ASSERT(more, "more is expected to be true if count < {}", expectedUpperBound);
	}

	auto dataIdx = 0;
	for (int i = 0; i < count; i++) {
		auto kv = *kvs++;
		auto key = std::string(kv.key().begin(), kv.key().end());
		auto value = std::string(kv.value().begin(), kv.value().end());
		auto& d = data[dataIdx++];
		ASSERT(d.first == key, "unmatched key, expected={}, actual={}", d.first, key);
		ASSERT(d.second == value, "unmatched value, expected={}, actual={}", d.second, value);
	}
});

AddTest testGetRangeStreamingModeWantAllNoLimitReverse(
    "GetRangeStreamingModeWantAllNoLimitReverse",
    ALL,
    [](const TestEnv& env) {
	    const std::string prefix("combify-get-range-streaming-mode-want-all-no-limit-reverse-");
	    auto data = applyKeyPrefix(prefix, { { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
	    auto tr = env.newNormalOrTenantTransaction();
	    insertAll(tr, data);

	    // FDB_STREAMING_MODE_WANT_ALL + no limit + reverse order
	    tr = env.newNormalOrTenantTransaction();
	    auto f = runToCompletion(
	        tr,
	        [&prefix](auto tr) {
		        return tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(prefix + "a")),
		                           fdb::key_select::lastLessOrEqual(fdb::toBytesRef(prefix + "d"), 1),
		                           /* limit */ 0,
		                           /* target_bytes */ 0,
		                           /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
		                           /* iteration */ 0,
		                           /* snapshot */ false,
		                           /* reverse */ true);
	        },
	        "GetRange");

	    auto [kvs, count, more] = f.get();
	    ASSERT(count > 0, "count is expected to be larger than zero");

	    auto expectedUpperBound = (int)data.size();
	    ASSERT(count <= expectedUpperBound, "unmatched count: expected<={}, actual={}", expectedUpperBound, count);
	    if (count < expectedUpperBound) {
		    ASSERT(more, "more is expected to be true if count < {}", expectedUpperBound);
	    }

	    auto dataIdx = data.size() - 1;
	    for (int i = 0; i < count; i++) {
		    auto kv = *kvs++;
		    auto key = std::string(kv.key().begin(), kv.key().end());
		    auto value = std::string(kv.value().begin(), kv.value().end());
		    auto& d = data[dataIdx--];
		    ASSERT(d.first == key, "unmatched key, expected={}, actual={}", d.first, key);
		    ASSERT(d.second == value, "unmatched value, expected={}, actual={}", d.second, value);
	    }
    });

AddTest testGetRangeStreamingModeWantAllWithLimit("GetRangeStreamingModeWantAllWithLimit", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-get-range-streaming-mode-want-all-with-limit-");
	auto data = applyKeyPrefix(prefix, { { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
	auto tr = env.newNormalOrTenantTransaction();
	insertAll(tr, data);

	// FDB_STREAMING_MODE_WANT_ALL + limit = 2
	tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    return tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(prefix + "a")),
		                       fdb::key_select::lastLessOrEqual(fdb::toBytesRef(prefix + "d"), 1),
		                       /* limit */ 2,
		                       /* target_bytes */ 0,
		                       /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
		                       /* iteration */ 0,
		                       /* snapshot */ false,
		                       /* reverse */ false);
	    },
	    "GetRange");

	auto [kvs, count, more] = f.get();
	auto expectedUpperBound = (int)data.size();

	ASSERT(count > 0, "count is expected to be larger than zero");
	ASSERT(count <= expectedUpperBound, "unmatched count: expected<={}, actual={}", expectedUpperBound, count);
	if (count < expectedUpperBound) {
		ASSERT(more, "more is expected to be true if count < {}", expectedUpperBound);
	}

	auto dataIdx = 0;
	for (int i = 0; i < count; i++) {
		auto kv = *kvs++;
		auto key = std::string(kv.key().begin(), kv.key().end());
		auto value = std::string(kv.value().begin(), kv.value().end());
		auto& d = data[dataIdx++];
		ASSERT(d.first == key, "unmatched key, expected={}, actual={}", d.first, key);
		ASSERT(d.second == value, "unmatched value, expected={}, actual={}", d.second, value);
	}
});

AddTest testGetRangeStreamingModeExactWithLimit("GetRangeStreamingModeExactWithLimit", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-get-range-streaming-mode-exact-with-limit-");
	auto data = applyKeyPrefix(prefix, { { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
	auto tr = env.newNormalOrTenantTransaction();
	insertAll(tr, data);

	// FDB_STREAMING_MODE_EXACT + limit = 3
	tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    return tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(prefix + "a")),
		                       fdb::key_select::lastLessOrEqual(fdb::toBytesRef(prefix + "d"), 1),
		                       /* limit */ 3,
		                       /* target_bytes */ 0,
		                       /* FDBStreamingMode */ FDB_STREAMING_MODE_EXACT,
		                       /* iteration */ 0,
		                       /* snapshot */ false,
		                       /* reverse */ false);
	    },
	    "GetRange");

	auto [kvs, count, more] = f.get();
	ASSERT(count == 3, "unmatched count: expected=3, actual={}", count);

	auto dataIdx = 0;
	for (int i = 0; i < count; i++) {
		auto kv = *kvs++;
		auto key = std::string(kv.key().begin(), kv.key().end());
		auto value = std::string(kv.value().begin(), kv.value().end());
		auto& d = data[dataIdx++];
		ASSERT(d.first == key, "unmatched key, expected={}, actual={}", d.first, key);
		ASSERT(d.second == value, "unmatched value, expected={}, actual={}", d.second, value);
	}
});

AddTest testClearKey("ClearKey", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-clear-key-");
	auto tr = env.newNormalOrTenantTransaction();

	insertAll(tr, applyKeyPrefix(prefix, { { "foo", "bar" } }));

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

/* Transaction Atomic Operations */
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
	insertAll(tr, applyKeyPrefix(prefix, { { "foo", "a" }, { "bar", "c" }, { "baz", "abc" } }));
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
	insertAll(tr, applyKeyPrefix(prefix, { { "foo", "a" }, { "bar", "b" }, { "baz", "abc" } }));
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
	    "FDBMutationBitOr");

	// Check results
	for (auto const& keyVal :
	     { std::make_tuple("foo", "c"_br), std::make_tuple("bar", "cd"_br), std::make_tuple("baz", "e"_br) }) {
		std::string key;
		fdb::BytesRef val;
		std::tie(key, val) = keyVal;

		tr = env.newNormalOrTenantTransaction();
		auto f = runToCompletion(
		    tr, [&prefix, &key](auto tr) { return tr.get(fdb::toBytesRef(prefix + key), false); }, "GetVal");
		ASSERT(f.get().value() == val,
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
	insertAll(tr, applyKeyPrefix(prefix, { { "foo", "bar" }, { "fdb", "foundation" } }));

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
	insertAll(tr, applyKeyPrefix(prefix, { { "foo", "a" }, { "bar", "b" }, { "baz", "cba" } }));

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

AddTest testAtomicOpMutationTypeMin("AtomicOpMutationTypeMin", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-atomic-op-mutation-type-min-");
	auto tr = env.newNormalOrTenantTransaction();
	insertAll(tr, applyKeyPrefix(prefix, { { "foo", "a" }, { "bar", "b" }, { "baz", "cba" } }));

	tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    tr.atomicOp(fdb::toBytesRef(prefix + "foo"), "b"_br, FDB_MUTATION_TYPE_MIN);
		    // Value in database will be extended with zeros to match length of param.
		    tr.atomicOp(fdb::toBytesRef(prefix + "bar"), "aa"_br, FDB_MUTATION_TYPE_MIN);
		    // Value in database will be truncated to match length of param.
		    tr.atomicOp(fdb::toBytesRef(prefix + "baz"), "b"_br, FDB_MUTATION_TYPE_MIN);
		    return tr.commit();
	    },
	    "MutationTypeMin");

	// Check results
	for (auto const& keyVal :
	     { std::make_tuple("foo", "a"_br), std::make_tuple("bar", "b\0"_br), std::make_tuple("baz", "b"_br) }) {
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

AddTest testAtomicOptionMutationTypeSetVersionstampedKey(
    "AtomicOptionMutationTypeSetVersionstampedKey",
    ALL,
    [](const TestEnv& env) {
	    const std::string prefix("combify-atomic-op-mutation-type-set-versionstamped-key-");
	    int offset = prefix.size() + 3;
	    const char* p = reinterpret_cast<const char*>(&offset);
	    char keybuf[] = { 'f',  'o',  'o',  '\0', '\0', '\0', '\0', '\0', '\0',
		                  '\0', '\0', '\0', '\0', p[0], p[1], p[2], p[3] };
	    std::string key = prefix + std::string(keybuf, 17);
	    std::string versionstamp("");

	    auto tr = env.newNormalOrTenantTransaction();
	    while (1) {
		    tr.atomicOp(fdb::toBytesRef(key), "bar"_br, FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);
		    auto f1 = tr.getVersionstamp();
		    auto f2 = tr.commit();

		    auto err = waitFuture(f2);
		    if (err) {
			    auto f3 = tr.onError(err);
			    if (auto err3 = waitFuture(f3)) {
				    ASSERT(false, "Unexpected failure: {}", err3.what());
			    }
			    continue;
		    }

		    if (auto err = waitFuture(f1)) {
			    ASSERT(false, "Unexpected failure: {}", err.what());
		    }
		    auto key = f1.get();
		    versionstamp = std::string(key.begin(), key.end());
		    break;
	    }

	    ASSERT(versionstamp.size() > 0, "Expect versionstamp's size > 0");

	    tr = env.newNormalOrTenantTransaction();
	    auto f = runToCompletion(
	        tr,
	        [&prefix, &versionstamp](auto tr) { return tr.get(fdb::toBytesRef(prefix + "foo" + versionstamp), false); },
	        "CheckVersionstamp");

	    ASSERT(f.get().has_value(), "Unexpected null value");
	    ASSERT(f.get() == "bar"_br, "unmatched value: expected=bar, actual={}", fdb::toCharsRef(f.get()));
    });

AddTest testAtomicOptionMutationTypeSetVersionstampedValue(
    "AtomicOptionMutationTypeSetVersionstampedValue",
    ALL,
    [](const TestEnv& env) {
	    const std::string prefix("combify-atomic-op-mutation-type-set-versionstamped-value-");
	    char valbuf[] = { 'b', 'a', 'r', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', 3, 0, 0, 0 };
	    std::string versionstamp("");
	    std::string key = prefix + "foo";

	    auto tr = env.newNormalOrTenantTransaction();

	    while (1) {
		    tr.atomicOp(fdb::toBytesRef(prefix + "foo"),
		                fdb::BytesRef(reinterpret_cast<const uint8_t*>(valbuf), 17),
		                FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE);
		    auto f1 = tr.getVersionstamp();
		    auto f2 = tr.commit();

		    auto err = waitFuture(f2);
		    if (err) {
			    auto f3 = tr.onError(err);
			    if (auto err3 = waitFuture(f3)) {
				    ASSERT(false, "Unexpected failure: {}", err3.what());
			    }
			    continue;
		    }

		    if (auto err = waitFuture(f1)) {
			    ASSERT(false, "Unexpected failure: {}", err.what());
		    }
		    auto key = f1.get();
		    versionstamp = std::string(key.begin(), key.end());
		    break;
	    }

	    ASSERT(versionstamp.size() > 0, "Expect versionstamp's size > 0");

	    tr = env.newNormalOrTenantTransaction();
	    auto f = runToCompletion(
	        tr, [&key](auto tr) { return tr.get(fdb::toBytesRef(key), false); }, "CheckValue");

	    auto expectedVal = "bar" + versionstamp;

	    ASSERT(f.get().has_value(), "Unexpected null value");
	    ASSERT(std::string(f.get()->begin(), f.get()->end()) == expectedVal,
	           "unmatched value: expected={}, actual={}",
	           fdb::toCharsRef(expectedVal),
	           fdb::toCharsRef(f.get()));
    });

AddTest testAtomicOpMutationTypeByteMax("AtomicOpMutationTypeByteMax", ALL, [](const TestEnv& env) {
	// The difference with FDB_MUTATION_TYPE_MAX is that strings will not be
	// extended/truncated so lengths match.
	const std::string prefix("combify-atomic-op-mutation-type-byte-max-");
	auto tr = env.newNormalOrTenantTransaction();
	insertAll(tr, applyKeyPrefix(prefix, { { "foo", "a" }, { "bar", "b" }, { "baz", "cba" } }));

	tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    tr.atomicOp(fdb::toBytesRef(prefix + "foo"), "b"_br, FDB_MUTATION_TYPE_BYTE_MAX);
		    tr.atomicOp(fdb::toBytesRef(prefix + "bar"), "cc"_br, FDB_MUTATION_TYPE_BYTE_MAX);
		    tr.atomicOp(fdb::toBytesRef(prefix + "baz"), "b"_br, FDB_MUTATION_TYPE_BYTE_MAX);
		    return tr.commit();
	    },
	    "MutationTypeByteMax");

	// Check results
	for (auto const& keyVal :
	     { std::make_tuple("foo", "b"_br), std::make_tuple("bar", "cc"_br), std::make_tuple("baz", "cba"_br) }) {
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

AddTest testAtomicOpMutationTypeByteMin("AtomicOpMutationTypeByteMin", ALL, [](const TestEnv& env) {
	// The difference with FDB_MUTATION_TYPE_MIN is that strings will not be
	// extended/truncated so lengths match.
	const std::string prefix("combify-atomic-op-mutation-type-byte-min-");
	auto tr = env.newNormalOrTenantTransaction();
	insertAll(tr, applyKeyPrefix(prefix, { { "foo", "a" }, { "bar", "b" }, { "baz", "abc" } }));

	tr = env.newNormalOrTenantTransaction();
	runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    tr.atomicOp(fdb::toBytesRef(prefix + "foo"), "b"_br, FDB_MUTATION_TYPE_BYTE_MIN);
		    tr.atomicOp(fdb::toBytesRef(prefix + "bar"), "aa"_br, FDB_MUTATION_TYPE_BYTE_MIN);
		    tr.atomicOp(fdb::toBytesRef(prefix + "baz"), "b"_br, FDB_MUTATION_TYPE_BYTE_MIN);
		    return tr.commit();
	    },
	    "MutationTypeByteMin");

	// Check results
	for (auto const& keyVal :
	     { std::make_tuple("foo", "a"_br), std::make_tuple("bar", "aa"_br), std::make_tuple("baz", "abc"_br) }) {
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

/* Transaction Watch */
AddTest testTransactionWatchRYWDisabled("TransactionWatchRYWDisabled", asTrustedClient, [](const TestEnv& env) {
	// Watches created on a transaction with the option READ_YOUR_WRITES_DISABLE
	// should return a watches_disabled error.
	auto tr = env.newNormalOrTenantTransaction();
	try {
		auto f = runToCompletion(
		    tr,
		    [](auto tr) {
			    tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
			    return tr.watch("foo"_br);
		    },
		    "WatchDisabled");
		ASSERT(false, "Expect watch to fail");
	} catch (fdb::Error e) {
		ASSERT(e.code() == 1034); // watches_disabled
	}
});

AddTest testTransactionWatchReset("TransactionWatchReset", asTrustedClient, [](const TestEnv& env) {
	// Resetting (or destroying) an uncommitted transaction should cause watches
	// created by the transaction to fail with a transaction_cancelled error.
	auto tr = env.newNormalOrTenantTransaction();
	try {
		auto f = runToCompletion(
		    tr,
		    [](auto tr) {
			    auto f = tr.watch("foo"_br);
			    tr.reset();
			    return f;
		    },
		    "TransactionCancelledAfterWatch");
		ASSERT(false, "Expect watch to fail");
	} catch (fdb::Error e) {
		ASSERT(e.code() == 1025); // transaction_cancelled
	}
});

AddTest testTransactionMaxWatches("TransactionMaxWatches", asTrustedClient, [](const TestEnv& env) {
	const std::string prefix("combify-transaction-max-watches-");
	int64_t max_watches = 3;
	auto db = env.newDatabase();
	db.setOption(FDB_DB_OPTION_MAX_WATCHES, max_watches);

	auto tr = db.createTransaction();
	while (1) {
		std::vector<fdb::Future> futures;
		for (auto c = 'a'; c <= 'd'; c++) {
			futures.push_back(tr.watch(fdb::toBytesRef(prefix + c)));
		}

		auto f = tr.commit();
		if (auto err = waitFuture(f)) {
			auto f2 = tr.onError(err);
			if (auto err2 = waitFuture(f2)) {
				ASSERT(false, "Unexpected failure: {}", err2.what());
			}
			continue;
		}

		std::binary_semaphore sem(0);
		// Callbacks will be triggered with operation_cancelled errors once the
		// too_many_watches error fires, as the other futures will go out of scope
		// and be cleaned up. The future which too_many_watches occurs on is
		// nondeterministic, so each future is checked.
		for (auto& ft : futures) {
			ft.then([&sem](fdb::Future f) {
				ASSERT(!f.blockUntilReady(), "Unexpected future unready");
				auto err = f.error();
				if (err.code() != /*operation_cancelled*/ 1101 && !err.retryable()) {
					ASSERT(err.code() == 1032); // too_many_watches
				}
				sem.release();
			});
		}

		// We only expect 1 of the callbacks above to be fired because the max watches
		// is set to 3 and there are four watches, leading to one of the future to fail
		// and causing the callback to be called. The other three watches will wait
		// because no changes have been made to any of the keys used in this test.
		sem.acquire();
		break;
	}

	// Reset available number of watches.
	max_watches = 10000;
	db.setOption(FDB_DB_OPTION_MAX_WATCHES, max_watches);
});

AddTest testTransactionWatch("TransactionWatch", asTrustedClient, [](const TestEnv& env) {
	const std::string prefix("combify-transaction-watch-");
	auto tr = env.newNormalOrTenantTransaction();
	insertAll(tr, applyKeyPrefix(prefix, { { "foo", "foo" } }));

	std::binary_semaphore sem(0);
	auto db = env.newDatabase();
	tr = env.newNormalOrTenantTransaction();

	while (1) {
		auto f1 = tr.watch(fdb::toBytesRef(prefix + "foo"));
		auto f2 = tr.commit();

		auto err = waitFuture(f2);
		if (err) {
			auto f3 = tr.onError(err);
			if (auto err = waitFuture(f3)) {
				ASSERT(false, "Unexpected failure: {}", err.what());
			}
			continue;
		}

		f1.then([&sem](fdb::Future f) { sem.release(); });

		// Update value for key "foo" to trigger the watch.
		insertAll(db, applyKeyPrefix(prefix, { { "foo", "bar" } }));
		sem.acquire();
		break;
	}
});

} // anonymous namespace
