#include "Combify.hpp"
#include "TestHelpers.hpp"

// Prevent ODR clash between sources
namespace {

using namespace combify;
using namespace fdb::literals;

// Helper function to add `prefix` to all keys in the given map. Returns a new map
auto applyKeyPrefix(const std::string& prefix, std::map<std::string, std::string>&& map) {
	std::map<std::string, std::string> out;
	for (const auto& [key, val] : map) {
		out[prefix + key] = val;
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
	const std::string prefix("combify-basic-get-key");
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
	const std::string prefix("combify-basic-get-value");
	auto tr = env.newNormalOrTenantTransaction();
	populateDBData(tr, applyKeyPrefix(prefix, { { "foo", "bar" } }));

	tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr, [&prefix](auto tr) { return tr.get(fdb::toBytesRef(prefix + "foo"), false); }, "GetValue");

	ASSERT(f.get() == "bar"_br, "unmatched read value: expected=bar actual={}", fdb::toCharsRef(f.get()));
});

AddTest testGetStringArray("GetKeyAddress", asTrustedClient, [](const TestEnv& env) {
	const std::string prefix("combify-get-string-array");
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

AddTest testReadYourWrites("ReadYourWrites", asTrustedClient, [](const TestEnv& env) {
	auto db = env.newDatabase();

	for (auto const option : { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE,
	                           FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE,
	                           FDB_TR_OPTION_SNAPSHOT_RYW_DISABLE }) {
		auto tr = env.newTransaction(db);
		tr.setOption(option);
		tr.set("foo"_br, "bar"_br);

		// Read before committing. Herer we test 3 different cases:
		// 1) If FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE is set, read with snapshot = false
		// 2) If FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE is set, read with snapshot = true
		// 3) If FDB_TR_OPTION_SNAPSHOT_RYW_DISABLE is set, read with both
		auto snapshot = option != FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE;

		auto f1 = runToCompletion(
		    tr, [snapshot](auto tr) { return tr.get("foo"_br, snapshot); }, "ReadValue");

		switch (option) {
		case FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE:
			ASSERT(!f1.get().has_value(), "Expect null value");
			break;
		case FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE:
			ASSERT(f1.get() == "bar"_br, "unmatched read value: expected=bar, actual={}", fdb::toCharsRef(f1.get()));
			break;
		case FDB_TR_OPTION_SNAPSHOT_RYW_DISABLE: {
			ASSERT(!f1.get().has_value(), "Expect null value");
			// Non-snapshot reads should still read writes in the transaction
			auto f2 = runToCompletion(
			    tr, [](auto tr) { return tr.get("foo"_br, false); }, "ReadValueNonSnapshot");

			ASSERT(f2.get() == "bar"_br, "unmatched read value: expected=bar, actual={}", fdb::toCharsRef(f2.get()));
			break;
		}
		default:
			ASSERT(false, "Unexpected option value: {}", option);
			break;
		}
	}
});

AddTest testGetKeyValueArray("GetKeyValueArray", ALL, [](const TestEnv& env) {
	const std::string prefix("combify-get-key-value-array");
	auto data = applyKeyPrefix(prefix, { { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
	auto tr = env.newNormalOrTenantTransaction();
	populateDBData(tr, data);

	tr = env.newNormalOrTenantTransaction();
	auto f = runToCompletion(
	    tr,
	    [&prefix](auto tr) {
		    return tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(prefix + "a")),
		                       fdb::key_select::lastLessOrEqual(fdb::toBytesRef(prefix + "c"), 1),
		                       /* limit */ 0,
		                       /* target_bytes */ 0,
		                       /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
		                       /* iteration */ 0,
		                       /* snapshot */ false,
		                       /* reverse */ false);
	    },
	    "GetKeyValueArray");

	auto [kvs, count, more] = f.get();
	ASSERT(count > 0, "count is expected to be larger than zero");
	ASSERT(count <= 3, "count is expected to be less or equan to 3");
	if (count < 3) {
		ASSERT(more, "more is expected to be true if count < 3");
	}
	for (int i = 0; i < count; i++) {
		auto kv = *kvs++;
		auto key = std::string(kv.key().begin(), kv.key().end());
		auto value = std::string(kv.value().begin(), kv.value().end());
		ASSERT(data[key] == value);
	}
});

} // anonymous namespace
