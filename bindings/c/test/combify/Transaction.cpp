#include "Combify.hpp"
#include "TestHelpers.hpp"
#include <optional>

// Prevent ODR clash between sources
namespace {

using namespace combify;
using namespace fdb::literals;

// getValue is a helper function to set options to a transaction and get the value of a given key
auto getValue(fdb::Transaction tr,
              fdb::KeyRef key,
              bool snapshot = false,
              string_view what = "",
              const std::vector<FDBTransactionOption>& options = {}) {

	return runToCompletion(
	    tr,
	    [key, snapshot, options](auto tr) {
		    for (const auto& opt : options) {
			    tr.setOption(opt);
		    }
		    return tr.get(key, snapshot);
	    },
	    what);
}

// setValue is a helper function to set options to a transaction and set a key value
auto setValue(fdb::Transaction tr,
              fdb::KeyRef key,
              fdb::ValueRef val,
              string_view what = "",
              const std::vector<FDBTransactionOption>& options = {}) {
	return runToCompletion(
	    tr,
	    [key, val, options](auto tr) {
		    for (const auto& opt : options) {
			    tr.setOption(opt);
		    }
		    tr.set(key, val);
		    return tr.commit();
	    },
	    what);
}

AddTest testWriteAndRead("BasicWriteAndReadBack", asTrustedClient, [](const TestEnv& env) {
	auto db = env.newDatabase();

	auto tr = env.newTransaction(db);
	setValue(tr, "abc"_br, "def"_br, "WriteSomeValue");

	tr = env.newTransaction(db);
	auto f = getValue(tr, "abc"_br, false, "ReadBackValue");
	ASSERT(f.get() == "def"_br, "unmatched read back value: expected=def actual={}", fdb::toCharsRef(f.get()));
});

AddTest testGetReadVersion("BasicGetReadVersion", asTrustedClient, [](const TestEnv& env) {
	auto db = env.newDatabase();
	auto tr = env.newTransaction(db);

	auto f = runToCompletion(
	    tr, [](auto tr) { return tr.getReadVersion(); }, "GetReadVersion");

	ASSERT(f.get() > 0, "unmatched read version value: expected > 0");
});

AddTest testGetKey("BasicGetKey", asTrustedClient, [](const TestEnv& env) {
	const std::string prefix("combify-basic-get-key");
	env.clearAndPopulateDBData(prefix, { { "a", "1" }, { "baz", "2" }, { "bar", "3" } });

	auto db = env.newDatabase();
	auto tr = env.newTransaction(db);

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

AddTest testGetValue("BasicGetValue", asTrustedClient, [](const TestEnv& env) {
	const std::string prefix("combify-basic-get-value");
	env.clearAndPopulateDBData(prefix, { { "foo", "bar" } });

	auto db = env.newDatabase();
	auto tr = env.newTransaction(db);
	auto f = getValue(tr, fdb::toBytesRef(prefix + "foo"), false, "GetValue");

	ASSERT(f.get() == "bar"_br, "unmatched read value: expected=bar actual={}", fdb::toCharsRef(f.get()));
});

AddTest testGetStringArray("GetStringArray", asTrustedClient, [](const TestEnv& env) {
	const std::string prefix("combify-get-string-array");
	env.clearAndPopulateDBData(prefix, { { "foo", "bar" } });
	auto db = env.newDatabase();
	auto tr = env.newTransaction(db);
	auto f = runToCompletion(
	    tr, [&prefix](auto tr) { return tr.getAddressForKey(fdb::toBytesRef(prefix + "foo")); }, "GetStringArray");

	auto const& [strings, count] = f.get();
	ASSERT(count > 0);
	for (int i = 0; i < count; ++i) {
		ASSERT(strlen(strings[i]) > 0, "unexpected empty string");
	}
});

AddTest testReadSystemKeyDisallowed("ReadSystemKeyDisallowed",
                                    AND(withoutMultiVersion, withoutGrvCache, asTrustedClient),
                                    [](const TestEnv& env) {
	                                    auto db = env.newDatabase();
	                                    auto tr = env.newTransaction(db);
	                                    try {
		                                    getValue(tr, "\xff/coordinators"_br, false, "ReadSystemKeyFail");
		                                    ASSERT(false);
	                                    } catch (fdb::Error e) {
		                                    ASSERT(e.code() == 2004); // key_outside_legal_range
	                                    }
                                    });

AddTest testReadSystemKey("ReadSystemKey", asTrustedClient, [](const TestEnv& env) {
	auto db = env.newDatabase();
	auto tr = env.newTransaction(db);
	auto f = getValue(tr, "\xff/coordinators"_br, false, "ReadSystemKey", { FDB_TR_OPTION_READ_SYSTEM_KEYS });
	ASSERT(f.get().has_value(), "Unexpected null value from read");
});

AddTest testWriteSystemKeyDisallowed("WriteSystemKeyDisallowed",
                                     AND(withoutMultiVersion, withoutGrvCache, asTrustedClient),
                                     [](const TestEnv& env) {
	                                     auto db = env.newDatabase();
	                                     auto tr = env.newTransaction(db);
	                                     try {
		                                     auto f = setValue(tr, "\xff\x02"_br, "bar"_br, "WriteSystemKeyFail");
		                                     ASSERT(false);
	                                     } catch (fdb::Error e) {
		                                     ASSERT(e.code() == 2004); // key_outside_legal_range
	                                     }
                                     });

AddTest testWriteSystemKey("WriteSystemKey", asTrustedClient, [](const TestEnv& env) {
	auto db = env.newDatabase();
	auto tr = env.newTransaction(db);
	auto syskey = "\xff\x02"_br;

	// Write a system key
	setValue(tr, syskey, "bar"_br, "WriteSystemKey", { FDB_TR_OPTION_ACCESS_SYSTEM_KEYS });

	// Read and verify the system key value
	tr = env.newTransaction(db);
	auto f = getValue(tr, syskey, false, "ReadBackSystemKey", { FDB_TR_OPTION_READ_SYSTEM_KEYS });

	ASSERT(f.get() == "bar"_br, "unexpected read value: expected=bar actual={}", fdb::toCharsRef(f.get()));
});

} // anonymous namespace
