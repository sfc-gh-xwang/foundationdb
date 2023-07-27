#include "Combify.hpp"
#include "TestHelpers.hpp"
#ifdef NDEBUG
#undef NDEBUG
#endif

// Prevent ODR clash between sources
namespace {

using namespace combify;
using namespace fdb::literals;

AddTest testWriteAndRead("BasicWriteAndReadBack", asTrustedClient, [](const TestEnv& env) {
	auto db = env.newDatabase();

	auto tr = env.newTransaction(db);

	runToCompletion(
	    tr,
	    [](auto tr) {
		    tr.set("abc"_br, "def"_br);
		    return tr.commit();
	    },
	    "WriteSomeValue");

	tr = env.newTransaction(db);
	auto f = runToCompletion(
	    tr, [](auto tr) { return tr.get("abc"_br, false); }, "ReadBackValue");
	ASSERT(f.get() == "def"_br);
});

AddTest testGetReadVersion("BasicGetReadVersion", asTrustedClient, [](const TestEnv& env) {
	auto db = env.newDatabase();
	auto tr = env.newTransaction(db);

	auto f = runToCompletion(
	    tr, [](auto tr) { return tr.getReadVersion(); }, "GetReadVersion");

	ASSERT(f.get() > 0);
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

	ASSERT(fdb::toCharsRef(f.get()) == prefix + "bar");
});

AddTest testGetValue("BasicGetValue", asTrustedClient, [](const TestEnv& env) {
	const std::string prefix("combify-basic-get-value");
	env.clearAndPopulateDBData(prefix, { { "foo", "bar" } });

	auto db = env.newDatabase();
	auto tr = env.newTransaction(db);
	auto f = runToCompletion(
	    tr, [&prefix](auto tr) { return tr.get(fdb::toBytesRef(prefix + "foo"), /* snapshot */ false); }, "GetValue");

	ASSERT(fdb::toCharsRef(f.get()) == "bar");
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
		ASSERT(strlen(strings[i]) > 0);
	}
});

AddTest testReadSystemKeyDisallowed(
    "ReadSystemKeyDisallowed",
    AND(withoutMultiVersion, withoutGrvCache, asTrustedClient),
    [](const TestEnv& env) {
	    auto db = env.newDatabase();
	    auto tr = env.newTransaction(db);
	    try {
		    auto f = runToCompletion(
		        tr, [](auto tr) { return tr.get("\xff/coordinators"_br, /* snapshot */ false); }, "ReadSystemKeyFail");
		    ASSERT(false);
	    } catch (fdb::Error e) {
		    ASSERT(e.code() == 2004); // key_outside_legal_range
	    }
    });

AddTest testReadSystemKey("ReadSystemKey", asTrustedClient, [](const TestEnv& env) {
	auto db = env.newDatabase();
	auto tr = env.newTransaction(db);
	auto f = runToCompletion(
	    tr,
	    [](auto tr) {
		    tr.setOption(FDB_TR_OPTION_READ_SYSTEM_KEYS);
		    return tr.get("\xff/coordinators"_br, /* snapshot */ false);
	    },
	    "ReadSystemKey");
	ASSERT(f.get().has_value());
});

AddTest testWriteSystemKeyDisallowed("WriteSystemKeyDisallowed",
                                     AND(withoutMultiVersion, withoutGrvCache, asTrustedClient),
                                     [](const TestEnv& env) {
	                                     auto db = env.newDatabase();
	                                     auto tr = env.newTransaction(db);
	                                     try {
		                                     auto f = runToCompletion(
		                                         tr,
		                                         [](auto tr) {
			                                         tr.set("\xff\x02"_br, "bar"_br);
			                                         return tr.commit();
		                                         },
		                                         "WriteSystemKeyFail");
		                                     ASSERT(false);
	                                     } catch (fdb::Error e) {
		                                     ASSERT(e.code() == 2004); // key_outside_legal_range
	                                     }
                                     });

} // anonymous namespace
