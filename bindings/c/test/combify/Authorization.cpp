#include "Combify.hpp"
#include "TestHelpers.hpp"
#include <fmt/format.h>
#ifdef NDEBUG
#undef NDEBUG
#endif

// Prevent ODR clash between sources
namespace {

using namespace combify;
using namespace fdb::literals;

AddTest test1("AuthZCrossTenantDisallowed", AND(withAuthZ, withTls, withTenantsMoreThan(1)), [](const TestEnv& env) {
	auto db = env.newDatabase();
	const auto& [tenantNameA, tenantIdA] = env.tenantByIndex(0);
	const auto& [tenantNameB, tenantIdB] = env.tenantByIndex(1);

	auto trA = env.newTenantTransaction(db, tenantNameA);

	runToCompletion(
	    trA,
	    [](auto trA) {
		    trA.set("abc"_br, "def"_br);
		    return trA.commit();
	    },
	    "SetKeyValueForTenantA");

	auto tokenForB = env.staticOptions.authzConfig->signToken({ tenantIdB });
	trA = env.newTenantTransaction(db, tenantNameA);
	trA.setOption(FDB_TR_OPTION_AUTHORIZATION_TOKEN, tokenForB);
	try {
		runToCompletion(
		    trA, [](auto trA) { return trA.get("abc"_br, false); }, "GetValueUsingTokenForTenantB");
		ASSERT(false);
	} catch (fdb::Error e) {
		ASSERT(e.code() == 6000);
	}
});

AddTest test2("BasicWriteAndReadBack", asTrustedClient, [](const TestEnv& env) {
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

} // anonymous namespace
