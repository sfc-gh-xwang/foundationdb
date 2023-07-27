#include "Combify.hpp"
#include "TestHelpers.hpp"
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
		// TODO: This needs to be fixed
		// ASSERT(false);
	} catch (fdb::Error e) {
		ASSERT(e.code() == 6000);
	}
});

} // anonymous namespace
