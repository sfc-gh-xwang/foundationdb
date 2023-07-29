#include "Combify.hpp"
#include "TestHelpers.hpp"
#include <fmt/format.h>

// Prevent ODR clash between sources
namespace {

using namespace combify;
using namespace fdb::literals;

AddTest test1("AuthZCrossTenantDisallowed",
              AND(withAuthZ, withTls, asUntrustedClient, withTenantsMoreThan(1)),
              [](const TestEnv& env) {
	              auto db = env.newDatabase();
	              const auto& [tenantNameA, tenantIdA] = env.tenantByIndex(0);
	              const auto& [tenantNameB, tenantIdB] = env.tenantByIndex(1);
	              fmt::print("TenantA: {} ID {}\n", fdb::toCharsRef(tenantNameA), tenantIdA);
	              fmt::print("TenantB: {} ID {}\n", fdb::toCharsRef(tenantNameB), tenantIdB);

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
		              ASSERT(false, "expected exception upon cross-tenant get");
	              } catch (fdb::Error e) {
		              ASSERT(e.code() == 6000);
	              }
              });

} // anonymous namespace
