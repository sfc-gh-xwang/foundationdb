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

AddTest test3(
    "TokenAsTenantOption",
    AND(asUntrustedClient, withTls, withAuthZ, withTenantsMoreThan(1)),
    [](const TestEnv& env) {
	    auto db = env.newDatabase();
	    auto const& [tenantName, tenantId] = env.anyTenant();
	    auto tenant = db.openTenant(tenantName);
	    tenant.setOption(FDB_TENANT_OPTION_AUTHORIZATION_TOKEN, env.staticOptions.authzConfig->signToken({ tenantId }));
	    // token set as tenant option forwards to any new transactions created from tenant.
	    auto tr = tenant.createTransaction();
	    runToCompletion(
	        tr,
	        [](auto tr) {
		        tr.set("abc"_br, "def"_br);
		        return tr.commit();
	        },
	        "WriteWithDefaultTenantToken");
	    // resetting does not clear the token, as it is transaction default.
	    tr.reset();
	    {
		    auto f = runToCompletion(
		        tr, [](auto tr) { return tr.get("abc"_br, false); }, "ReadBackValue");
		    ASSERT(f.get() == "def"_br, "unmatched read back value: expected=def actual={}", fdb::toCharsRef(f.get()));
	    }

	    // you can override the default token by setting it directly on the transaction (in this case, the wrong token)
	    tr.reset(); // reset RYW cache and set the token back to tenant default
	    tr.setOption(FDB_TR_OPTION_AUTHORIZATION_TOKEN, env.staticOptions.authzConfig->signToken({ tenantId + 1 }));
	    try {
		    runToCompletion(
		        tr, [](auto tr) { return tr.get("abc"_br, false); }, "GetValueUsingTokenForAnotherTenant");
		    ASSERT(false, "expected exception upon cross-tenant get");
	    } catch (fdb::Error e) {
		    ASSERT(e.code() == 6000);
	    }

	    // resetting the transaction would restore the default token again.
	    tr.reset();
	    {
		    auto f = runToCompletion(
		        tr, [](auto tr) { return tr.get("abc"_br, false); }, "ReadBackValue");
		    ASSERT(f.get() == "def"_br, "unmatched read back value: expected=def actual={}", fdb::toCharsRef(f.get()));
	    }

	    // but not if you reset the token option on the tenant
	    tenant.setOption(FDB_TENANT_OPTION_AUTHORIZATION_TOKEN, fdb::BytesRef());
	    tr = tenant.createTransaction();
	    try {
		    runToCompletion(
		        tr, [](auto tr) { return tr.get("abc"_br, false); }, "GetValueUsingTransactionFromTenantWithoutToken");
		    ASSERT(false, "expected exception upon cross-tenant get");
	    } catch (fdb::Error e) {
		    ASSERT(e.code() == 6000);
	    }
    });

} // anonymous namespace
