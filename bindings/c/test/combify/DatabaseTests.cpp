#include "Combify.hpp"
#include "TestHelpers.hpp"

// Prevent ODR clash between sources
namespace {

using namespace combify;
using namespace fdb::literals;
AddTest testDBTransactionOptionTimeout("DBTransactionOptionTimeout", ALL, [](const TestEnv& env) {
	auto db = env.newDatabase();
	// Set smallest possible timeout, retry until a timeout occurs.
	int64_t timeout = 1;
	db.setOption(FDB_DB_OPTION_TRANSACTION_TIMEOUT, timeout);
	auto tr = env.newNormalOrTenantTransaction(db);
	fdb::Error err;
	while (!err) {
		auto f1 = tr.get("foo"_br, /* snapshot */ false);
		err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			err = waitFuture(f2);
		}
	}
	ASSERT(err.code() == 1031 /* transaction_timed_out */, "Unexpected failure, expect=1031, actual={}", err.code());
});

AddTest testDBTransactionExceedsSizeLimit("DBTransactionExceedsSizeLimit", asTrustedClient, [](const TestEnv& env) {
	auto db = env.newDatabase();
	auto sizeLimit = 32;
	db.setOption(FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT, sizeLimit);
	auto tr = env.newTransaction(db);

	try {
		runToCompletion(
		    tr,
		    [](auto tr) {
			    tr.set("foo"_br, "foundation database is amazing"_br);
			    return tr.commit();
		    },
		    "SetKeyExceedSizeLimit");
		ASSERT(false, "unexpected transaction successful return");
	} catch (fdb::Error e) {
		ASSERT(e.code() == 2101, "unmatched error code: expected=2101 (transaction_too_large), actual={}", e.code());
	}

	// Set size limit back to default.
	sizeLimit = 10000000;
	db.setOption(FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT, sizeLimit);
});

AddTest testDBGetServerProtocol("DBGetServerProtocol", ALL, [](const TestEnv& env) {
	auto db = env.newDatabase();
	// We don't really have any expectations other than "don't crash" here
	auto protocolFuture = db.getServerProtocol(0);
	auto err = waitFuture(protocolFuture);
	ASSERT(!err, "Unexpected failure: {}", err.what());

	uint64_t out = protocolFuture.get();

	// Passing in an expected version that's different than the cluster version
	protocolFuture = db.getServerProtocol(0x0FDB00A200090000LL);
	err = waitFuture(protocolFuture);
	ASSERT(!err, "Unexpected failure: {}", err.what());

	err = protocolFuture.getNothrow(out);
	ASSERT(!err, "Unexpected failure: {}", err.what());
});

AddTest testDBCreateSnapshot("DBCreateSnapshot", asTrustedClient, [](const TestEnv& env) {
	auto db = env.newDatabase();
	std::string snapshot_command = "test";
	std::string uid = "invalid_uid";
	bool retry = false;
	while (1) {
		auto f = db.createSnapshot(fdb::toBytesRef(uid), fdb::toBytesRef(snapshot_command));
		auto err = waitFuture(f);
		if (err.code() == 2509) { // expected error code
			ASSERT(!retry);
			uid = randomHexString<std::string>(32, 32);
			retry = true;
		} else if (err.code() == 2505) {
			ASSERT(retry);
			break;
		} else if (err.retryable()) {
			// Retry on other transient errors (e.g. cluster protocol change)
			continue;
		} else {
			// Otherwise, something went wrong.
			ASSERT(false, "Unexpected behavior: {}", err.what());
		}
	}
});

AddTest testMonitorNetworkBusyness("DBMonitorNetworkBusyness", ALL, [](const TestEnv& env) {
	// monitors network busyness for 2 sec (40 readings)
	auto db = env.newDatabase();
	bool containsGreaterZero = false;
	for (int i = 0; i < 40; i++) {
		double busyness = db.getMainThreadBusyness();
		// make sure the busyness is between 0 and 1
		ASSERT(busyness >= 0, "busyness is expected to be equal or greater than 0, actual: {}", busyness);
		ASSERT(busyness <= 1, "busyness is expected to be equal or smaller than 1, actual: {}", busyness);
		if (busyness > 0) {
			containsGreaterZero = true;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}

	// assert that at least one of the busyness readings was greater than 0
	ASSERT(containsGreaterZero);
});
} // namespace
