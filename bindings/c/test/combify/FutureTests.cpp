#include <semaphore>
#include "Combify.hpp"
#include "TestHelpers.hpp"

namespace {

using namespace combify;
using namespace fdb::literals;

/* Transaction future operation  */
AddTest testFutureCallback("FutureCallback", ALL, [](const TestEnv& env) {
	auto db = env.newDatabase();
	auto tr = env.newNormalOrTenantTransaction(db);
	while (1) {
		auto f = tr.get("foo"_br, /* snapshot */ true);

		std::binary_semaphore sem(0);
		f.then([&sem](fdb::Future f) { sem.release(); });

		auto err = waitFuture(f);
		// Wait until callback is called
		sem.acquire();

		if (err) {
			auto f2 = tr.onError(err);
			if (auto err2 = waitFuture(f2)) {
				ASSERT(false, "Unexpected failure: {}", err2.what());
			}
			continue;
		}

		break;
	}
});

AddTest testFutureCancelAfterCompletion("FutureCancelAfterCompletion", ALL, [](const TestEnv& env) {
	auto db = env.newDatabase();
	auto tr = env.newNormalOrTenantTransaction(db);
	auto f = runToCompletion(
	    tr, [](auto tr) { return tr.get("foo"_br, false); }, "FutureCancelAfterCompletion");

	// Should have no effect
	f.cancel();
	std::optional<fdb::ValueRef> val;
	auto err = f.getNothrow(val);
	ASSERT(!err, "Unexpected error: {}", err.code());
});

AddTest testFutureReleaseMemory("FutureReleaseMemory", ALL, [](const TestEnv& env) {
	auto db = env.newDatabase();
	auto tr = env.newNormalOrTenantTransaction(db);
	auto f = runToCompletion(
	    tr, [](auto tr) { return tr.get("foo"_br, false); }, "FutureReleaseMemory");

	// "After [fdb_future_release_memory] has been called the same number of
	// times as fdb_future_get_*(), further calls to fdb_future_get_*() will
	// return a future_released error".
	std::optional<fdb::ValueRef> value;
	auto err = f.getNothrow(value);
	ASSERT(!err, "Unexpected error: {}", err.code());
	err = f.getNothrow(value);
	ASSERT(!err, "Unexpected error: {}", err.code());

	f.releaseMemory();
	err = f.getNothrow(value);
	ASSERT(!err, "Unexpected error: {}", err.code());

	f.releaseMemory();
	f.releaseMemory();
	err = f.getNothrow(value);
	ASSERT(err.code() == 1102, "unmatched error code: expect=1102 (future_released), actual={}", err.code());
});

AddTest testFutureBlockFromCallback("FutureBlockFromCallback", ALL, [](const TestEnv& env) {
	auto db = env.newDatabase();
	auto tr = env.newNormalOrTenantTransaction(db);
	auto f1 = tr.get("foo"_br, /*snapshot*/ true);
	std::binary_semaphore sem(0);
	f1.then([&sem, &tr](fdb::Future f) {
		auto f2 = tr.get("bar"_br, /*snapshot*/ true);
		auto err = f2.blockUntilReady();
		if (err) {
			ASSERT(err.code() == /*blocked_from_network_thread*/ 2026);
		}
		sem.release();
	});
	sem.acquire();
});

} // namespace
