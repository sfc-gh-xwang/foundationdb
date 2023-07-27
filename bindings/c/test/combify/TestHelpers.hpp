/*
 * TestHelpers.hpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */
#ifndef COMBIFY_TEST_HELPERS_HPP
#define COMBIFY_TEST_HELPERS_HPP

#include <concepts>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>

#include <fmt/format.h>
#include <fdb_api.hpp>

namespace combify {

namespace detail {

// Utility to check whether a class is from a template
template <class T, template <class...> class TEMPLATE>
struct IsInstance : std::false_type {};

template <class T, template <class...> class TEMPLATE>
struct IsInstance<TEMPLATE<T>, TEMPLATE> : std::true_type {};

} // namespace detail

template <class T, template <class...> class TEMPLATE>
concept TemplateInstance = detail::IsInstance<T, TEMPLATE>::value;

template <class T>
concept TypedFuture = TemplateInstance<T, fdb::TypedFuture>;

template <class T>
concept AnyFuture = TemplateInstance<T, fdb::TypedFuture> || std::same_as<T, fdb::Future>;

template <class Func>
concept TransactionLambda = requires(Func&& fn) {
	{ fn(std::declval<fdb::Transaction>()) } -> AnyFuture;
};

// Utility function to avoid having to repeatedly write transaction retry loops.
// Function shall take a transaction object as parameter and return some future.
template <TransactionLambda Fn>
auto runToCompletion(fdb::Transaction tr, Fn&& fn, string_view what = "") {
	while (true) {
		auto f = fn(tr);
		if (auto err = f.blockUntilReady()) {
			throw std::runtime_error(fmt::format("blockUntilReady() RETURNED '{}' ERROR{}'",
			                                     err.what(),
			                                     what.empty() ? string() : fmt::format(" DURING {}", what)));
		}
		if (auto err = f.error()) {
			fmt::print("[DEBUG] {} TRANSACTION RETURNED {}ERROR {}\n",
			           what.empty() ? "ANONYMOUS" : what,
			           err.retryable() ? "RETRYABLE " : "",
			           err.what());
			auto errf = tr.onError(err);
			if (auto berr = errf.blockUntilReady()) {
				throw std::runtime_error(fmt::format("blockUntilReady() ON ON_ERROR() FUTURE RETURNED '{}' ERROR{}'",
				                                     berr.what(),
				                                     what.empty() ? string() : fmt::format(" DURING {}", what)));
			}
			if (auto oerr = errf.error()) {
				// Cannot go on. Throw.
				throw oerr;
			}
		} else {
			return f;
		}
	}
}

// Starts a network thread which joins as it destructs.
inline auto setupAndStartNetworkGuarded() {
	fdb::network::setup();
	auto thread = std::thread([]() {
		if (auto err = fdb::network::run()) {
			throw std::runtime_error(fmt::format("SETUP NETWORK THREAD RAN INTO ERROR: {}", err.what()));
		}
	});
	return ScopeExit([thread = std::move(thread)]() mutable {
		fdb::network::stop();
		thread.join();
	});
}

// Given a string s, returns the "lowest" string greater than any string that
// starts with s. Taken from
// https://github.com/apple/foundationdb/blob/e7d72f458c6a985fdfa677ae021f357d6f49945b/flow/flow.cpp#L223.
inline std::string strincStr(const std::string& s) {
	int index = -1;
	for (index = s.size() - 1; index >= 0; --index) {
		if ((uint8_t)s[index] != 255) {
			break;
		}
	}

	assert(index >= 0);

	std::string r = s.substr(0, index + 1);
	char* p = r.data();
	p[r.size() - 1]++;
	return r;
}

#define ASSERT(condition)                                                                                              \
	if (!(condition)) {                                                                                                \
		fmt::print(stderr, "Assertion {} failed @ {}:{}", #condition, __FILE__, __LINE__);                             \
		throw std::runtime_error("assertion failure");                                                                 \
	}

} // namespace combify

#endif /*COMBIFY_TEST_HELPERS_HPP*/
