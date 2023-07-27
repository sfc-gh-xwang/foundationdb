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

} // namespace combify

#endif /*COMBIFY_TEST_HELPERS_HPP*/
