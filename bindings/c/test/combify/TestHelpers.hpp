/*
 * TestHelpers.hpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */
#ifndef COMBIFY_TEST_HELPERS_HPP
#define COMBIFY_TEST_HELPERS_HPP

#include <concepts>
#include <iterator>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>

#include <execinfo.h>

#include <fmt/format.h>
#include <fdb_api.hpp>

#ifdef ASSERT
#error "ASSERT macro clashes with existing definition"
#else
#define ASSERT(condition, ...)                                                                                         \
	do {                                                                                                               \
		if (!(condition)) {                                                                                            \
			fmt::memory_buffer buf;                                                                                    \
			fmt::format_to(                                                                                            \
			    std::back_inserter(buf), "Assertion '" #condition "' failed at " __FILE__ ":{} ", __LINE__);           \
			fmt::format_to(std::back_inserter(buf), "" __VA_ARGS__);                                                   \
			throw std::runtime_error(std::string(buf.data(), buf.size()));                                             \
		}                                                                                                              \
	} while (0)
#endif /*ASSERT*/

// UID mirrors the one defined in flow as client test should not depend on flow.
struct UID {
	uint64_t part[2];
	constexpr UID(uint64_t a, uint64_t b) : part{ a, b } {}
	uint64_t first() const { return part[0]; }
	uint64_t second() const { return part[1]; }
	bool operator==(const UID& r) const { return part[0] == r.part[0] && part[1] == r.part[1]; }

	static UID fromString(std::string const& s) {
		ASSERT(s.size() == 32);
		uint64_t a = 0, b = 0;
		ASSERT(sscanf(s.c_str(), "%16" SCNx64 "%16" SCNx64, &a, &b) == 2);
		return UID(a, b);
	}

	std::string toString() const { return fmt::format("{:016x}{:016x}", part[0], part[1]); }
};

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
	std::map<fdb::Error, int> stats;
	// make sure error stats print before leaving scope
	auto errorPrintGuard = ScopeExit([&stats, &what]() {
		if (!stats.empty()) {
			fmt::print("runToCompletion({}) errors:", what);
			for (const auto& [err, times] : stats) {
				fmt::print(" ('{}', {})", err.what(), times);
			}
			fmt::print("\n");
		}
	});
	auto throwError = [&what](std::string_view format, const char* whichError) {
		auto buf = fmt::memory_buffer{};
		fmt::format_to(std::back_inserter(buf), "runToCompletion({}): ", what);
		fmt::format_to(std::back_inserter(buf), fmt::runtime(format), whichError);
		throw std::runtime_error(std::string(buf.data(), buf.size()));
	};
	while (true) {
		auto f = fn(tr);
		if (auto err = f.blockUntilReady()) {
			throwError("blockUntilReady() returned '{}'", err.what());
		}
		if (auto err = f.error()) {
			stats[err] += 1;
			auto errf = tr.onError(err);
			if (auto berr = errf.blockUntilReady()) {
				throwError("blockUntilReady() on onError() future returned '{}'", berr.what());
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
			throw std::runtime_error(fmt::format("fdb::network::run(): {}", err.what()));
		}
	});
	return ScopeExit([thread = std::move(thread)]() mutable {
		fdb::network::stop();
		thread.join();
	});
}

inline void insertAll(fdb::Transaction tr, const std::vector<std::pair<std::string, std::string>>& data) {
	runToCompletion(
	    tr,
	    [&data](auto tr) {
		    // Insert data
		    for (const auto& d : data) {
			    tr.set(fdb::toBytesRef(d.first), fdb::toBytesRef(d.second));
		    }
		    return tr.commit();
	    },
	    "PopulateDBData");
}

inline void insertAll(fdb::Database db, const std::vector<std::pair<std::string, std::string>>& data) {
	return insertAll(db.createTransaction(), data);
}

} // namespace combify

#endif /*COMBIFY_TEST_HELPERS_HPP*/
