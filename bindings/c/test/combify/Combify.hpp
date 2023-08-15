/*
 * Combify.hpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once
#ifndef COMBIFY_HPP
#define COMBIFY_HPP

#ifndef FDB_API_VERSION
#define FDB_USE_LATEST_API_VERSION
#endif

#include <algorithm>
#include <array>
#include <cctype>
#include <concepts>
#include <list>
#include <map>
#include <string>
#include <string_view>
#include <optional>
#include <type_traits>
#include <utility>
#include <stdexcept>

#include <fdb_api.hpp>

#include <fmt/format.h>
#include <rapidjson/document.h>

#include "IterableOptions.hpp"
#include "NumericUtils.hpp"
#include "ScopeExit.hpp"
#include "StaticOptions.hpp"
#include "StringUtils.hpp"

namespace combify {

using std::optional;
using std::string;
using std::string_view;

namespace detail {

template <class... Ts>
std::tuple<const Ts&...> constTie(const Ts&... ts) {
	return std::tuple<const Ts&...>(ts...);
}

} // namespace detail

// Test functions will only have const-access to this object.
struct TestOptions {
	StaticOptions staticOptions;
	ForceMultiVersionOption forceMultiVersion;
	TrustedClientOption trustedClient;
	UseGrvCacheOption grvCache;

	// Shifts the test option to next valid combination.
	// Advancement is purely at the combinatoric level.
	// This function does not skip e.g. if static option and iterable option don't match (that check is done via
	// allOptionsValid()). Returns the iterable tier of shifted option if there are test state spaces to be explored.
	// Otherwise returns an empty optional, which should be the condition for terminating test run.
	optional<IterableOptionTier> next();

	auto getIterableOptions() {
		// Higher tier option (hence more expensive to shift to next option value) must place to the left:
		// e.g. forceMultiVersion is a network option, shifting of which requires
		// starting a new client subprocess, unlike grvCache, which can be trivially set on a transaction object.
		return std::tie(forceMultiVersion, trustedClient, grvCache);
	}

	auto getIterableOptions() const {
		// Higher tier option (hence more expensive to shift to next option value) must place to the left:
		// e.g. forceMultiVersion is a network option, shifting of which requires
		// starting a new client subprocess, unlike grvCache, which can be trivially set on a transaction object.
		return detail::constTie(forceMultiVersion, trustedClient, grvCache);
	}

	// Returns that all iterable options are in valid range AND it is a valid combination.
	bool allOptionsValid() const;

	string shortSummary() const { return shortSummaryStatic() + " " + shortSummaryIterable(); }

	string shortSummaryStatic() const { return staticOptions.shortSummary(); }

	// Summarize only the iterable part
	string shortSummaryIterable() const {
		string ret;
		auto addDesc = [&ret](const auto& param) -> void {
			if (!ret.empty()) {
				ret.push_back(' ');
			}
			ret += param.shortSummary();
		};
		addDesc(forceMultiVersion);
		addDesc(trustedClient);
		addDesc(grvCache);
		return ret;
	}
};

struct TestCase;

class TestEnv : public TestOptions {
	std::vector<std::pair<fdb::ByteString, int64_t>> tenants; ///< Randomly pre-generated tenant names and IDs
	fdb::Database adminDb;
	fdb::Database adminDatabase();

	// Incremented at the end of each runTest(). Also used to identify client trace file [combify_client_x].
	unsigned runCount;
	const TestCase* currentTestCase;
	void populateTenants();
	void cleanupDatabase();
	void assertNonZeroTenantCount() const;

	// Sets process-wide global FDB network options based on test options.
	void setNetworkOptionsForAdmin();

	// Sets process-wide global FDB network options based on test options.
	void setNetworkOptions();

	// Sets all configured options on a transaction object to be used by test code.
	void setTransactionOptions(fdb::Transaction tr) const;

public:
	TestEnv(const rapidjson::Document& conf);

	// To be called by main()
	bool runTest(const TestCase&);

	// const test helper functions for use by test functions

	// Create a database object from static option clusterFilePath
	// TODO: Apply any database-level options to the object, once we have iterable database options.
	fdb::Database newDatabase() const;

	// Returns a new normal (i.e. non-tenant) transaction pre-configured with applicable test options.
	// Will throw if the database is not configured for normal transactions.
	fdb::Transaction newTransaction(fdb::Database db) const;

	// Returns random tenant amongst pre-allocated tenants.
	// Will throw if tenantCount option is not set or set to zero.
	const std::pair<fdb::ByteString, int64_t>& anyTenant() const;

	unsigned numTenants() const;

	const std::pair<fdb::ByteString, int64_t>& tenantByIndex(unsigned index) const;

	// Returns a new tenant transaction pre-configured with applicable test options.
	// If authorization is enabled, tester must set the token using getTokenForTenant().
	// Will throw if the database is not configured for multi-tenancy.
	fdb::Transaction newTenantTransaction(fdb::Database db, fdb::BytesRef tenantName) const;

	// Returns authorization token signed for this tenant.
	// Will throw if authorization is not enabled in test options or tenantName is not among the elements of
	// tenantNames().
	string tokenForTenant(fdb::BytesRef tenantName) const;

	// Returns a DB transaction for trustedClient and a tenant transaction for untrusted client.
	// For simplicity, when tenant transaction is used, the first tenant is always used, allowing
	// the caller to use this function to read its own writes. For advanced use case when testing
	// multi-tenant case, consider using tenantByIndex and newTenantTransaction methods.
	fdb::Transaction newNormalOrTenantTransaction() const;
};

template <class PredicateFunc>
concept Predicate = requires(PredicateFunc&& fn) {
	{ fn(std::declval<const TestOptions&>()) } -> std::same_as<bool>;
};

// Returns whether server/client options under which the testcase is expected to run
using AffinityPredicateType = std::function<bool(const TestOptions&)>;

static_assert(Predicate<AffinityPredicateType>, "AffinityPredicateType must satisfy Predicate concept");

constexpr auto AND = [](Predicate auto&&... predicates) {
	return [... predicates = std::forward<decltype(predicates)>(predicates)](const TestOptions& option) -> bool {
		return (... && predicates(option));
	};
};

constexpr auto OR = [](Predicate auto&&... predicates) {
	return [... predicates = std::forward<decltype(predicates)>(predicates)](const TestOptions& option) -> bool {
		return (... || predicates(option));
	};
};

constexpr auto NOT = [](Predicate auto&& p) {
	return [p = std::forward<decltype(p)>(p)](const TestOptions& option) -> bool { return !p(option); };
};

// When you want the test to run for all possible configurations.
constexpr auto ALL = [](const TestOptions&) -> bool { return true; };

constexpr auto withTls = [](const TestOptions& o) -> bool { return o.staticOptions.tlsConfig; };

constexpr auto withoutTls = [](const TestOptions& o) -> bool { return !o.staticOptions.tlsConfig; };

constexpr auto withAuthZ = [](const TestOptions& o) -> bool { return o.staticOptions.authzConfig; };

constexpr auto withoutAuthZ = [](const TestOptions& o) -> bool { return !o.staticOptions.authzConfig; };

constexpr auto withMultiVersion = [](const TestOptions& o) -> bool { return *o.forceMultiVersion; };

constexpr auto withoutMultiVersion = [](const TestOptions& o) -> bool { return !*o.forceMultiVersion; };

constexpr auto withGrvCache = [](const TestOptions& o) -> bool { return *o.grvCache; };

constexpr auto withoutGrvCache = [](const TestOptions& o) -> bool { return !*o.grvCache; };

constexpr auto withTenantsMoreThan(unsigned count) {
	return [count](const TestOptions& o) -> bool {
		return o.staticOptions.tenantCount && *o.staticOptions.tenantCount > count;
	};
}

constexpr auto withTenants = [](const TestOptions& o) -> bool {
	return o.staticOptions.tenantCount && *o.staticOptions.tenantCount > 0;
};

constexpr auto tenantDisabled = [](const TestOptions& o) -> bool {
	return *o.staticOptions.tenantMode == "disabled" && !o.staticOptions.tenantCount;
};

constexpr auto withoutTenants = [](const TestOptions& o) -> bool {
	return !o.staticOptions.tenantCount || *o.staticOptions.tenantCount == 0;
};

constexpr auto asTrustedClient = [](const TestOptions& o) -> bool { return *o.trustedClient; };

constexpr auto asUntrustedClient = [](const TestOptions& o) -> bool { return !*o.trustedClient; };

template <class TestFn>
concept TestFunction = requires(TestFn&& testFn) {
	{ testFn(std::declval<const TestEnv&>()) } -> std::same_as<void>;
};

using TestFunctionType = std::function<void(const TestEnv&)>;

static_assert(TestFunction<TestFunctionType>, "TestFunctionType must satisfy TestFunction concept");

struct TestCase {
	string_view name;
	AffinityPredicateType predicate;
	TestFunctionType testFunction;
};

// Helper class to add tests directly from source files
struct AddTest {
	template <Predicate P, TestFunction TestFn>
	AddTest(string_view name, P&& p, TestFn&& fn) {
		doAddTest(TestCase{ name, std::forward<P>(p), std::forward<TestFn>(fn) });
	}

	void doAddTest(TestCase&& tc);
};

} // namespace combify

#endif /*COMBINATORIAL_HPP*/
