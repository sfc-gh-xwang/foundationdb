#pragma once
#ifndef COMBINATORIAL_HPP
#define COMBINATORIAL_HPP

#ifndef FDB_API_VERSION
#define FDB_USE_LATEST_API_VERSION
#endif

#include <fdb_api.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <concepts>
#include <list>
#include <string>
#include <string_view>
#include <optional>
#include <type_traits>
#include <utility>
#include <thread>
#include <fstream>
#include <sstream>
#include <stdexcept>

#include <fmt/format.h>
#include <rapidjson/document.h>

namespace cbt {

using std::optional;
using std::string;
using std::string_view;

template <class E>
concept Enum = std::is_enum_v<E>;

template <Enum E>
std::underlying_type_t<E> enumToUnderlying(E e) {
	return static_cast<std::underlying_type_t<E>>(e);
}

// Execute lambda as this object goes out of scope.
template <typename Func>
class ScopeExit {
	std::decay_t<Func> fn;

public:
	ScopeExit(Func&& fn) : fn(std::forward<Func>(fn)) {}

	~ScopeExit() { fn(); }
};

inline string camelToSnakeAllCaps(string_view s) {
	string ret;
	ret.reserve(s.size() * 2);
	for (auto i = 0u; i < s.size(); i++) {
		if (std::isupper(s[i]) && i > 0)
			ret.push_back('_');
		ret.push_back(std::toupper(s[i]));
	}
	return ret;
}

template <class StringLike>
StringLike randomAlnumString(int lengthMin, int lengthMax) {
	static_assert(sizeof(typename StringLike::value_type) == 1);
	assert(lengthMin > 0 && lengthMin <= lengthMax);
	constexpr string_view dict = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	auto const ldiff = lengthMax - lengthMin;
	auto const len = lengthMin + (std::rand() % (ldiff + 1));
	StringLike ret;
	ret.reserve(len);
	for (auto i = 0; i < len; i++)
		ret.push_back(static_cast<typename StringLike::value_type>(dict[std::rand() % static_cast<int>(dict.size())]));
	return ret;
}

template <size_t N>
struct StringLiteral {
	constexpr StringLiteral(const char (&str)[N]) { std::copy_n(str, N, value); }
	char value[N];

	constexpr string_view toView() const noexcept { return string_view(value, N - 1); }

	constexpr optional<string_view> toOptional() const noexcept {
		if constexpr (N <= 1)
			return {};
		return string_view(value, N - 1);
	}
};

constexpr inline string_view jsonTypeString(rapidjson::Type type) {
	constexpr const string_view typeStringMap[] = { "null", "false", "true", "object", "array", "string", "number" };
	return typeStringMap[static_cast<int>(type)];
}

[[noreturn]] inline void throwUnmatchedFieldType(string_view fieldName,
                                                 string_view typeExpected,
                                                 rapidjson::Type typeActual) {
	throw std::runtime_error(fmt::format("field type mismatch: name={} type_actual={} type_expected={}",
	                                     fieldName,
	                                     jsonTypeString(typeActual),
	                                     typeExpected));
}

[[noreturn]] inline void throwNoSuchField(string_view fieldName) {
	throw std::runtime_error(fmt::format("no such field: {}", fieldName));
}

[[noreturn]] inline void throwNotAnObject(const rapidjson::Value& v, string_view fieldName) {
	throw std::runtime_error(fmt::format("JSON value from which to parse field '{}' is not an object; actual type={}",
	                                     fieldName,
	                                     jsonTypeString(v.GetType())));
}

struct MustExist {
	static constexpr bool value = true;
};

struct Optional {
	static constexpr bool value = false;
};

template <class T, StringLiteral NAME, class MUST_EXIST>
struct JsonOptionCommonTraits {
	static constexpr const string_view name = NAME.value;
	static constexpr const bool fieldMustExist = MUST_EXIST::value;

	optional<T> value;

	const T& operator*() const { return *value; }

	const T* operator->() const { return &(*value); }

	operator bool() const noexcept { return value.has_value(); }
};

template <class T, StringLiteral NAME, class MUST_EXIST>
struct JsonOption : JsonOptionCommonTraits<T, NAME, MUST_EXIST> {
	using ParentType = JsonOptionCommonTraits<T, NAME, MUST_EXIST>;
	// Base members are not visible to derived class by default if base is from a class template.
	using ParentType::name;
	using ParentType::value;

	// Try to read an attribute [name] from the object [v] and use its content to build T
	void parseFrom(const rapidjson::Value& v) {
		if (!v.IsObject())
			throwNotAnObject(v, name);
		auto memberItr = v.FindMember(NAME.value);
		if (memberItr == v.MemberEnd()) {
			if constexpr (MUST_EXIST::value)
				throwNoSuchField(name);
			else {
				value.reset();
				return;
			}
		}
		const auto& f = memberItr->value;
		if constexpr (std::is_same_v<T, bool>) {
			if (!f.isBool())
				throwUnmatchedFieldType(name, "bool", f.GetType());
			value = f.GetBool();
		} else if constexpr (std::is_arithmetic_v<T>) {
			if (!f.IsNumber())
				throwUnmatchedFieldType(name, "number", f.GetType());
			double const number = f.GetDouble();
			if (std::is_unsigned_v<T> && number < 0) {
				throw std::runtime_error(
				    fmt::format("ATTEMPTED TO ASSIGN NEGATIVE NUMBER TO UNSIGNED OPTION {}. VALUE:{}", name, number));
			}
			// no precision loss for integers within [-2^53, 2^53]
			value = static_cast<T>(number);
		} else if constexpr (std::is_same_v<T, string>) {
			if (!f.IsString())
				throwUnmatchedFieldType(name, "string", f.GetType());
			value = string(f.GetString(), f.GetStringLength());
		} else {
			throw std::runtime_error(fmt::format(
			    "DEFAULT PARSE LOGIC FOR TYPE {} IS NOT SUPPORTED; IMPLEMENT A SPECIALIZATION", typeid(T).name()));
		}
	}

	// short summary to be prefixed at the beginning of every test
	string shortSummary() const {
		return camelToSnakeAllCaps(name) + ":" + [this]() -> string {
			if (!value)
				return "X"; // unset
			if constexpr (std::is_same_v<T, bool>) {
				if (*value)
					return "1";
				else
					return "0";
			} else if constexpr (std::is_arithmetic_v<T>) {
				auto s = std::to_string(*value);
				if (s.size() > 3)
					return "SET";
				else
					return s;
			} else if constexpr (std::is_same_v<T, string>) {
				if (value->size() > 3)
					return "SET";
				else
					return *value;
			} else {
				throw std::runtime_error(fmt::format(
				    "default short description logic for type {} is not supported; implement a specialization",
				    typeid(T).name()));
			}
		}();
	}

	string longSummary() const { return fmt::format("{}:{}", camelToSnakeAllCaps(name), *value); }
};

template <StringLiteral... CHOICES>
struct StringChoice : string {
	static constexpr std::array<string_view, sizeof...(CHOICES)> choices{ CHOICES.toView()... };

	// -1 if not found
	static int indexOf(string param) {
		auto itr = std::find(choices.begin(), choices.end(), param);
		if (itr == choices.end())
			return -1;
		return itr - choices.begin();
	}
};

// specialization for parsing StringChoice
template <StringLiteral NAME, class MUST_EXIST, StringLiteral... CHOICES>
struct JsonOption<StringChoice<CHOICES...>, NAME, MUST_EXIST> : JsonOptionCommonTraits<string, NAME, MUST_EXIST> {
	using ParentType = JsonOptionCommonTraits<string, NAME, MUST_EXIST>;
	using ParentType::name;
	using ParentType::value;

	int index = -1;

	void parseFrom(const rapidjson::Value& v) {
		if (!v.IsObject())
			throwNotAnObject(v, name);
		auto memberItr = v.FindMember(NAME.value);
		if (memberItr == v.MemberEnd()) {
			if constexpr (MUST_EXIST::value)
				throwNoSuchField(name);
			else
				return;
		}
		const auto& f = memberItr->value;
		if (!f.IsString())
			throwUnmatchedFieldType(name, "string", f.GetType());
		string maybeValue(f.GetString(), f.GetStringLength());

		using ChoiceType = StringChoice<CHOICES...>;
		index = ChoiceType::indexOf(maybeValue);
		if (index == -1) {
			throw std::runtime_error(
			    fmt::format("INVALID VALUE '{}' FOR STRING CHOICE OPTION {}. VALID VALUES ARE: [{}]",
			                maybeValue,
			                name,
			                fmt::join(ChoiceType::choices, ", ")));
		} else {
			value = maybeValue;
		}
	}

	string shortSummary() const {
		assert(value);
		return camelToSnakeAllCaps(name) + ":" + std::to_string(index);
	}
};

struct TLSConfig {
	JsonOption<string, "caPath", MustExist>
	    caPath; // if TLS is enabled in any form, client trust anchor is always needed
	JsonOption<string, "certPath", MustExist> certPath;
	JsonOption<string, "keyPath", MustExist> keyPath;
};

// specialization for parsing TLSConfig
template <StringLiteral NAME, class MUST_EXIST>
struct JsonOption<TLSConfig, NAME, MUST_EXIST> : JsonOptionCommonTraits<TLSConfig, NAME, MUST_EXIST> {
	using ParentType = JsonOptionCommonTraits<TLSConfig, NAME, MUST_EXIST>;
	using ParentType::name;
	using ParentType::value;

	void parseFrom(const rapidjson::Value& v) {
		if (!v.IsObject())
			throwNotAnObject(v, name);
		auto memberItr = v.FindMember(NAME.value);
		if (memberItr == v.MemberEnd()) {
			if constexpr (MUST_EXIST::value)
				throwNoSuchField(name);
			else
				return;
		}
		auto& tlsConfigJson = memberItr->value;
		value.emplace(); // default construct TLSConfig
		auto& tlsConfig = *value;
		tlsConfig.caPath.parseFrom(tlsConfigJson);
		tlsConfig.certPath.parseFrom(tlsConfigJson);
		tlsConfig.keyPath.parseFrom(tlsConfigJson);
	}

	string shortSummary() const {
		return fmt::format("{}:{}", camelToSnakeAllCaps(name), [this]() -> string_view {
			if (!value) {
				return "OFF";
			} else if (value->certPath && value->keyPath) {
				return "MUTUAL";
			} else {
				return "UNTRUSTED";
			}
		}());
	}
};

struct AuthZConfig {
	JsonOption<string, "privateKeyPath", MustExist> privateKeyPath;
	JsonOption<string, "keyName", MustExist> keyName;
	// Loaded during parse
	optional<string> privateKeyPem;
	string signToken(const std::vector<int64_t> tenantIds) const;
};

// specialization for parsing AuthZConfig
template <StringLiteral NAME, class MUST_EXIST>
struct JsonOption<AuthZConfig, NAME, MUST_EXIST> : JsonOptionCommonTraits<AuthZConfig, NAME, MUST_EXIST> {
	using ParentType = JsonOptionCommonTraits<AuthZConfig, NAME, MUST_EXIST>;
	using ParentType::name;
	using ParentType::value;

	void parseFrom(const rapidjson::Value& v) {
		if (!v.IsObject())
			throwNotAnObject(v, name);
		auto memberItr = v.FindMember(NAME.value);
		if (memberItr == v.MemberEnd())
			throwNoSuchField(name);
		auto& authzConfigJson = memberItr->value;
		value.emplace(); // default construct AuthZConfig
		auto& config = *value;
		config.privateKeyPath.parseFrom(authzConfigJson);
		config.keyName.parseFrom(authzConfigJson);
		// This is the only case where any file read is needed. For now just read mid-parse.
		auto ifs = std::ifstream(*value->privateKeyPath);
		std::stringstream fileBuf;
		fileBuf << ifs.rdbuf();
		value->privateKeyPem = std::move(fileBuf).str();
	}

	string shortSummary() const {
		return camelToSnakeAllCaps(name) + ":" + [this]() {
			if (value->privateKeyPath && value->keyName) {
				return "ON";
			} else {
				return "OFF";
			}
		}();
	}
};

// fixed for the duration of this library: mostly cluster configurations
struct StaticOptions {
	JsonOption<string, "clusterFilePath", MustExist> clusterFilePath;
	JsonOption<TLSConfig, "tls", Optional> tlsConfig;
	JsonOption<AuthZConfig, "authz", Optional> authzConfig;
	JsonOption<StringChoice<"disabled", "required_experimental", "optional_experimental">, "tenantMode", MustExist>
	    tenantMode;
	JsonOption<unsigned, "tenantCount", Optional> tenantCount;
	JsonOption<unsigned, "randomSeed", Optional> randomSeed;

	void parseFrom(const rapidjson::Value& v) {
		clusterFilePath.parseFrom(v);
		tlsConfig.parseFrom(v);
		authzConfig.parseFrom(v);
		tenantMode.parseFrom(v);
		tenantCount.parseFrom(v);
		randomSeed.parseFrom(v);
	}

	string shortSummary() const {
		string ret;
		auto addDesc = [&ret](const auto& param) -> void {
			if (!ret.empty())
				ret.push_back(' ');
			ret += param.shortSummary();
		};
		addDesc(clusterFilePath);
		addDesc(tlsConfig);
		addDesc(authzConfig);
		addDesc(tenantMode);
		addDesc(tenantCount);
		// Keep randomSeed out for now. Printing at beginning or end of run instead would be more helpful.
		return ret;
	}
};

enum class IterableOptionTier : unsigned {
	Network,
	Database,
	Transaction,
};

template <StringLiteral NAME, IterableOptionTier TIER, std::integral T, T... CHOICES>
struct IntegralChoiceOption {
	static_assert(sizeof...(CHOICES) > 0);
	static constexpr const string_view name = NAME.value;
	static constexpr const IterableOptionTier tier = TIER;
	static constexpr const std::array<T, sizeof...(CHOICES)> choices = { CHOICES... };

	const T& value() const {
		assert(index < choices.size());
		return choices[index];
	}

	const T& operator*() const {
		assert(index < choices.size());
		return choices[index];
	}

	void reset() { index = 0; }

	operator bool() const noexcept { return index < choices.size(); }

	bool hasNext() const noexcept { return index + 1 < choices.size(); }

	void operator++() {
		if (index >= choices.size()) {
			throw std::out_of_range(fmt::format("attempted to iterate option {} out of legal range", name));
		}
		index++;
	}

	string shortSummary() const {
		static_assert(tier == IterableOptionTier::Network || tier == IterableOptionTier::Database ||
		              tier == IterableOptionTier::Transaction);
		// Prefix to indicate option tier: e.g. T:GRV_CACHE:1 means GRV_CACHE is a transaction level option and it is
		// set
		constexpr const char prefix[]{ "CDT" };
		fmt::memory_buffer buf;
		fmt::format_to(std::back_inserter(buf), "{}:{}:", prefix[enumToUnderlying(tier)], camelToSnakeAllCaps(name));
		if (index < choices.size()) {
			fmt::format_to(std::back_inserter(buf), "{:d}", choices[index]);
		} else {
			// end-of-range
			fmt::format_to(std::back_inserter(buf), "X");
		}
		return string(buf.data(), buf.size());
	}

	unsigned index = 0;
};

template <StringLiteral NAME, IterableOptionTier TIER>
using BooleanOption = IntegralChoiceOption<NAME, TIER, bool, false, true>;

struct ForceMultiVersionOption : BooleanOption<"forceMultiVersion", IterableOptionTier::Network> {
	bool validForStaticConfig(const StaticOptions&) const { return true; }

	void apply(const StaticOptions&) const { fdb::network::setOption(FDB_NET_OPTION_DISABLE_CLIENT_BYPASS); }
};

struct TrustedClientOption : BooleanOption<"trustedClient", IterableOptionTier::Network> {
	bool validForStaticConfig(const StaticOptions& o) const {
		if (!value()) {
			// Untrusted mode requires valid AuthZ config
			return o.tlsConfig && o.authzConfig;
		}
		// Trusted mode is applicable both with and without TLS
		return true;
	}

	void apply(const StaticOptions& o) const {
		if (o.tlsConfig) {
			auto const& tlsConfig = *o.tlsConfig;
			fdb::network::setOption(FDB_NET_OPTION_TLS_CA_PATH, *tlsConfig.caPath);
			if (value()) {
				// Only enable cert and key if in trusted mode (i.e. mTLS)
				fdb::network::setOption(FDB_NET_OPTION_TLS_CERT_PATH, *tlsConfig.certPath);

				fdb::network::setOption(FDB_NET_OPTION_TLS_KEY_PATH, *tlsConfig.keyPath);
			}
		}
	}

	void applyForAdmin(const StaticOptions& o) const {
		if (o.tlsConfig) {
			// Configure client TLS as trusted mode (i.e. mTLS)
			auto const& tlsConfig = *o.tlsConfig;
			fdb::network::setOption(FDB_NET_OPTION_TLS_CA_PATH, *tlsConfig.caPath);
			fdb::network::setOption(FDB_NET_OPTION_TLS_CERT_PATH, *tlsConfig.certPath);
			fdb::network::setOption(FDB_NET_OPTION_TLS_KEY_PATH, *tlsConfig.keyPath);
		}
	}
};

struct UseGrvCacheOption : BooleanOption<"grvCache", IterableOptionTier::Transaction> {
	bool validForStaticConfig(const StaticOptions&) const { return true; }

	void apply(fdb::Transaction tr) const { tr.setOption(FDB_TR_OPTION_USE_GRV_CACHE); }
};

template <class... Ts>
std::tuple<const Ts&...> constTie(const Ts&... ts) {
	return std::tuple<const Ts&...>(ts...);
}

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
		return constTie(forceMultiVersion, trustedClient, grvCache);
	}

	// Returns that all iterable options are in valid range AND it is a valid combination.
	bool allOptionsValid() const;

	string shortSummary() const { return shortSummaryStatic() + " " + shortSummaryIterable(); }

	string shortSummaryStatic() const { return staticOptions.shortSummary(); }

	// Summarize only the iterable part
	string shortSummaryIterable() const {
		string ret;
		auto addDesc = [&ret](const auto& param) -> void {
			if (!ret.empty())
				ret.push_back(' ');
			ret += param.shortSummary();
		};
		addDesc(forceMultiVersion);
		addDesc(trustedClient);
		addDesc(grvCache);
		return ret;
	}
};

fdb::ByteString randomAlnumString(int minLen, int maxLenInclusive);

struct TestCase;

class TestEnv : public TestOptions {
	std::vector<std::pair<fdb::ByteString, int64_t>> tenants; ///< Randomly pre-generated tenant names and IDs
	fdb::Database adminDb;
	fdb::Database adminDatabase();

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

	static const TestEnv& instance();

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

extern std::list<TestCase> testCases;

// Helper class to add tests directly from source files
struct AddTest {
	template <Predicate P, TestFunction TestFn>
	AddTest(string_view name, P&& p, TestFn&& fn) {
		testCases.push_back(TestCase{ name, std::forward<P>(p), std::forward<TestFn>(fn) });
	}
};

std::vector<const TestCase*> collectTestCases(const TestOptions& options);

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

// Network or database option can have a transaction implication:
// e.g. trustedClient option affects whether AuthZ token transaction option should be set.
template <class Opt>
concept HasTransactionImplication = requires(const Opt& o) {
	                                    { o.apply(std::declval<fdb::Transaction>()) } -> std::same_as<void>;
                                    };

} // namespace cbt

#endif /*COMBINATORIAL_HPP*/
