/*
 * Combify.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include <unistd.h>
#include <sys/wait.h>

#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <thread>
#include <type_traits>
#include <utility>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/optional.hpp>
#include <boost/process.hpp>
#include <boost/serialization/optional.hpp>
#include <boost/serialization/string.hpp>

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <fmt/format.h>

#include "Combify.hpp"
#include "TestHelpers.hpp"
#include "fdbrpc/TokenSignStdTypes.h"

namespace {
std::optional<combify::TestEnv> env;
}

namespace combify {

// Network or database option can have a transaction implication:
// e.g. trustedClient option affects whether AuthZ token transaction option should be set.
// Clang-format is disabled for the concept definition below as it is currently lacking
// concepts syntax support: https://stackoverflow.com/questions/62219221/using-clang-format-with-c20-concepts
// clang-format off
template <class Opt>
concept HasTransactionImplication = requires(const Opt& o) {
	{ o.apply(std::declval<fdb::Transaction>()) } -> std::same_as<void>;
};
// clang-format on

using namespace fdb::literals;

std::list<TestCase> testCases;

void AddTest::doAddTest(TestCase&& tc) {
	testCases.push_back(std::move(tc));
}

auto nextIterationImpl = [](auto&... opts) -> optional<IterableOptionTier> {
	// shift set of iterable options into the next iteration
	// e.g. if current option of a boolean set is [1, 0, 1], the next iterable set is [1, 1, 0].
	// To achieve this, we first seek right-to-left for the first element for whom hasNext() == true
	// (which would be the "0" in the above example, as there are no iterations left on the options on the either side)
	// and then reset all options to its right (flip the rightmost "1" in the above example into 0)
	constexpr const auto len = (int)sizeof...(opts);

	bool hasNext[] = { opts.hasNext()... };
	int pivotPos = -1;
	for (auto i = len - 1; i >= 0; i--) {
		if (hasNext[i]) {
			if (pivotPos == -1) {
				pivotPos = i;
				break;
			}
		}
	}
	if (pivotPos == -1)
		return {};
	auto ret = optional<IterableOptionTier>();
	// now shift the pivot option to next iteration, and reset all options to its right
	int index = 0;
	(
	    [&](auto&& opt) {
		    if (index < pivotPos) {
			    // do nothing
		    } else if (index == pivotPos) {
			    ASSERT(opt.hasNext());
			    ++opt; // Shift pivotal parameter to next value.
			    // Mark the tier of the shifted option so we know on which level we should refresh the test environment.
			    ret = std::decay_t<decltype(opt)>::tier;
		    } else {
			    opt.reset();
		    }
		    index++;
	    }(opts),
	    ...);
	return ret;
};

bool TestOptions::allOptionsValid() const {
	// Check iterable options are not violating constraints. For instance, GRV caching is only
	// supported when multi-version client is used
	if (withoutMultiVersion(*this) && withGrvCache(*this)) {
		return false;
	}

	size_t count = 0;
	std::apply(
	    [this, &count](const auto&... opts) {
		    (
		        [this, &count](const auto& opts) {
			        if (!opts) {
				        // Option value is not valid (end of range), which is unlikely during normal iteration.
				        // Skip this iteration by not incrementing count.
			        } else if (!opts.validForStaticConfig(staticOptions)) {
				        // Option value is in valid range, but is not applicable to this particular static config,
				        // which is expected to happen e.g. if a test case did not specify trustedClient option,
				        // tester automatically explores both trusted and untrusted client mode,
				        // but if the static configuration does not have authorization enabled,
				        // that option value is unfit to be tested.
				        // This case can be removed if we also move the temporary cluster provisioning into the C++
				        // combify code. Skip this iteration by not incrementing count.
			        } else {
				        count++;
			        }
		        }(opts),
		        ...);
	    },
	    getIterableOptions());
	return count == std::tuple_size<decltype(getIterableOptions())>::value;
}

optional<IterableOptionTier> TestOptions::next() {
	return std::apply(nextIterationImpl, getIterableOptions());
}

TestEnv::TestEnv(const rapidjson::Document& conf) : currentTestCase(nullptr) {
	staticOptions.parseFrom(conf);
}

void TestEnv::setNetworkOptions() {
	if (!allOptionsValid()) {
		throw std::runtime_error(fmt::format("NOT ALL OPTIONS ARE VALID FOR A RUN: {}", shortSummary()));
	}
	fdb::selectApiVersion(fdb::maxApiVersion());
	if (!staticOptions.disableClientTrace || !*staticOptions.disableClientTrace) {
		fdb::network::setOption(FDB_NET_OPTION_TRACE_ENABLE, "./");
		fdb::network::setOption(FDB_NET_OPTION_TRACE_FILE_IDENTIFIER,
		                        fdb::toBytesRef(fmt::format("combify_client_{}", runCount)));
	}
	std::apply(
	    [this](auto&... opts) {
		    (
		        [this](auto& o) {
			        constexpr auto tier = std::decay_t<decltype(o)>::tier;
			        if constexpr (tier == IterableOptionTier::Network) {
				        o.apply(staticOptions);
			        }
		        }(opts),
		        ...);
	    },
	    getIterableOptions());
}

void TestEnv::setNetworkOptionsForAdmin() {
	if (!allOptionsValid()) {
		throw std::runtime_error(fmt::format("NOT ALL OPTIONS ARE VALID FOR A RUN: {}", shortSummary()));
	}
	fdb::selectApiVersion(fdb::maxApiVersion());
	// Do the same as setNetworkOptions() but ignore TrustedClientOption and, instead, separately apply mTLS option.
	std::apply(
	    [this](const auto&... opts) {
		    (
		        [this](const auto& o) {
			        using O = std::decay_t<decltype(o)>;
			        constexpr auto tier = O::tier;
			        constexpr auto name = O::name;
			        if constexpr (tier == IterableOptionTier::Network && name != TrustedClientOption::name) {
				        o.apply(staticOptions);
			        }
		        }(opts),
		        ...);
	    },
	    getIterableOptions());
	trustedClient.applyForAdmin(staticOptions);
}

fdb::Database TestEnv::adminDatabase() {
	if (!adminDb) {
		adminDb = fdb::Database(*staticOptions.clusterFilePath);
	}
	return adminDb;
}

fdb::Database TestEnv::newDatabase() const {
	return fdb::Database(*staticOptions.clusterFilePath);
	// TODO: Apply all applicable database options. Currently there aren't any.
}

void TestEnv::setTransactionOptions(fdb::Transaction tr) const {
	std::apply(
	    [&tr](auto&&... opts) {
		    (
		        [&tr](auto const& opt) {
			        using O = std::decay_t<decltype(opt)>;
			        if constexpr (HasTransactionImplication<O>) {
				        opt.apply(tr);
			        }
		        }(opts),
		        ...);
	    },
	    getIterableOptions());
}

fdb::Transaction TestEnv::newTenantTransaction(fdb::Database db, fdb::BytesRef tenantName) const {
	auto tenant = db.openTenant(tenantName);
	auto tr = tenant.createTransaction();
	setTransactionOptions(tr);

	// AuthZ token does not fit the typical iterable option abstraction in that
	// it requires static options, tenant name, and server-assigned tenant ID to build the option value itself.
	// Begrudgingly do a special handling.
	if (staticOptions.authzConfig && !*trustedClient) {
		bool knownTenantFound = false;
		for (const auto& [knownTenantName, tenantId] : tenants) {
			if (knownTenantName == tenantName) {
				auto token = staticOptions.authzConfig->signToken({ tenantId });
				tr.setOption(FDB_TR_OPTION_AUTHORIZATION_TOKEN, token);
				knownTenantFound = true;
				break;
			}
		}
		if (!knownTenantFound) {
			throw std::runtime_error(
			    "AUTHZ-ENABLED, UNTRUSTED_CLIENT newTenantTransaction() WITH AD-HOC TENANT IS NOT SUPPORTED YET");
		}
	}
	return tr;
}

fdb::Transaction TestEnv::newTransaction(fdb::Database db) const {
	auto tr = db.createTransaction();
	setTransactionOptions(tr);
	return tr;
}

void TestEnv::assertNonZeroTenantCount() const {
	if (!staticOptions.tenantCount) {
		throw std::runtime_error(fmt::format("ATTEMPTED TENANT ACCESS IN TENANTLESS CONFIG: {}", shortSummary()));
	}
	ASSERT(*staticOptions.tenantCount == tenants.size()); // By now tenant names and IDs should have been filled out.
	if (tenants.size() == 0) {
		throw std::runtime_error(fmt::format("ATTEMPTED TENANT ACCESS IN ZERO-TENANT CONFIG: {}", shortSummary()));
	}
}

const std::pair<fdb::ByteString, int64_t>& TestEnv::anyTenant() const {
	assertNonZeroTenantCount();
	return tenants[std::rand() % tenants.size()];
}

unsigned TestEnv::numTenants() const {
	if (staticOptions.tenantCount) {
		ASSERT(*staticOptions.tenantCount == tenants.size());
	}
	return staticOptions.tenantCount ? tenants.size() : 0;
}

const std::pair<fdb::ByteString, int64_t>& TestEnv::tenantByIndex(unsigned index) const {
	assertNonZeroTenantCount();
	if (index >= tenants.size()) {
		throw std::out_of_range(fmt::format("TENANT INDEX OUT OF RANGE: {} of [0,{})", index, tenants.size()));
	}
	return tenants[index];
}

string AuthZConfig::signToken(std::vector<int64_t> tenantIds) const {
	auto t = authz::jwt::stdtypes::TokenSpec{};
	auto const timeSinceEpoch = std::chrono::system_clock::now().time_since_epoch();
	auto const now = std::chrono::duration_cast<std::chrono::duration<uint64_t>>(timeSinceEpoch).count();
	t.algorithm = authz::Algorithm::ES256;
	t.keyId = *keyName;
	t.issuer = "combify";
	t.subject = "client-test";
	t.audience = std::vector<string>{ "aud" };
	t.issuedAtUnixTime = now;
	t.expiresAtUnixTime = now + 60 * 60 * 12; // Valid for 12 hrs
	t.notBeforeUnixTime = now - 60 * 5; // Activated 5 mins ago
	t.tokenId = randomAlnumString<string>(10, 20);
	t.tenants = std::move(tenantIds);
	return authz::jwt::stdtypes::signToken(t, *privateKeyPem);
}

std::string drainPipeFd(int pipeFd) {
	int readRc = 0;
	std::string ret;
	char buf[PIPE_BUF];
	while ((readRc = ::read(pipeFd, buf, PIPE_BUF)) > 0) {
		ret.append(buf, readRc);
	}
	if (readRc != 0) {
		throw std::runtime_error(fmt::format("PIPE READ ERROR: {}", strerror(errno)));
	}
	return ret;
}

// Run function fn in a child process. Capture .what() string of any standard exception it ran into, and also its stdout
// and stderr.
template <class Fn>
std::tuple<optional<string>, string, string> runForked(Fn&& fn) {
	// Catch any child exception and return the exception.what() to parent
	int childStdout[2];
	int childStderr[2];
	int childExc[2];
	if (::pipe(childStdout) || ::pipe(childStderr) || ::pipe(childExc)) {
		fmt::print(stderr, "FAILED TO OPEN CHILD STDOUT/STDERR/EXCEPTION PIPES: {}", strerror(errno));
		throw std::runtime_error(
		    fmt::format("FAILED TO OPEN CHILD STDOUT/STDERR/EXCEPTION PIPES: {}", strerror(errno)));
	}
	// Flush out stdout and stderr such that their buffers don't get duplicated to child process and print twice upon
	// exit
	fflush(stdout);
	fflush(stderr);
	auto pid = fork();
	if (pid < 0) {
		throw std::runtime_error(fmt::format("FAILED TO FORK PROCESS: {}", strerror(errno)));
	} else if (pid == 0) {
		// subprocess: call passed function
		::close(childStdout[0]);
		::close(childStderr[0]);
		::close(childExc[0]);
		auto pipeCleanup = ScopeExit([&]() {
			::close(childStdout[1]);
			::close(childStderr[1]);
			::close(childExc[1]);
		});
		auto errorMsg = optional<string>();

		if (::dup2(childStdout[1], STDOUT_FILENO) == -1 || ::dup2(childStderr[1], STDERR_FILENO) == -1) {
			errorMsg = fmt::format("FAILED TO REDIRECT CHILD STDOUT/STDERR TO PARENT PIPES: {}", strerror(errno));
		} else {
			try {
				fn();
			} catch (const std::exception& e) {
				errorMsg = string(e.what());
			} catch (fdb::Error e) {
				errorMsg = string(e.what());
			}
		}
		if (errorMsg) {
			::write(childExc[1], errorMsg->data(), errorMsg->size());
		} else {
			::close(childExc[1]);
		}
		std::exit(0);
	} else {
		::close(childStdout[1]);
		::close(childStderr[1]);
		::close(childExc[1]);
		auto pipeCleanup = ScopeExit([&]() {
			::close(childStdout[0]);
			::close(childStderr[0]);
			::close(childExc[0]);
		});

		int status = 0;
		auto wpid = ::waitpid(pid, &status, 0);
		if (wpid < 0) {
			throw std::runtime_error(fmt::format("SUBPROCESS WAITPID FAILED WITH {}", strerror(errno)));
		}
		auto errorMsg = optional<string>();
		if (status != 0) {
			errorMsg = fmt::format(
			    "CHILD PROCESS RETURNED ABNORMALLY WITH STATUS: EXITED({}) EXITSTATUS({}) SIGNALED({}) SIGNAL({})",
			    WIFEXITED(status),
			    WEXITSTATUS(status),
			    WIFSIGNALED(status),
			    WTERMSIG(status));
		}
		// parent process: read back optional<string> whose value is set if something went wrong.
		auto out = drainPipeFd(childStdout[0]);
		auto err = drainPipeFd(childStderr[0]);
		auto exc = drainPipeFd(childExc[0]);
		if (!exc.empty()) {
			if (!errorMsg) {
				errorMsg.emplace();
			} else {
				errorMsg->append("\nadditional error message: ");
			}
			errorMsg->append(exc);
		}
		return { errorMsg, out, err };
	}
}

void TestEnv::populateTenants() {
	auto const tenantCount = *env->staticOptions.tenantCount;
	tenants.resize(tenantCount);
	for (auto i = 0; i < tenantCount; i++)
		tenants[i].first = randomAlnumString<fdb::ByteString>(10, 24);
	boost::process::pstream tenantIdPipe; ///< Child writes, parent reads.
	auto [exc, out, err] = runForked([this, &tenantIdPipe]() {
		setNetworkOptionsForAdmin();
		auto networkThread = setupAndStartNetworkGuarded();
		auto db = adminDatabase();
		auto tr = db.createTransaction();
		runToCompletion(
		    tr,
		    [this](fdb::Transaction tr) {
			    for (const auto& [tenantName, _] : tenants) {
				    fdb::Tenant::createTenant(tr, tenantName);
			    }
			    return tr.commit();
		    },
		    "PopulateTenants");
		for (auto& [tenantName, tenantId] : tenants) {
			while (true) {
				auto tenant = db.openTenant(tenantName);
				auto tenantIdFuture = tenant.getId();
				if (auto err = tenantIdFuture.blockUntilReady()) {
					throw std::runtime_error(fmt::format("ERROR WHILE BLOCKING FOR TENANT ID GET: {}", err.what()));
				}
				if (auto err = tenantIdFuture.error()) {
					if (err.retryable()) {
						std::this_thread::sleep_for(std::chrono::milliseconds(50));
						continue;
					}
					throw std::runtime_error(fmt::format("UNRETRYABLE ERROR DURING TENANT ID GET: {}", err.what()));
				} else {
					tenantId = tenantIdFuture.get();
					break;
				}
			}
		}
		// Write tenant IDs back to parent process through pipe.
		boost::archive::binary_oarchive oa(tenantIdPipe);
		oa << tenants.size();
		for (const auto& [_, tenantId] : tenants)
			oa << tenantId;
	});
	if (exc) {
		throw std::runtime_error(*exc);
	}
	// Read tenant ID from pipe
	boost::archive::binary_iarchive ia(tenantIdPipe);
	size_t len = 0;
	ia >> len;
	ASSERT(len == tenants.size());
	// Fill out parent tenant IDs
	for (auto& [_, tenantId] : tenants)
		ia >> tenantId;
}

void TestEnv::cleanupDatabase() {
	auto [exc, out, err] = runForked([this]() {
		setNetworkOptionsForAdmin();
		auto networkThread = setupAndStartNetworkGuarded();
		auto db = adminDatabase();
		auto tr = db.createTransaction();
		runToCompletion(
		    tr,
		    [](fdb::Transaction tr) {
			    tr.clearRange(""_br, "\xff"_br);
			    return tr.commit();
		    },
		    "CleanupKeyspace");
		tr.reset();
		if (tenants.size() > 0) {
			runToCompletion(
			    tr,
			    [this](fdb::Transaction tr) {
				    // TODO: List and delete ad-hoc tenants, too
				    for (const auto& [tenantName, _] : tenants) {
					    fdb::Tenant::deleteTenant(tr, tenantName);
				    }
				    return tr.commit();
			    },
			    "DeleteTenants");
		}
	});
	if (exc) {
		throw std::runtime_error(*exc);
	}
}

bool TestEnv::runTest(const TestCase& t) {
	// fork and run test
	// TODO: Run all database/transaction option iterations within the same child process to save time.
	auto const& tenantCountOption = staticOptions.tenantCount;
	currentTestCase = &t;
	auto currentTestResetGuard = ScopeExit([this]() { currentTestCase = nullptr; });
	if (tenantCountOption && *tenantCountOption > 0) {
		populateTenants();
	}
	auto [exc, out, err] = runForked([this, &t]() {
		setNetworkOptions();
		auto networkThread = setupAndStartNetworkGuarded();
		t.testFunction(*this);
	});

	auto dbCleanupGuard = ScopeExit([this]() { cleanupDatabase(); });
	if (exc) {
		fmt::print("  TEST FAILED WITH EXCEPTION\n"
		           "	{}\n",
		           *exc);
	}

	if (exc) {
		if (!out.empty()) {
			fmt::print("=== BEGIN TEST STDOUT ===\n{}===  END  TEST STDOUT ===\n", out);
		}
		if (!err.empty()) {
			fmt::print("=== BEGIN TEST STDERR ===\n{}===  END  TEST STDERR ===\n", err);
		}
	}
	runCount++;
	return !exc.has_value();
}

fdb::Transaction TestEnv::newNormalOrTenantTransaction(fdb::Database db) const {
	// For trusted client or if there is no tenant, return a new DB transaction
	if (*trustedClient || tenants.empty()) {
		return newTransaction(db);
	}

	// Otherwise, create a transaction from the first tenant
	return newTenantTransaction(db, tenants[0].first);
}

std::vector<const TestCase*> collectTestCases(const TestOptions& options) {
	std::vector<const TestCase*> ret;
	for (const auto& testCase : testCases) {
		if (testCase.predicate(options)) {
			ret.push_back(&testCase);
		}
	}
	return ret;
}

} // namespace combify

template <class Duration>
int diffMillis(Duration tdiff) {
	return std::chrono::duration_cast<std::chrono::milliseconds>(tdiff).count();
}

int main(int argc, char** argv) {
	if (argc != 1 && argc != 2) {
		fmt::print("usage: {} [CONFIG_JSON_STRING] (or feed JSON config through stdin)\n", argv[0]);
		return 1;
	}
	fmt::print("Starting in {} mode\n", argc == 1 ? "STDIN" : "CLI_ARG");
	std::string jsonConfig;
	if (argc == 1) {
		// take whole stdin as config JSON
		std::stringstream buf;
		buf << std::cin.rdbuf();
		jsonConfig = buf.str();
	} else {
		// argc == 2
		// take second arg as JSON string and read whole
		jsonConfig = argv[1];
	}
	rapidjson::Document d;
	d.Parse(jsonConfig.data(), jsonConfig.size());
	if (d.HasParseError()) {
		fmt::print("CONFIG JSON PARSE ERROR AT OFFSET {}: {}\n",
		           d.GetErrorOffset(),
		           rapidjson::GetParseError_En(d.GetParseError()));
		return 2;
	}
	env.emplace(d);
	auto randomSeed = std::time(nullptr);
	if (env->staticOptions.randomSeed) {
		randomSeed = *env->staticOptions.randomSeed;
		fmt::print("Random seed set by option to {}\n", randomSeed);
	} else {
		fmt::print("Random seed set by current time to {}\n", randomSeed);
	}
	fmt::print("{} test cases found{}\n", combify::testCases.size(), combify::testCases.empty() ? "" : ":");
	for (const auto& [name, _, __] : combify::testCases) {
		fmt::print("  {}\n", name);
	}

	std::srand(randomSeed);
	combify::optional<combify::IterableOptionTier> shiftedTier;
	std::vector<std::pair<const combify::TestCase*, combify::TestOptions>> failedCases;
	do {
		// Skip if a combination is unfit
		if (!env->allOptionsValid()) {
			fmt::print("\n{} Combination unfit, skipping \n", env->shortSummary());
			continue;
		}

		auto cases = combify::collectTestCases(*env);
		fmt::print("\n{} [{} cases]\n", env->shortSummaryIterable(), cases.size());
		if (cases.empty()) {
			continue;
		}
		for (auto testCasePtr : cases) {
			auto const timeBegin = std::chrono::steady_clock::now();
			auto timeEnd = timeBegin;
			bool ok = true;
			try {
				ok = env->runTest(*testCasePtr);
				timeEnd = std::chrono::steady_clock::now();
			} catch (const std::exception& e) {
				timeEnd = std::chrono::steady_clock::now();
				fmt::print("!!! EXCEPTION DURING TEST: {}\n", e.what());
				ok = false;
			}
			fmt::print("  {} {} ({} ms)\n", (ok ? "O" : "X"), testCasePtr->name, diffMillis(timeEnd - timeBegin));
			if (!ok) {
				failedCases.emplace_back(testCasePtr, *env);
			}
		}
	} while ((shiftedTier = env->next()));

	fmt::print("Test {}\n", failedCases.empty() ? "OK" : "FAILED");
	if (!failedCases.empty()) {
		fmt::print("Failed tests and configurations:\n");
		for (const auto& [casePtr, options] : failedCases) {
			fmt::print("  {} {}\n", casePtr->name, options.shortSummaryIterable());
		}
		return 1;
	}
	return 0;
}
