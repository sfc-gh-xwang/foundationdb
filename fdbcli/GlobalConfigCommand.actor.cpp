/**
 * GlobalConfigCommand.actor.cpp
 */

#include "fmt/format.h"
#include "fdbcli/fdbcli.actor.h"
#include "fdbclient/Tuple.h"
#include "flow/actorcompiler.h" // This must be the last include

namespace {

class IGlobalConfigKey : public ReferenceCounted<IGlobalConfigKey> {
protected:
	Key key;
	IGlobalConfigKey(Key key) : key(key) {}

public:
	virtual ~IGlobalConfigKey() = default;
	static Reference<IGlobalConfigKey> create(Key key);
	Key withPrefix() const { return key.withPrefix("\xff\xff/global_config/"_sr); }
	virtual Optional<Value> pack(Value) const = 0;
};

class GlobalConfigKeyDouble : public IGlobalConfigKey {
public:
	GlobalConfigKeyDouble(Key key) : IGlobalConfigKey(key) {}
	Optional<Value> pack(Value value) const override {
		try {
			return Tuple::makeTuple(static_cast<double>(std::stod(value.toString()))).pack();
		} catch (...) {
			TraceEvent(SevWarnAlways, "GlobalConfigKeyDoubleFailedToPackTuple")
			    .detail("Key", key)
			    .detail("Value", value);
			return {};
		}
	}
};

Reference<IGlobalConfigKey> IGlobalConfigKey::create(Key key) {
	if (key.compare("quotaThrottling/readSampleRate"_sr) == 0) {
		return makeReference<GlobalConfigKeyDouble>(key);
	} else {
		return Reference<IGlobalConfigKey>();
	}
}

ACTOR Future<Void> globalConfigGet(Reference<IDatabase> db, Reference<IGlobalConfigKey> key) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			ThreadFuture<ValueReadResult> resultFuture = tr->get(key->withPrefix());
			ValueReadResult v = wait(safeThreadFutureToFuture(resultFuture));
			if (v.present()) {
				fmt::print("{}", v.get().toString());
			} else {
				fmt::print("<not set>\n");
			}
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<Void> globalConfigSet(Reference<IDatabase> db, Reference<IGlobalConfigKey> key, Value value) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			auto const packedValue = key->pack(value);
			if (packedValue.present()) {
				tr->set(key->withPrefix(), packedValue.get());
			} else {
				fmt::print(stderr, "Unable to pack value\n");
				return Void();
			}
			wait(safeThreadFutureToFuture(tr->commit()));
			fmt::print("Successfully set global_config value.\n");
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

constexpr auto usage = "global_config [get <key> | set <key> <value>]";

bool exitFailure() {
	fmt::print("{}\n", usage);
	return false;
}

} // namespace

namespace fdb_cli {

ACTOR Future<bool> globalConfigCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	state bool result = true;
	if (tokens.size() != 3 && tokens.size() != 4) {
		return exitFailure();
	} else {
		auto const key = IGlobalConfigKey::create(tokens[2]);
		if (!key.isValid()) {
			fmt::print(stderr, "Unrecognized key: {}\n", tokens[2].toString());
			exitFailure();
		}
		if (tokens[1] == "get"_sr) {
			if (tokens.size() != 3) {
				return exitFailure();
			}
			wait(globalConfigGet(db, key));
			return true;
		} else if (tokens[1] == "set"_sr) {
			if (tokens.size() != 4) {
				return exitFailure();
			}
			auto const value = tokens[3];
			wait(globalConfigSet(db, key, value));
			return true;
		} else {
			return exitFailure();
		}
	}
}

void globalConfigGenerator(const char* text,
                           const char* line,
                           std::vector<std::string>& lc,
                           std::vector<StringRef> const& tokens) {
	const char* opts[] = { "get", "set", nullptr };
	arrayGenerator(text, line, opts, lc);
}

CommandFactory globalConfigFactory("global_config",
                                   CommandHelp("global_config",
                                               "global_config [get <key> | set <key> <value>]",
                                               "Get or set a key in the global configuration."),
                                   &globalConfigGenerator);

} // namespace fdb_cli
