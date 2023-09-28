/**
 * GlobalConfigCommand.actor.cpp
 */

#include "fdbcli/fdbcli.actor.h"
#include "flow/actorcompiler.h" // This must be the last include

namespace {

ACTOR Future<Void> globalConfigGet(Reference<IDatabase> db, Key key) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			state ThreadFuture<ValueReadResult> resultFuture = tr->get(GlobalConfig::prefixedKey(key));
			ValueReadResult v = wait(safeThreadFutureToFuture(resultFuture));
			if (v.present()) {
				fmt::print("{}\n", v.get().toString());
			} else {
				fmt::print("<not set>\n");
			}
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<Void> globalConfigSet(Reference<IDatabase> db, Key key, Value value) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			tr->set(GlobalConfig::prefixedKey(key), value);
			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

constexpr auto usage = "global_config [get <key> | set <key> <value>]";

bool exitFailure() {
	fmt::print(usage);
	return false;
}

} // namespace

namespace fdb_cli {

ACTOR Future<bool> globalConfigCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	state bool result = true;
	if (tokens.size() != 3 && tokens.size() != 4) {
		return exitFailure();
	} else {
		auto const key = tokens[2];
		if (tokens[1] == "get"_sr) {
			if (tokens.size() != 3) {
				return exitFailure();
			}
			wait(globalConfigGet(db, key));
			return true;
		} else if (tokens[2] == "set"_sr) {
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
