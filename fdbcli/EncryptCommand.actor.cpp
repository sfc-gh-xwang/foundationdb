/*
 * EncryptCommand.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/json_spirit/json_spirit_value.h"
#include "fdbclient/json_spirit/json_spirit_writer_options.h"
#include "flow/Arena.h"
#include "flow/EncryptUtils.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> encryptCommandActor(Database db, std::vector<StringRef> tokens) {
	ASSERT_EQ(tokens[0], "encrypt");
	if (tokens.size() < 2) {
		printUsage(tokens[0]);
		return false;
	}
	if (tokencmp(tokens[1], "listlatestcipherkeys")) {
		Optional<EncryptCipherDomainId> domainId;
		if (tokens.size() == 3) {
			domainId = atoll(tokens[2].toString().c_str());
		} else if (tokens.size() > 3) {
			printUsage(tokens[0]);
			return false;
		}
		Standalone<StringRef> result = wait(db->getLatestEncryptCipherKeysStatus(domainId));
		json_spirit::mValue jsonValue;
		json_spirit::read_string(result.toString(), jsonValue);
		printf("%s\n", json_spirit::write_string(jsonValue, json_spirit::Output_options::pretty_print).c_str());
	} else {
		printUsage(tokens[0]);
		return false;
	}
	return true;
}

CommandFactory encryptFactory("encrypt", fdb_cli::CommandHelp("encrypt listlatestCipherkeys", "", ""));

} // namespace fdb_cli