/*
 * EncryptCommand.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/json_spirit/json_spirit_value.h"
#include "fdbclient/json_spirit/json_spirit_writer_options.h"
#include "fdbclient/SpecialKeySpace.actor.h"
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
		state Reference<ReadYourWritesTransaction> txn = makeReference<ReadYourWritesTransaction>(db);
		KeyRange range = SpecialKeySpace::getEncryptionApiCommandRange("latestcipherkeysbydomain");
		state Key prefix = range.begin;
		if (domainId.present()) {
			range = singleKeyRange(Tuple::makeTuple(domainId.get()).pack().withPrefix(prefix));
		}
		RangeReadResult result = wait(txn->getRange(range, GetRangeLimits()));
		JsonBuilderObject jsonResult;
		for (auto& domain : result) {
			EncryptCipherDomainId domainId = Tuple::unpack(domain.key.removePrefix(prefix)).getInt(0);
			auto cipherKeys =
			    ObjectReader::fromStringRef<std::vector<EncryptCipherKeyEntry>>(domain.value, Unversioned());
			JsonBuilderArray domainArray;
			for (auto& cipherKey : cipherKeys) {
				JsonBuilderObject cipherKeyObject;
				cipherKeyObject["cipher_key_id"] = std::to_string(cipherKey.id);
				cipherKeyObject["last_access_time"] = std::to_string(cipherKey.lastAccessTime);
				domainArray.push_back(cipherKeyObject);
			}
			jsonResult[std::to_string(domainId)] = domainArray;
		}
		json_spirit::mValue jsonValue;
		json_spirit::read_string(jsonResult.getJson(), jsonValue);
		printf("%s\n", json_spirit::write_string(jsonValue, json_spirit::Output_options::pretty_print).c_str());
	} else {
		printUsage(tokens[0]);
		return false;
	}
	return true;
}

CommandFactory encryptFactory("encrypt", fdb_cli::CommandHelp("encrypt listlatestCipherkeys", "", ""));

} // namespace fdb_cli