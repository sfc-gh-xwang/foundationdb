/*
 * BulkLoadCommand.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbcli/fdbcli.actor.h"
#include "fdbclient/BulkLoad.h"
#include "fdbclient/BulkLoadMetadata.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

ACTOR Future<Optional<BulkLoadInfo>> getBulkLoadInfo(BulkLoadID id, Database db, Optional<Reference<Tenant>> tenant) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	loop {
		try {
			SystemDBWriteLockedNow(db.getReference())->setOptions(tr);
			int64_t tenantId = tenant.present() ? tenant.get()->id() : TenantInfo::INVALID_TENANT;
			Optional<BulkLoadInfo> info = wait(BulkLoadMetadata::infoMap().get(tr, { tenantId, id }));
			return info;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> printBulkLoadStatus(BulkLoadID id, Database db, Optional<Reference<Tenant>> tenant) {
	BulkLoadStatus status = wait(db->getBulkLoadStatus(id, tenant));
	fmt::print("          ID: {}\n", id.printable());
	fmt::print("       State: {}\n", status.toStateString());

	Optional<BulkLoadInfo> info = wait(getBulkLoadInfo(id, db, tenant));
	if (info.present()) {
		fmt::print("       Range: {}\n", info->keyRange.toString());
		if (tenant.present()) {
			fmt::print("      Tenant: {}\n", tenant.get()->description());
		}
	}
	return Void();
}
} // namespace

namespace fdb_cli {

ACTOR Future<bool> bulkLoadCommandActor(Database localDb,
                                        Optional<TenantMapEntry> tenantEntry,
                                        std::vector<StringRef> tokens) {
	// enables blob writing for the given range
	if (tokens.size() != 2 && tokens.size() != 3 && tokens.size() != 4) {
		printUsage(tokens[0]);
		return false;
	}

	state Optional<Reference<Tenant>> tenant;
	if (tenantEntry.present()) {
		tenant = Reference<Tenant>(new Tenant(localDb, tenantEntry.get().id));
	}

	Key command = tokens[1];
	state Key id;
	if (tokencmp(tokens[1], "start")) {
		if (tokens.size() != 4) {
			fmt::print("Missing key range.\n");
			printUsage(tokens[0]);
			return false;
		}
		Key begin = tokens[2];
		Key end = tokens[3];
		BulkLoadID id = wait(localDb->startBulkLoad(KeyRangeRef(begin, end), tenant));
		fmt::print("Bulk Load {} is started successfully.\n", id.printable());
	} else if (tokencmp(tokens[1], "stop")) {
		if (tokens.size() != 3) {
			fmt::print("Missing bulk load ID.\n");
			printUsage(tokens[0]);
			return false;
		}
		id = tokens[2];
		wait(localDb->abortBulkLoad(id, tenant));
		fmt::print("Bulk Load {} is stopped successfully.\n", id.printable());
	} else if (tokencmp(tokens[1], "status")) {
		if (tokens.size() != 3) {
			fmt::print("Missing bulk load ID.\n");
			printUsage(tokens[0]);
			return false;
		}
		id = tokens[2];
		wait(printBulkLoadStatus(id, localDb, tenant));
	} else if (tokencmp(tokens[1], "cleanup")) {
		if (tokens.size() != 3) {
			fmt::print("Missing bulk load ID.\n");
			printUsage(tokens[0]);
			return false;
		}
		id = tokens[2];
		wait(localDb->cleanupBulkLoad(id, tenant));
		fmt::print("Bulk Load {} is cleaned up successfully.\n", id.printable());
	} else if (tokencmp(tokens[1], "list")) {
		Standalone<VectorRef<BulkLoadIDRef>> list = wait(localDb->listBulkLoads(true, tenant));
		if (list.empty()) {
			fmt::print("No active bulk loads.\n");
		}
		int i = 0;
		for (auto& id : list) {
			fmt::print("{}. {}\n", ++i, id.printable());
		}
	} else {
		printUsage(tokens[0]);
		return false;
	}

	return true;
}

CommandFactory bulkLoadFactory("bulkload",
                               CommandHelp("bulkload <start|stop|list|status|cleanup> [ID] [BEGINKEY] [ENDKEY]",
                                           "Manage bulk load jobs",
                                           ""));
} // namespace fdb_cli
