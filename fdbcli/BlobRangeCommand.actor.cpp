/*
 * BlobRangeCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

ACTOR Future<Version> getLatestReadVersion(Database db) {
	state Transaction tr(db);
	loop {
		try {
			Version rv = wait(tr.getReadVersion());
			fmt::print("Resolved latest read version as {0}\n", rv);
			return rv;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// print after delay if not cancelled
ACTOR Future<Void> printAfterDelay(double delaySeconds, std::string message) {
	wait(delay(delaySeconds));
	fmt::print("{}\n", message);
	return Void();
}

ACTOR Future<Void> doBlobPurge(Database db,
                               Optional<Reference<Tenant>> tenant,
                               Key startKey,
                               Key endKey,
                               Optional<Version> version,
                               bool force) {
	state Version purgeVersion;
	if (version.present()) {
		purgeVersion = version.get();
	} else {
		wait(store(purgeVersion, getLatestReadVersion(db)));
	}

	state Key purgeKey =
	    wait(db->purgeBlobGranules(KeyRange(KeyRangeRef(startKey, endKey)), purgeVersion, tenant, force));

	fmt::print("Blob purge registered for [{0} - {1}) @ {2}\n", startKey.printable(), endKey.printable(), purgeVersion);
	fmt::print("Purge Key: {0}\n", purgeKey.printable());

	state Future<Void> printWarningActor = printAfterDelay(
	    5.0, "Waiting for purge to complete. (interrupting this wait with CTRL+C will not cancel the purge)");
	wait(db->waitPurgeGranulesComplete(purgeKey));

	fmt::print("Blob purge complete for [{0} - {1}) @ {2}\n", startKey.printable(), endKey.printable(), purgeVersion);

	return Void();
}

ACTOR Future<Void> doBlobCheck(Database db,
                               Optional<Reference<Tenant>> tenant,
                               Key startKey,
                               Key endKey,
                               Optional<Version> version) {
	state double elapsed = -timer_monotonic();

	state Version readVersionOut = wait(db->verifyBlobRange(KeyRangeRef(startKey, endKey), version, tenant));

	elapsed += timer_monotonic();

	fmt::print("Blob check complete for [{0} - {1}) @ {2} in {3:.6f} seconds\n",
	           startKey.printable(),
	           endKey.printable(),
	           readVersionOut,
	           elapsed);
	return Void();
}

ACTOR Future<Void> doBlobDebug(Database db,
                               Optional<Reference<Tenant>> tenant,
                               Key startKey,
                               Key endKey,
                               Optional<Version> readVersion) {
	state Transaction tr(db);

	state Version version = wait(tr.getReadVersion());
	state Standalone<VectorRef<BlobGranuleChunkRef>> chunks =
	    wait(tr.readBlobGranules(KeyRangeRef(startKey, endKey), version, readVersion));
	printGranuleChunks(chunks);
	return Void();
}

ACTOR Future<Void> doBlobFlush(Database db,
                               Optional<Reference<Tenant>> tenant,
                               Key startKey,
                               Key endKey,
                               Optional<Version> version,
                               bool compact) {
	state double elapsed = -timer_monotonic();
	state KeyRange keyRange(KeyRangeRef(startKey, endKey));
	bool result = wait(db->flushBlobRange(keyRange, compact, version, tenant));
	elapsed += timer_monotonic();

	fmt::print("Blob Flush [{0} - {1}) {2} in {3:.6f} seconds\n",
	           startKey.printable(),
	           endKey.printable(),
	           result ? "succeeded" : "failed",
	           elapsed);

	return Void();
}

} // namespace

namespace fdb_cli {

enum class SubCommand { check, compact, debug, flush, forcepurge, purge, start, stop };
ACTOR Future<bool> blobRangeCommandActor(Database localDb,
                                         Optional<TenantMapEntry> tenantEntry,
                                         std::vector<StringRef> tokens) {
	// enables blob writing for the given range
	// tokens:
	// 0 -- literal "blobrange"
	// 1 -- subcommand name
	// 2 -- start of range
	// 3 -- end of range
	// 4 -- for some subcommands, the version to read
	if (tokens.size() != 4 && tokens.size() != 5) {
		printUsage(tokens[0]);
		return false;
	}

	state Optional<Reference<Tenant>> tenant;
	if (tenantEntry.present()) {
		tenant = Reference<Tenant>(new Tenant(localDb, tenantEntry.get().id));
	}

	state StringRef subcommand = tokens[1];
	state Key begin = tokens[2];
	state Key end = tokens[3];

	if (end > "\xff"_sr) {
		// TODO is this something we want?
		fmt::print("Cannot blobbify system keyspace! Problematic End Key: {0}\n", end.printable());
		return false;
	}

	if (begin >= end) {
		fmt::print("Invalid blob range [{0} - {1})\n", begin.printable(), end.printable());
		return false;
	}

	state SubCommand subc;
	if (tokencmp(subcommand, "check")) {
		subc = SubCommand::check;
	} else if (tokencmp(subcommand, "compact")) {
		subc = SubCommand::compact;
	} else if (tokencmp(subcommand, "debug")) {
		subc = SubCommand::debug;
	} else if (tokencmp(subcommand, "flush")) {
		subc = SubCommand::flush;
	} else if (tokencmp(subcommand, "forcepurge")) {
		subc = SubCommand::forcepurge;
	} else if (tokencmp(subcommand, "purge")) {
		subc = SubCommand::purge;
	} else if (tokencmp(subcommand, "start")) {
		subc = SubCommand::start;
	} else if (tokencmp(subcommand, "stop")) {
		subc = SubCommand::stop;
	} else {
		printUsage(tokens[0]);
		return false;
	}
	state Optional<Version> version;
	if (tokens.size() > 4) {
		Version v;
		int n = 0;
		if (sscanf(tokens[4].toString().c_str(), "%" PRId64 "%n", &v, &n) != 1 || n != tokens[4].size()) {
			printUsage(tokens[0]);
			return false;
		}
		version = v;
	}

	fmt::print("{0} blob range [{1} - {2})", subcommand.printable(), begin.printable(), end.printable());
	if (version.present()) {
		fmt::print(" @ {0}", version.get());
	}
	fmt::print("\n");

	if (subc == SubCommand::start || subc == SubCommand::stop) {
		// Cannot take a version parameter
		if (version.present()) {
			printUsage(tokens[0]);
			return false;
		}
		// start and stop commands can fail, whereas others always succeed, so need slightly more complicated machinery
		Future<bool> startstop;
		if (subc == SubCommand::start) {
			startstop = localDb->blobbifyRange(KeyRangeRef(begin, end), tenant);
		} else {
			startstop = localDb->unblobbifyRange(KeyRangeRef(begin, end), tenant);
		}
		bool success = wait(std::move(startstop));
		fmt::print("{0} blobbify range for [{1} - {2}) {3}\n",
		           subc == SubCommand::start ? "Starting" : "Stopping",
		           begin.printable(),
		           end.printable(),
		           success ? "succeeded" : "failed");
		return success;
	}

	Future<Void> actor;

	if (subc == SubCommand::purge || subc == SubCommand::forcepurge) {
		actor = doBlobPurge(localDb, tenant, begin, end, version, subc == SubCommand::forcepurge);
	} else if (subc == SubCommand::check) {
		actor = doBlobCheck(localDb, tenant, begin, end, version);
	} else if (subc == SubCommand::debug) {
		actor = doBlobDebug(localDb, tenant, begin, end, version);
	} else if (subc == SubCommand::compact || subc == SubCommand::flush) {
		actor = doBlobFlush(localDb, tenant, begin, end, version, subc == SubCommand::compact);
	} else {
		ASSERT(false);
	}

	wait(std::move(actor));
	return true;
}

CommandFactory blobRangeFactory(
    "blobrange",
    CommandHelp("blobrange <start|stop|check|debug|purge|forcepurge|flush|compact> <startkey> <endkey> [version]",
                "",
                ""));
} // namespace fdb_cli
