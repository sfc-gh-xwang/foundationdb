/*
 * BulkLoadOpsWorkload.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/Util.h"
#include "fdbclient/BulkLoad.h"
#include "fdbclient/BulkLoadMetadata.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define BLW_DEBUG true

// A workload specifically designed to stress bulk loading functionality
struct BulkLoadOpsWorkload : TestWorkload {
	static constexpr auto NAME = "BulkLoadOps";
	// test settings
	double testDuration;
	int concurrentLoads;
	bool sequential;
	int sequentialGap;

	Future<Void> client;
	Future<Void> unitClient;
	bool stopTest;
	Optional<TenantName> tenantName;
	Optional<Reference<Tenant>> tenant;

	int32_t nextKey;

	std::vector<Future<Void>> loadClients;

	BulkLoadOpsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 30.0);
		concurrentLoads = getOption(options, "concurrentLoads"_sr, deterministicRandom()->randomInt(1, 100));

		stopTest = false;
		tenantName = StringRef("bulkLoadOpsTenant" + std::to_string(clientId));

		int64_t rand = wcx.sharedRandomNumber;
		sequential = rand % 2;
		rand /= 2;

		sequentialGap = 1 + rand % 2;
		rand /= 2;

		nextKey = 10000000 * clientId;

		TraceEvent("BulkLoadOpsWorkloadInit").detail("ConcurrentLoads", concurrentLoads);
	}

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	std::string newPrefix() {
		if (sequential) {
			nextKey += sequentialGap;
			return format("%08x", nextKey);
		} else {
			return deterministicRandom()->randomUniqueID().toString();
		}
	}

	ACTOR Future<TenantMapEntry> setupTenant(Database cx, TenantName name) {
		if (BLW_DEBUG) {
			fmt::print("Creating tenant: {0}\n", name.printable());
		}

		Optional<TenantMapEntry> entry = wait(TenantAPI::createTenant(cx.getReference(), name));
		ASSERT(entry.present());

		if (BLW_DEBUG) {
			fmt::print("Created tenant {0}: {1}\n", name.printable(), entry.get().prefix.printable());
		}

		return entry.get();
	}

	ACTOR Future<Void> _setup(Database cx, BulkLoadOpsWorkload* self) {
		// create initial target ranges
		TraceEvent("BulkLoadOpsSetup").detail("ConcurrentLoads", self->concurrentLoads).log();

		// enable blob granules
		wait(success(ManagementAPI::changeConfig(cx.getReference(), "blob_granules_enabled=1", true)));

		if (cx->clientInfo->get().tenantMode != TenantMode::REQUIRED && deterministicRandom()->coinflip()) {
			self->tenantName.reset();
		}

		if (self->tenantName.present()) {
			wait(success(self->setupTenant(cx, self->tenantName.get())));

			self->tenant = makeReference<Tenant>(cx, self->tenantName.get());
		}

		TraceEvent("BulkLoadOpsSetupComplete");
		return Void();
	}

	std::string newKey() {
		if (sequential) {
			nextKey += sequentialGap;
			return format("%08x", nextKey);
		} else {
			return deterministicRandom()->randomUniqueID().toString();
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			for (int i = 0; i < concurrentLoads; i++) {
				loadClients.push_back(bulkLoadClient(cx->clone(), this, i));
			}
			unitClient = bulkLoadUnitTests(cx->clone(), this);
		} else {
			unitClient = Future<Void>(Void());
		}
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		client = Future<Void>();
		stopTest = true;
		if (clientId == 0) {
			return _check(cx, this);
		}
		return true;
	}

	ACTOR Future<bool> _check(Database cx, BulkLoadOpsWorkload* self) {
		// FIXME: REENABLE!
		/*if (deterministicRandom()->coinflip()) {
		    cx->internal = IsInternal::False;
		}*/
		// TODO: REMOVE
		cx->internal = IsInternal::True;

		TraceEvent("BulkLoadOpsCheck").detail("ClientId", self->clientId).log();

		wait(waitForAll(self->loadClients));
		TraceEvent("BulkLoadOpsClientsComplete");

		wait(self->unitClient);
		TraceEvent("BulkLoadOpsUnitsComplete");

		std::vector<BulkLoadID> loadsNotCleanedUp = wait(cx->listBulkLoads(false));
		fmt::print("{0} running loads\n", loadsNotCleanedUp.size());
		for (auto& it : loadsNotCleanedUp) {
			fmt::print("    {0}\n", it.printable());
		}
		ASSERT(loadsNotCleanedUp.empty());

		TraceEvent("BulkLoadOpsQuietCheckComplete");

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> doInjectAbort(Database cx,
	                                 BulkLoadOpsWorkload* self,
	                                 BulkLoadID id,
	                                 Future<Void> loadDone,
	                                 bool* injectedAbort,
	                                 bool* abortCompleted) {
		wait(delay(deterministicRandom()->random01() * 5) || loadDone);
		*injectedAbort = true;
		try {
			wait(cx->abortBulkLoad(id, self->tenant));
			*abortCompleted = true;
		} catch (Error& e) {
			if (e.code() == error_code_bulk_load_abort_failed) {
				// ensure load completes without error if abort failed
				wait(loadDone);
			} else {
				throw e;
			}
		}
		return Void();
	}

	ACTOR Future<Void> doWaitLoad(Future<Void> loadF, const bool* injectedAbort, bool* loadComplete) {
		try {
			wait(loadF);
			*loadComplete = true;
		} catch (Error& e) {
			if (e.code() == error_code_bulk_load_aborted) {
				ASSERT(*injectedAbort);
			} else {
				throw e;
			}
		}
		return Void();
	}

	ACTOR Future<Void> waitOrInjectAbort(Database cx,
	                                     BulkLoadOpsWorkload* self,
	                                     BulkLoadID id,
	                                     KeyRange range,
	                                     bool injectAbort) {
		state bool injectedAbort = false;
		state bool abortCompleted = false;
		state bool loadCompleted = false;

		// wait for bulk load to finish, and maybe try to inject an abort while waiting
		// if we do inject an abort, we wait to verify that either:
		//  - bulk load completes and the abort injection gets bulk_load_abort_failed
		//  - abort injection completes and the bulk load gets bulk_load_aborted
		Future<Void> loadFuture = cx->waitBulkLoadComplete(id, self->tenant);
		Future<Void> abortFuture = injectAbort
		                               ? self->doInjectAbort(cx, self, id, loadFuture, &injectedAbort, &abortCompleted)
		                               : Future<Void>(Void());
		wait(self->doWaitLoad(loadFuture, &injectedAbort, &loadCompleted) && abortFuture);

		// both load and abort cannot have successfully completed
		ASSERT(loadCompleted != abortCompleted);
		// but one of them must have completed
		ASSERT(loadCompleted || abortCompleted);

		if (BLW_DEBUG) {
			fmt::print("BLW {0} {1}: got done\n", range.begin.printable(), id.printable());
		}

		// Validate bulk load state at end.

		BulkLoadStatus status = wait(cx->getBulkLoadStatus(id));
		BulkLoadState current = status.state;

		if (BLW_DEBUG) {
			fmt::print(
			    "BLW {0} {1}: done status {2}\n", range.begin.printable(), id.printable(), static_cast<int>(current));
		}

		if (abortCompleted) {
			ASSERT(current == BulkLoadState::ABORTED || current == BulkLoadState::ABORTED_COMPLETE);
		} else if (!injectedAbort) {
			ASSERT(current == BulkLoadState::DONE);
		} else {
			ASSERT(current == BulkLoadState::DONE || current == BulkLoadState::ABORTED ||
			       current == BulkLoadState::ABORTED_COMPLETE);
		}

		return Void();
	}

	ACTOR Future<Void> doOneBulkLoad(Database cx, BulkLoadOpsWorkload* self, KeyRange range, bool injectAbort) {
		if (BLW_DEBUG) {
			fmt::print("BLW {0}: starting\n", range.begin.printable());
		}
		// set up bulk load
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		if (BLW_DEBUG) {
			fmt::print("BLW {0} {1}: started\n", range.begin.printable(), id.printable());
		}

		// FIXME: actually load data into at least 1 file and register for bulk load!!
		wait(delay(deterministicRandom()->random01() * 2));

		wait(cx->bulkLoadAllFilesRegistered(id, self->tenant));

		if (BLW_DEBUG) {
			fmt::print("BLW: {0} {1}: registered\n", range.begin.printable(), id.printable());
		}

		wait(self->waitOrInjectAbort(cx, self, id, range, injectAbort));

		// FIXME: if DONE, check data matches loaded data for range!

		if (BLW_DEBUG) {
			fmt::print("BLW {0} {1}: cleaning up\n", range.begin.printable(), id.printable());
		}
		// FIXME: once auto-cleanup implemented, sometimes don't clean up, especially if aborted, and ensure auto clean
		// up gets it
		wait(cx->cleanupBulkLoad(id, self->tenant));

		if (BLW_DEBUG) {
			fmt::print("BLW {0} {1}: cleaned up\n", range.begin.printable(), id.printable());
		}

		return Void();
	}

	ACTOR Future<Void> bulkLoadClient(Database cx, BulkLoadOpsWorkload* self, int clientIdx) {
		// FIXME: REENABLE!
		/*if (deterministicRandom()->coinflip()) {
		    cx->internal = IsInternal::False;
		}*/
		// TODO: REMOVE
		cx->internal = IsInternal::True;

		state bool completedAny = false;
		if (BLW_DEBUG) {
			fmt::print("BLW: client {0} workload starting\n", clientIdx);
		}
		loop {
			if (completedAny && self->stopTest) {
				if (BLW_DEBUG) {
					fmt::print("BLW: client {0} workload done\n", clientIdx);
				}
				return Void();
			}

			std::string nextRangeKey = "R_" + self->newKey();
			state KeyRange range(KeyRangeRef(StringRef(nextRangeKey), strinc(StringRef(nextRangeKey))));
			bool injectAbort = deterministicRandom()->random01() < 0.2;
			try {
				fmt::print("BLW {0}: starting one bulk load (client {1})\n", range.begin.printable(), clientIdx);
				wait(self->doOneBulkLoad(cx, self, range, injectAbort));
				fmt::print("BLW {0}: completed one bulk load (client {1})\n", range.begin.printable(), clientIdx);
			} catch (Error& e) {
				if (BLW_DEBUG) {
					fmt::print("BLW {0}: unexpected error {1}\n", range.begin.printable(), e.name());
				}
				throw e;
			}

			completedAny = true;
		}
	}

	enum UnitTestTypes {
		OVERLAPPING_LOADS_UNIT,
		DONE_THEN_ABORT_UNIT,
		ABORT_THEN_DONE_UNIT,
		EARLY_CLEANUP_UNIT,
		INVALID_LOAD_ID_UNIT,
		WRITE_DURING_LOAD_UNIT,
		READ_DURING_LOAD_UNIT,
		OP_COUNT = 7 /* keep this last */
	};

	ACTOR Future<Void> abortAndCleanupAfterUnit(Database cx, BulkLoadOpsWorkload* self, BulkLoadID id) {
		wait(cx->abortBulkLoad(id, self->tenant));
		wait(cx->cleanupBulkLoad(id, self->tenant));
		return Void();
	}

	ACTOR Future<Void> overlappingLoadsUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that client cannot start a bulk load that overlaps with an active one
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		try {
			wait(success(cx->startBulkLoad(range, self->tenant)));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_range_conflict);
		}

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, self, id));

		return Void();
	}

	ACTOR Future<Void> doneThenAbortUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that client cannot abort an already completed bulk load
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		wait(cx->bulkLoadAllFilesRegistered(id, self->tenant));
		wait(cx->waitBulkLoadComplete(id, self->tenant));

		try {
			wait(cx->abortBulkLoad(id, self->tenant));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_abort_failed);
		}

		// cleanup
		wait(cx->cleanupBulkLoad(id, self->tenant));

		return Void();
	}

	ACTOR Future<Void> abortThenDoneUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that client cannot mark an already aborted bulk load as complete
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		wait(cx->abortBulkLoad(id, self->tenant));

		try {
			wait(cx->updateBulkLoadState(id, BulkLoadState::DONE));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_aborted);
		}

		wait(cx->cleanupBulkLoad(id, self->tenant));

		return Void();
	}

	ACTOR Future<Void> earlyCleanupUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that client cannot clean up a bulk load before it is done or aborted
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		try {
			wait(cx->cleanupBulkLoad(id, self->tenant));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_cleanup_failed);
		}

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, self, id));

		return Void();
	}

	ACTOR Future<Void> checkCorrectInvalidIDError(Future<Void> f) {
		try {
			wait(success(f));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_not_found);
		}
		return Void();
	}

	ACTOR Future<Void> invalidLoadIDUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that client accessing invalid bulk load id gets expected error
		state BulkLoadID id = deterministicRandom()->randomUniqueID().toString();
		wait(self->checkCorrectInvalidIDError(cx->bulkLoadAllFilesRegistered(id, self->tenant)));
		wait(self->checkCorrectInvalidIDError(cx->abortBulkLoad(id, self->tenant)));
		wait(self->checkCorrectInvalidIDError(cx->waitBulkLoadComplete(id, self->tenant)));
		// don't test cleanup because it's idempotent if it was already cleaned up
		wait(self->checkCorrectInvalidIDError(cx->waitBulkLoadStateAtLeast(id, BulkLoadState::STAGING)));
		wait(self->checkCorrectInvalidIDError(cx->updateBulkLoadState(id, BulkLoadState::STAGING)));
		wait(self->checkCorrectInvalidIDError(success(cx->getBulkLoadStatus(id))));

		return Void();
	}

	ACTOR Future<Void> writeDuringLoadUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that clients cannot write to the range of an active bulk load
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		state Transaction tr(cx);
		try {
			loop {
				try {
					tr.set(range.begin, "abcd"_sr);
					wait(tr.commit());
					ASSERT(false);
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		} catch (Error& e) {
			// TODO: same error or new one?
			ASSERT(e.code() == error_code_bulk_load_range_conflict);
		}

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, self, id));

		return Void();
	}

	ACTOR Future<Void> readDuringLoadUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that clients cannot read from the range of an active bulk load
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		state Transaction tr(cx);
		try {
			loop {
				try {
					wait(success(tr.get(range.begin)));
					ASSERT(false);
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		} catch (Error& e) {
			// TODO: same error or new one?
			ASSERT(e.code() == error_code_bulk_load_range_conflict);
		}

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, self, id));

		return Void();
	}

	ACTOR Future<Void> bulkLoadUnitTests(Database cx, BulkLoadOpsWorkload* self) {
		// FIXME: REENABLE!
		/*if (deterministicRandom()->coinflip()) {
		    cx->internal = IsInternal::False;
		}*/
		// TODO: REMOVE
		cx->internal = IsInternal::True;

		loop {
			if (self->stopTest) {
				return Void();
			}
			std::set<UnitTestTypes> excludedTypes;
			excludedTypes.insert(OP_COUNT);

			std::string nextRangeKey = "U_" + self->newKey();
			state KeyRange range(KeyRangeRef(StringRef(nextRangeKey), strinc(StringRef(nextRangeKey))));

			// FIXME: no lock implemented yet so these succeed - unexclude when implemented!
			excludedTypes.insert(WRITE_DURING_LOAD_UNIT);
			excludedTypes.insert(READ_DURING_LOAD_UNIT);

			// prevent infinite loop
			int loopTries = 1000;
			int op = OP_COUNT;
			loop {
				op = deterministicRandom()->randomInt(0, OP_COUNT);
				if (!excludedTypes.count((UnitTestTypes)op)) {
					break;
				}
				loopTries--;
				ASSERT(loopTries >= 0);
			}
			if (BLW_DEBUG) {
				fmt::print(
				    "Selected range [{0} - {1}) for unit {2}.\n", range.begin.printable(), range.end.printable(), op);
			}

			if (op == OVERLAPPING_LOADS_UNIT) {
				wait(self->overlappingLoadsUnit(cx, self, range));
			} else if (op == DONE_THEN_ABORT_UNIT) {
				wait(self->doneThenAbortUnit(cx, self, range));
			} else if (op == ABORT_THEN_DONE_UNIT) {
				wait(self->abortThenDoneUnit(cx, self, range));
			} else if (op == EARLY_CLEANUP_UNIT) {
				wait(self->earlyCleanupUnit(cx, self, range));
			} else if (op == INVALID_LOAD_ID_UNIT) {
				wait(self->invalidLoadIDUnit(cx, self, range));
			} else if (op == WRITE_DURING_LOAD_UNIT) {
				wait(self->writeDuringLoadUnit(cx, self, range));
			} else if (op == READ_DURING_LOAD_UNIT) {
				wait(self->readDuringLoadUnit(cx, self, range));
			} else {
				ASSERT(false);
			}

			wait(delay(1.0));
		}
	}
};

WorkloadFactory<BulkLoadOpsWorkload> BulkLoadOpsWorkloadFactory;
