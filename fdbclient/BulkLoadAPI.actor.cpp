/*
 * BulkLoadAPI.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "flow/CodeProbe.h"
#include "fmt/format.h"

#include "fdbrpc/TenantInfo.h"
#include "fdbclient/BlobGranuleRequest.actor.h"
#include "fdbclient/BulkLoadMetadata.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/IKnobCollection.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define BULKLOAD_DEBUG false

BulkLoadID newBulkLoadID() {
	// for now just generate UID and convert to string - we assume no 128-bit UID collisions
	UID randomUID = deterministicRandom()->randomUniqueID();
	// TODO this is the same as granuleIDToCFKey - make common function?
	BinaryWriter wr(Unversioned());
	wr << randomUID;
	return wr.toValue();
}

ACTOR Future<BulkLoadID> startBulkLoadActor(Reference<DatabaseContext> db,
                                            KeyRange range,
                                            Optional<Reference<Tenant>> tenant) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state Standalone<StringRef> id = newBulkLoadID();
	state BulkLoadInfo info(TenantInfo::INVALID_TENANT, range);
	state KeyRange physicalRange = range;

	if (!CLIENT_KNOBS->ALLOW_BULK_LOADS) {
		throw unsupported_operation();
	}

	if (tenant.present()) {
		wait(tenant.get()->ready());
		info.tenantId = tenant.get()->id();
		physicalRange = range.withPrefix(tenant.get()->prefix());
	}

	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);

			// make sure no other bulk load is already using range
			state Reference<BulkLoadMetadata::RangeMapSnapshot> existingRanges =
			    wait(BulkLoadMetadata::rangeMap().getSnapshot(tr, physicalRange.begin, physicalRange.end));

			// detect conflicts but exempt this load already existing for idempotency. Do this before the updateRange
			// call to avoid conflicts/issues
			auto ranges = existingRanges->ranges();
			auto it = ranges.begin();
			while (it != ranges.end()) {
				if (!it.value().empty() && it.value() == id && it.range().begin == physicalRange.begin &&
				    it.range().end == physicalRange.end) {
					// idempotency - already set this exact bulk load and retried
					CODE_PROBE(true, "start bulk load idempotent on retry");
					return id;
				}
				if (!it.value().empty()) {
					throw bulk_load_range_conflict();
				}
				++it;
			}

			// other sanity checks
			state Future<RangeReadResult> keysInRangeF = tr->getRange(physicalRange, 1);
			state Future<Standalone<VectorRef<KeyRangeRef>>> blobRangesInRangeF =
			    getBlobRanges(&tr->getTransaction(), physicalRange, 1);

			RangeReadResult keysInRange = wait(keysInRangeF);
			ASSERT(keysInRange.size() <= 1);
			if (!keysInRange.empty() || keysInRange.more) {
				throw bulk_load_range_not_empty();
			}

			state Standalone<VectorRef<KeyRangeRef>> blobRangesInRange = wait(blobRangesInRangeF);
			if (!blobRangesInRange.empty()) {
				throw bulk_load_range_not_empty();
			}

			BulkLoadMetadata::statusMap().set(tr, id, BulkLoadStatus(BulkLoadState::STARTED));
			BulkLoadMetadata::versionMap().setVersionstamp(tr, id, Versionstamp(), 0);
			BulkLoadMetadata::infoMap().set(tr, id, info);

			wait(BulkLoadMetadata::rangeMap().updateRange(tr, physicalRange.begin, physicalRange.end, id, true));

			wait(tr->commit());

			return id;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

Future<BulkLoadID> DatabaseContext::startBulkLoad(KeyRange range, Optional<Reference<Tenant>> tenant) {
	return startBulkLoadActor(Reference<DatabaseContext>::addRef(this), range, tenant);
}

ACTOR Future<Void> updateBulkLoadStateActor(Reference<DatabaseContext> db, BulkLoadID id, BulkLoadState desiredState) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state KeyRange physicalRange;
	ASSERT(desiredState != BulkLoadState::UNKNOWN);
	if (BULKLOAD_DEBUG) {
		fmt::print("UpdateBulkLoadState {0}: {1}\n", id.printable(), static_cast<int>(desiredState));
	}
	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);

			if (BULKLOAD_DEBUG) {
				fmt::print("UpdateBulkLoadState {0}: get current\n", id.printable());
			}
			Optional<BulkLoadStatus> currentStatus = wait(BulkLoadMetadata::statusMap().get(tr, id));
			if (!currentStatus.present()) {
				// idempotency
				if (desiredState == BulkLoadState::CLEANUP) {
					return Void();
				}
				throw bulk_load_not_found();
			}

			// throw an error if invalid state transition, otherwise if current state already meets desired, return
			if (BULKLOAD_DEBUG) {
				fmt::print("UpdateBulkLoadState {0}: {1} -> {2}\n",
				           id.printable(),
				           static_cast<int>(currentStatus.get().state),
				           static_cast<int>(desiredState));
			}
			if (!checkValidBulkLoadStateTransition(currentStatus.get().state, desiredState)) {
				if (BULKLOAD_DEBUG) {
					fmt::print("UpdateBulkLoadState {0}: already in desired state\n", id.printable());
				}
				CODE_PROBE(true, "update bulk load state idempotent");
				return Void();
			}
			if (BULKLOAD_DEBUG) {
				fmt::print("UpdateBulkLoadState {0}: transition ok\n", id.printable());
			}

			if (desiredState == BulkLoadState::CLEANUP) {
				if (BULKLOAD_DEBUG) {
					fmt::print("UpdateBulkLoadState {0}: erasing on cleanup\n", id.printable());
				}
				BulkLoadMetadata::statusMap().erase(tr, id);
			} else {
				BulkLoadStatus newStatus = currentStatus.get();
				newStatus.state = desiredState;
				BulkLoadMetadata::statusMap().set(tr, id, newStatus);

				// if cleaning up after abort or completed, clean up other metadata since it is no longer active
				if (desiredState == BulkLoadState::DONE || desiredState == BulkLoadState::ABORTED_COMPLETE) {
					// load bulk load info to get range for load
					Optional<BulkLoadInfo> info = wait(BulkLoadMetadata::infoMap().get(tr, id));
					ASSERT(info.present());

					physicalRange = info.get().keyRange;
					if (info.get().tenantId != TenantInfo::INVALID_TENANT) {
						physicalRange = physicalRange.withPrefix(TenantAPI::idToPrefix(info.get().tenantId));
					}

					// fmt::print("UpdateBulkLoadState {0}: Checking range [{1} - {2}) in map\n", id.printable(),
					// physicalRange.begin.printable(), physicalRange.end.printable());

					// load and check range map for sanity check/validation and to get read conflict range
					Optional<BulkLoadMetadata::RangeMap::RangeValue> range =
					    wait(BulkLoadMetadata::rangeMap().getRangeForKey(tr, physicalRange.begin));
					ASSERT(range.present());
					ASSERT(range.get().range.begin == physicalRange.begin &&
					       range.get().range.end == physicalRange.end && range.get().value == id);

					// set range to inactive when complete and clear info
					// FIXME: any way to make this coalescing?
					wait(BulkLoadMetadata::rangeMap().updateRange(
					    tr, physicalRange.begin, physicalRange.end, ""_sr, true));
					BulkLoadMetadata::infoMap().erase(tr, id);
					BulkLoadMetadata::versionMap().erase(tr, id);
				}
			}

			// fmt::print("UpdateBulkLoadState {0}: committing\n", id.printable());

			wait(tr->commit());
			if (BULKLOAD_DEBUG) {
				fmt::print("CheckBulkLoadStateTransition {0}: -> {1} committed\n",
				           id.printable(),
				           static_cast<int>(desiredState));
			}
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

Future<Void> DatabaseContext::updateBulkLoadState(BulkLoadID id, BulkLoadState desiredState) {
	// right now we don't need tenant, we will likely need it for authz though
	return updateBulkLoadStateActor(Reference<DatabaseContext>::addRef(this), id, desiredState);
}

Future<Void> DatabaseContext::abortBulkLoad(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	// right now we don't need tenant, we will likely need it for authz though
	return updateBulkLoadStateActor(Reference<DatabaseContext>::addRef(this), id, BulkLoadState::ABORTED);
}

Future<Void> DatabaseContext::bulkLoadAllFilesRegistered(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	// right now we don't need tenant, we will likely need it for authz though
	return updateBulkLoadStateActor(Reference<DatabaseContext>::addRef(this), id, BulkLoadState::STAGING);
}

ACTOR Future<Void> waitBulkLoadStateAtLeastActor(Reference<DatabaseContext> db,
                                                 BulkLoadID id,
                                                 BulkLoadState desiredState) {
	ASSERT(desiredState != BulkLoadState::UNKNOWN);
	state Database cx(db);
	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				SystemDBWriteLockedNow(db)->setOptions(tr);

				Optional<BulkLoadStatus> status = wait(BulkLoadMetadata::statusMap().get(tr, id));

				if (!status.present()) {
					CODE_PROBE(true, "bulk load cleaned up while waiting for state at least");
					if (desiredState == BulkLoadState::CLEANUP) {
						return Void();
					}
					throw bulk_load_not_found();
				}
				auto st = status.get().state;
				if (st == BulkLoadState::ABORTED_COMPLETE || st == BulkLoadState::ABORTED) {
					if (desiredState != BulkLoadState::ABORTED && desiredState != BulkLoadState::ABORTED_COMPLETE) {
						throw bulk_load_aborted();
					}
				}
				if (st >= desiredState) {
					return Void();
				}

				state Future<Void> onChange = BulkLoadMetadata::statusMap().watch(tr, id);
				wait(tr->commit());
				wait(onChange);
				tr->reset();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}
}

ACTOR Future<BulkLoadStatus> getBulkLoadStatusActor(Reference<DatabaseContext> db, BulkLoadID id) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);

			Optional<BulkLoadStatus> status = wait(BulkLoadMetadata::statusMap().get(tr, id));
			if (!status.present()) {
				throw bulk_load_not_found();
			}
			return status.get();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> cleanupBulkLoadActor(Reference<DatabaseContext> db, BulkLoadID id) {
	// FIXME: could merge this into 1 transaction somehow if desired, either with custom code or just making
	// updateBulkLoadState ABORTED->ABORTED_COMPLETE handle this case?
	if (BULKLOAD_DEBUG) {
		fmt::print("CleanUpBulkLoad {0}: start\n", id.printable());
	}
	BulkLoadStatus startStatus = wait(getBulkLoadStatusActor(db, id));
	if (BULKLOAD_DEBUG) {
		fmt::print("CleanUpBulkLoad {0}: status {1}\n", id.printable(), static_cast<int>(startStatus.state));
	}
	// if bulk load is aborted, wait for internal server cleanup to happen before client can do final clean up
	if (startStatus.state == BulkLoadState::ABORTED) {
		if (BULKLOAD_DEBUG) {
			fmt::print("CleanUpBulkLoad {0}: waiting abort complete\n", id.printable());
		}
		wait(waitBulkLoadStateAtLeastActor(db, id, BulkLoadState::ABORTED_COMPLETE));
	}
	if (BULKLOAD_DEBUG) {
		fmt::print("CleanUpBulkLoad {0}: finalizing\n", id.printable());
	}
	wait(updateBulkLoadStateActor(db, id, BulkLoadState::CLEANUP));
	if (BULKLOAD_DEBUG) {
		fmt::print("CleanUpBulkLoad {0}: done\n", id.printable());
	}
	return Void();
}

Future<Void> DatabaseContext::cleanupBulkLoad(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	// right now we don't need tenant, we will likely need it for authz though
	return cleanupBulkLoadActor(Reference<DatabaseContext>::addRef(this), id);
}

Future<Void> DatabaseContext::waitBulkLoadStateAtLeast(BulkLoadID id, BulkLoadState desiredState) {
	return waitBulkLoadStateAtLeastActor(Reference<DatabaseContext>::addRef(this), id, desiredState);
}

Future<Void> DatabaseContext::waitBulkLoadComplete(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	// right now we don't need tenant, we will likely need it for authz though
	return waitBulkLoadStateAtLeastActor(Reference<DatabaseContext>::addRef(this), id, BulkLoadState::DONE);
}

ACTOR Future<std::vector<BulkLoadID>> listBulkLoadsActor(Reference<DatabaseContext> db, bool onlyActive) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state std::vector<BulkLoadID> values;
	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);
			values.clear();
			BulkLoadMetadata::StatusMap::RangeResultType result =
			    wait(BulkLoadMetadata::statusMap().getRange(tr, {}, {}, CLIENT_KNOBS->TOO_MANY)); // TODO
			ASSERT(!result.more);
			for (auto& it : result.results) {
				if (!onlyActive ||
				    (it.second.state != BulkLoadState::ABORTED && it.second.state != BulkLoadState::DONE)) {
					values.push_back(it.first);
				}
			}

			return values;

		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

Future<std::vector<BulkLoadID>> DatabaseContext::listBulkLoads(bool onlyActive) {
	return listBulkLoadsActor(Reference<DatabaseContext>::addRef(this), onlyActive);
}

Future<BulkLoadStatus> DatabaseContext::getBulkLoadStatus(BulkLoadID id) {
	return getBulkLoadStatusActor(Reference<DatabaseContext>::addRef(this), id);
}

ACTOR Future<Void> cleanupAbortedBulkLoadActor(Reference<DatabaseContext> db, BulkLoadID id) {
	BulkLoadStatus startStatus = wait(getBulkLoadStatusActor(db, id));

	// ensure load was in aborted state before starting cleanup
	if (!checkValidBulkLoadStateTransition(startStatus.state, BulkLoadState::ABORTED_COMPLETE)) {
		if (BULKLOAD_DEBUG) {
			fmt::print("CleanupAbortedBulkLoadActor {0}: already cleaned up\n", id.printable());
		}
		CODE_PROBE(true, "cleanup aborted bulk load already done");
		return Void();
	}

	// TODO: implement actual cleanup!

	// change bulk load state to ABORTED_COMPLETE
	wait(updateBulkLoadStateActor(db, id, BulkLoadState::ABORTED_COMPLETE));

	return Void();
}

Future<Void> DatabaseContext::cleanupAbortedBulkLoad(BulkLoadID id) {
	return cleanupAbortedBulkLoadActor(Reference<DatabaseContext>::addRef(this), id);
}

ACTOR Future<Version> getBulkLoadVersionActor(Reference<DatabaseContext> db, BulkLoadID id) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);

			Optional<Versionstamp> status = wait(BulkLoadMetadata::versionMap().get(tr, id));
			if (!status.present()) {
				throw bulk_load_not_found();
			}
			return status.get().version;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

Future<Version> DatabaseContext::getBulkLoadVersion(BulkLoadID id) {
	return getBulkLoadVersionActor(Reference<DatabaseContext>::addRef(this), id);
}