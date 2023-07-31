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

// get bulk loader interfaces, and if unavailable, re-fetch them

bool newLoadersDifferentAndValid(Reference<BulkLoaderInfo> oldLoaders, Reference<BulkLoaderInfo> newLoaders) {
	if (!newLoaders.isValid() || newLoaders->size() == 0) {
		return false;
	}
	if (!oldLoaders.isValid() || oldLoaders->size() == 0) {
		return true;
	}
	if (oldLoaders->size() != newLoaders->size()) {
		return true;
	}
	std::unordered_set<UID> oldLoaderIDs;
	for (int i = 0; i < oldLoaders->size(); i++) {
		// assert ids are unique while building the set
		ASSERT(oldLoaderIDs.insert(oldLoaders->getId(i)).second);
	}
	for (int i = 0; i < newLoaders->size(); i++) {
		if (!oldLoaderIDs.count(newLoaders->getId(i))) {
			return true;
		}
	}
	return false;
}

ACTOR template <class Request, bool P>
Future<REPLY_TYPE(Request)> doBulkLoadRequest(Reference<DatabaseContext> db,
                                              Optional<Reference<Tenant>> tenant,
                                              Request req,
                                              RequestStream<Request, P> BulkLoaderInterface::*channel) {
	// fill out tenant info before requests
	if (BULKLOAD_DEBUG) {
		fmt::print("DoBulkLoadReq: Start\n");
	}
	if (tenant.present()) {
		wait(tenant.get()->ready());
		// FIXME: need to be able to get auth token from a tenant to construct TenantInfo! this won't work with authz
		req.tenantInfo = TenantInfo(tenant.get()->id(), {});
	}
	// FIXME: cache the interfaces in db context

	// Make a request with the set of bulk loaders we find, until all of the loaders we found become failed, because
	// loadBalance assumes the set of interfaces is up to date. If all failed or we can't find any loaders, wait until
	// we get a new valid set of loaders to restart the request to the new loaders
	state Future<REPLY_TYPE(Request)> requestFuture = Never();
	state Future<Void> retryFuture = Never();
	state Reference<BulkLoaderInfo> bulkLoaders;
	loop {
		try {
			Reference<BulkLoaderInfo> newLoaders = wait(db->getBulkLoaders());

			if (newLoadersDifferentAndValid(bulkLoaders, newLoaders)) {
				if (BULKLOAD_DEBUG) {
					fmt::print("DoBulkLoadReq: Found New Loaders ({0})\n", newLoaders->size());
				}
				bulkLoaders = newLoaders;

				resetReply(req);
				requestFuture = loadBalance(bulkLoaders,
				                            channel,
				                            req,
				                            TaskPriority::DefaultPromiseEndpoint,
				                            AtMostOnce::False,
				                            db->enableLocalityLoadBalance ? &db->queueModel : nullptr);

				std::vector<Future<Void>> waitForFailed;
				for (int i = 0; i < bulkLoaders->size(); i++) {
					waitForFailed.push_back(IFailureMonitor::failureMonitor().onStateEqual(
					    bulkLoaders->get(i, channel).getEndpoint(), FailureStatus(true)));
				};
				retryFuture = waitForAll(waitForFailed);
			} else {
				// don't set request future in case they all failed, but it was just temporary because of a network
				// issue instead of actually all dead, so the current set of loaders is still the correct one
				bool alreadyHadLoaders = bulkLoaders.isValid() && bulkLoaders->size() > 0;
				CODE_PROBE(alreadyHadLoaders, "no change in bulk loader interfaces");
				CODE_PROBE(!alreadyHadLoaders, "no valid bulk loader interfaces");
				retryFuture = delay(0);
			}
			choose {
				when(REPLY_TYPE(Request) rep = wait(requestFuture)) {
					if (BULKLOAD_DEBUG) {
						fmt::print("DoBulkLoadReq: Complete\n");
					}
					// FIXME: could buggify randomly retrying the same request because all of these operations need to
					// be idempotent necessarily
					return rep;
				}
				when(wait(retryFuture)) {
					CODE_PROBE(bulkLoaders.isValid() && bulkLoaders->size() > 0, "all bulk loaders failed");
				}
			}
			wait(delayJittered(1.0));
		} catch (Error& e) {
			// FIXME: any errors to retry?
			TraceEvent("BulkLoadRequestError").errorUnsuppressed(e);
			throw e;
		}
	}
}

BulkLoadID newBulkLoadID() {
	// for now just generate UID and convert to string - we assume no 128-bit UID collisions
	UID randomUID = deterministicRandom()->randomUniqueID();
	// TODO this is the same as granuleIDToCFKey - make common function?
	BinaryWriter wr(Unversioned());
	wr << randomUID;
	return wr.toValue();
}

ACTOR Future<BulkLoadID> startBulkLoadExternalActor(Reference<DatabaseContext> db,
                                                    KeyRange range,
                                                    Optional<Reference<Tenant>> tenant) {
	// choose bulk load id on client side to guarantee idempotency for retransmits/duplication, and to not start extra
	// random bulk loads
	state BulkLoadID id = newBulkLoadID();
	BulkLoadStartRequest req;
	req.arena.dependsOn(range.arena());
	req.arena.dependsOn(id.arena());
	req.id = id;
	req.keyRange = range;
	if (BULKLOAD_DEBUG) {
		fmt::print("StartBulkLoadExternal {0}: begin\n", id.printable());
	}
	wait(success(doBulkLoadRequest(db, tenant, req, &BulkLoaderInterface::startRequest)));
	if (BULKLOAD_DEBUG) {
		fmt::print("StartBulkLoadExternal {0}: complete\n", id.printable());
	}
	return id;
}

Future<BulkLoadID> DatabaseContext::startBulkLoad(KeyRange range, Optional<Reference<Tenant>> tenant) {
	if (range.empty()) {
		throw unsupported_operation();
	}
	return startBulkLoadExternalActor(Reference<DatabaseContext>::addRef(this), range, tenant);
}

Future<Void> DatabaseContext::bulkLoadAllFilesRegistered(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	BulkLoadStateChangeRequest req;
	req.arena.dependsOn(id.arena());
	req.id = id;
	req.desired = BulkLoadStatus::State::STAGING;
	return doBulkLoadRequest(
	    Reference<DatabaseContext>::addRef(this), tenant, req, &BulkLoaderInterface::stateChangeRequest);
}

Future<Void> DatabaseContext::abortBulkLoad(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	BulkLoadStateChangeRequest req;
	req.arena.dependsOn(id.arena());
	req.id = id;
	req.desired = BulkLoadStatus::State::ABORTED;
	return doBulkLoadRequest(
	    Reference<DatabaseContext>::addRef(this), tenant, req, &BulkLoaderInterface::stateChangeRequest);
}

Future<Void> DatabaseContext::cleanupBulkLoad(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	BulkLoadStateChangeRequest req;
	req.arena.dependsOn(id.arena());
	req.id = id;
	req.desired = BulkLoadStatus::State::CLEANUP;
	return doBulkLoadRequest(
	    Reference<DatabaseContext>::addRef(this), tenant, req, &BulkLoaderInterface::stateChangeRequest);
}

Future<Void> DatabaseContext::waitBulkLoadComplete(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	BulkLoadWaitCompleteRequest req;
	req.arena.dependsOn(id.arena());
	req.id = id;
	return doBulkLoadRequest(
	    Reference<DatabaseContext>::addRef(this), tenant, req, &BulkLoaderInterface::waitCompleteRequest);
}

ACTOR Future<Void> startBulkLoadInternalActor(Reference<DatabaseContext> db,
                                              BulkLoadID id,
                                              KeyRange range,
                                              Optional<Reference<Tenant>> tenant) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state BulkLoadInfo info(TenantInfo::INVALID_TENANT, range);
	state KeyRange physicalRange = range;

	if (!CLIENT_KNOBS->ALLOW_BULK_LOADS) {
		throw unsupported_operation();
	}

	if (BULKLOAD_DEBUG) {
		fmt::print("StartBulkLoad {0}: begin\n", id.printable());
	}

	if (tenant.present()) {
		wait(tenant.get()->ready());
		info.tenantId = tenant.get()->id();
		physicalRange = range.withPrefix(tenant.get()->prefix());
	}

	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);

			if (BULKLOAD_DEBUG) {
				fmt::print("StartBulkLoad {0}: loopstart\n", id.printable());
			}

			// explicitly check info map to handle idempotency/retransmits
			Optional<BulkLoadInfo> currentInfo = wait(BulkLoadMetadata::infoMap().get(tr, id));
			if (currentInfo.present()) {
				if (currentInfo.get().tenantId != info.tenantId) {
					throw bulk_load_tenant_mismatch();
				}
				if (currentInfo.get().keyRange != info.keyRange) {
					throw bulk_load_range_conflict();
				}
				return Void();
			}

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
					return Void();
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

			BulkLoadMetadata::statusMap().set(tr, id, BulkLoadStatus(BulkLoadStatus::State::STARTED));
			BulkLoadMetadata::versionMap().setVersionstamp(tr, id, Versionstamp(), 0);
			BulkLoadMetadata::infoMap().set(tr, id, info);

			wait(BulkLoadMetadata::rangeMap().updateRange(tr, physicalRange.begin, physicalRange.end, id, true));

			wait(tr->commit());

			if (BULKLOAD_DEBUG) {
				fmt::print("StartBulkLoad {0}: complete\n", id.printable());
			}

			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

Future<Void> DatabaseContext::startBulkLoadInternal(BulkLoadID id, KeyRange range, Optional<Reference<Tenant>> tenant) {
	return startBulkLoadInternalActor(Reference<DatabaseContext>::addRef(this), id, range, tenant);
}

ACTOR Future<Void> updateBulkLoadStateActor(Reference<DatabaseContext> db,
                                            BulkLoadID id,
                                            BulkLoadStatus::State desiredState,
                                            Optional<int64_t> tenantIdToVerify) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state KeyRange physicalRange;
	ASSERT(desiredState != BulkLoadStatus::State::UNKNOWN);
	if (BULKLOAD_DEBUG) {
		fmt::print("UpdateBulkLoadState {0}: {1}\n", id.printable(), static_cast<int>(desiredState));
	}
	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);

			if (BULKLOAD_DEBUG) {
				fmt::print("UpdateBulkLoadState {0}: get current\n", id.printable());
			}
			state Optional<BulkLoadStatus> currentStatus = wait(BulkLoadMetadata::statusMap().get(tr, id));
			if (!currentStatus.present()) {
				// idempotency
				if (desiredState == BulkLoadStatus::State::CLEANUP) {
					if (BULKLOAD_DEBUG) {
						fmt::print("UpdateBulkLoadState {0}: status missing on cleanup\n", id.printable());
					}
					return Void();
				}
				throw bulk_load_not_found();
			}

			if (tenantIdToVerify.present()) {
				Optional<BulkLoadInfo> currentInfo = wait(BulkLoadMetadata::infoMap().get(tr, id));
				if (currentInfo.get().tenantId != tenantIdToVerify.get()) {
					TraceEvent("UpdateBulkLoadStateTenantMismatch")
					    .detail("BulkLoadID", id)
					    .detail("InfoTenant", currentInfo.get().tenantId)
					    .detail("UserTenant", tenantIdToVerify.get());
					throw bulk_load_tenant_mismatch();
				}
				tenantIdToVerify = {};
			}

			auto st = currentStatus.get().state;

			// throw an error if invalid state transition, otherwise if current state already meets desired, return
			if (BULKLOAD_DEBUG) {
				fmt::print("UpdateBulkLoadState {0}: {1} -> {2}\n",
				           id.printable(),
				           static_cast<int>(st),
				           static_cast<int>(desiredState));
			}
			if (!checkValidBulkLoadStateTransition(st, desiredState)) {
				if (BULKLOAD_DEBUG) {
					fmt::print("UpdateBulkLoadState {0}: already in desired state\n", id.printable());
				}
				CODE_PROBE(true, "update bulk load state idempotent");
				return Void();
			}
			if (BULKLOAD_DEBUG) {
				fmt::print("UpdateBulkLoadState {0}: transition ok\n", id.printable());
			}

			if (desiredState == BulkLoadStatus::State::CLEANUP) {
				if (BULKLOAD_DEBUG) {
					fmt::print("UpdateBulkLoadState {0}: erasing on cleanup\n", id.printable());
				}
				BulkLoadMetadata::statusMap().erase(tr, id);
				BulkLoadMetadata::infoMap().erase(tr, id);
			} else {
				BulkLoadStatus newStatus = currentStatus.get();
				newStatus.state = desiredState;
				BulkLoadMetadata::statusMap().set(tr, id, newStatus);

				// if cleaning up after abort or completed, clean up other metadata since it is no longer active
				if (desiredState == BulkLoadStatus::State::DONE ||
				    desiredState == BulkLoadStatus::State::ABORTED_COMPLETE) {
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
					BulkLoadMetadata::versionMap().erase(tr, id);
				} else if (desiredState == BulkLoadStatus::State::STAGING_BLOB_DONE) {
					// clean up pointers once granules actually staged
					std::pair<BulkLoadMetadata::FilePointerKey, BulkLoadMetadata::FilePointerKey> idRange =
					    BulkLoadMetadata::allFilePointersForLoadRange(id);
					BulkLoadMetadata::createdFilePointersMap().erase(tr, idRange.first, idRange.second);
					BulkLoadMetadata::writtenFilePointersMap().erase(tr, idRange.first, idRange.second);
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

Future<Void> DatabaseContext::updateBulkLoadState(BulkLoadID id,
                                                  BulkLoadStatus::State desiredState,
                                                  Optional<int64_t> tenantIdToVerify) {
	// right now we don't need tenant, we will likely need it for authz though
	return updateBulkLoadStateActor(Reference<DatabaseContext>::addRef(this), id, desiredState, tenantIdToVerify);
}

ACTOR Future<Void> waitBulkLoadStateAtLeastActor(Reference<DatabaseContext> db,
                                                 BulkLoadID id,
                                                 BulkLoadStatus::State desiredState,
                                                 Optional<int64_t> tenantIdToVerify) {
	ASSERT(desiredState != BulkLoadStatus::State::UNKNOWN);
	state Database cx(db);
	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				SystemDBWriteLockedNow(db)->setOptions(tr);

				Optional<BulkLoadStatus> status = wait(BulkLoadMetadata::statusMap().get(tr, id));

				if (!status.present()) {
					CODE_PROBE(true, "bulk load cleaned up while waiting for state at least");
					if (desiredState == BulkLoadStatus::State::CLEANUP) {
						return Void();
					}
					throw bulk_load_not_found();
				}
				state BulkLoadStatus::State st = status.get().state;
				if (st == BulkLoadStatus::State::ABORTED_COMPLETE || st == BulkLoadStatus::State::ABORTED) {
					if (desiredState != BulkLoadStatus::State::ABORTED &&
					    desiredState != BulkLoadStatus::State::ABORTED_COMPLETE) {
						throw bulk_load_aborted();
					}
				}

				if (tenantIdToVerify.present()) {
					Optional<BulkLoadInfo> currentInfo = wait(BulkLoadMetadata::infoMap().get(tr, id));
					if (currentInfo.get().tenantId != tenantIdToVerify.get()) {
						TraceEvent("WaitBulkLoadStateAtLeastTenantMismatch")
						    .detail("BulkLoadID", id)
						    .detail("InfoTenant", currentInfo.get().tenantId)
						    .detail("UserTenant", tenantIdToVerify.get());
						throw bulk_load_tenant_mismatch();
					}
					tenantIdToVerify = {};
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

ACTOR Future<Void> cleanupBulkLoadInternalActor(Reference<DatabaseContext> db,
                                                BulkLoadID id,
                                                Optional<int64_t> tenantIdToVerify) {
	// FIXME: could merge this into 1 transaction somehow if desired, either with custom code or just making
	// updateBulkLoadState ABORTED->ABORTED_COMPLETE handle this case?
	if (BULKLOAD_DEBUG) {
		fmt::print("CleanUpBulkLoad {0}: start\n", id.printable());
	}
	try {
		BulkLoadStatus startStatus = wait(getBulkLoadStatusActor(db, id));
		if (BULKLOAD_DEBUG) {
			fmt::print("CleanUpBulkLoad {0}: status {1}\n", id.printable(), static_cast<int>(startStatus.state));
		}
		if (startStatus.state == BulkLoadStatus::State::ABORTED) {
			if (BULKLOAD_DEBUG) {
				fmt::print("CleanUpBulkLoad {0}: waiting abort complete\n", id.printable());
			}
			wait(waitBulkLoadStateAtLeastActor(db, id, BulkLoadStatus::State::ABORTED_COMPLETE, tenantIdToVerify));
		}

	} catch (Error& e) {
		if (e.code() == error_code_bulk_load_not_found) {
			// idempotent
			return Void();
		}
		throw e;
	}

	// if bulk load is aborted, wait for internal server cleanup to happen before client can do final clean up

	if (BULKLOAD_DEBUG) {
		fmt::print("CleanUpBulkLoad {0}: finalizing\n", id.printable());
	}
	wait(updateBulkLoadStateActor(db, id, BulkLoadStatus::State::CLEANUP, tenantIdToVerify));
	if (BULKLOAD_DEBUG) {
		fmt::print("CleanUpBulkLoad {0}: done\n", id.printable());
	}
	return Void();
}

Future<Void> DatabaseContext::cleanupBulkLoadInternal(BulkLoadID id, Optional<int64_t> tenantIdToVerify) {
	// right now we don't need tenant, we will likely need it for authz though
	return cleanupBulkLoadInternalActor(Reference<DatabaseContext>::addRef(this), id, tenantIdToVerify);
}

Future<Void> DatabaseContext::waitBulkLoadStateAtLeast(BulkLoadID id,
                                                       BulkLoadStatus::State desiredState,
                                                       Optional<int64_t> tenantIdToVerify) {
	return waitBulkLoadStateAtLeastActor(Reference<DatabaseContext>::addRef(this), id, desiredState, tenantIdToVerify);
}

ACTOR Future<Standalone<VectorRef<BulkLoadIDRef>>> listBulkLoadsActor(Reference<DatabaseContext> db, bool onlyActive) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state Standalone<VectorRef<BulkLoadIDRef>> values;
	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);
			values = Standalone<VectorRef<BulkLoadIDRef>>();
			BulkLoadMetadata::StatusMap::RangeResultType result =
			    wait(BulkLoadMetadata::statusMap().getRange(tr, {}, {}, CLIENT_KNOBS->TOO_MANY)); // TODO
			ASSERT(!result.more);
			for (auto& it : result.results) {
				if (!onlyActive || (it.second.state != BulkLoadStatus::State::ABORTED &&
				                    it.second.state != BulkLoadStatus::State::ABORTED_COMPLETE &&
				                    it.second.state != BulkLoadStatus::State::DONE)) {

					values.push_back_deep(values.arena(), it.first);
				}
			}

			return values;

		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

Future<Standalone<VectorRef<BulkLoadIDRef>>> DatabaseContext::listBulkLoads(bool onlyActive,
                                                                            Optional<Reference<Tenant>> tenant) {
	// FIXME: use/limit to tenant
	return listBulkLoadsActor(Reference<DatabaseContext>::addRef(this), onlyActive);
}

Future<BulkLoadStatus> DatabaseContext::getBulkLoadStatus(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	return getBulkLoadStatusActor(Reference<DatabaseContext>::addRef(this), id);
}

ACTOR Future<Void> cleanupAbortedBulkLoadActor(Reference<DatabaseContext> db, BulkLoadID id) {
	BulkLoadStatus startStatus = wait(getBulkLoadStatusActor(db, id));

	// ensure load was in aborted state before starting cleanup
	if (!checkValidBulkLoadStateTransition(startStatus.state, BulkLoadStatus::State::ABORTED_COMPLETE)) {
		if (BULKLOAD_DEBUG) {
			fmt::print("CleanupAbortedBulkLoadActor {0}: already cleaned up\n", id.printable());
		}
		CODE_PROBE(true, "cleanup aborted bulk load already done");
		return Void();
	}

	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	// TODO: implement actual cleanup! Must be idempotent to handle retries/multiple concurrent cleanup callers
	// remaining: actually delete blob files, purge hybrid data, clear range, unblobbify/clean up purged range
	loop {
		try {
			// clean up file pointers if deleted in staging phase
			SystemDBWriteLockedNow(db)->setOptions(tr);
			auto idRange = BulkLoadMetadata::allFilePointersForLoadRange(id);
			BulkLoadMetadata::createdFilePointersMap().erase(tr, idRange.first, idRange.second);
			BulkLoadMetadata::writtenFilePointersMap().erase(tr, idRange.first, idRange.second);

			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	// change bulk load state to ABORTED_COMPLETE
	wait(updateBulkLoadStateActor(db, id, BulkLoadStatus::State::ABORTED_COMPLETE, {}));

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

ACTOR Future<Standalone<VectorRef<BulkLoadFileToWriteRef>>> getBulkLoadFilesToWriteActor(
    Reference<DatabaseContext> db,
    BulkLoadID id,
    int count,
    Optional<Reference<Tenant>> tenant) {
	BulkLoadFilesToWriteRequest req;
	req.arena.dependsOn(id.arena());
	req.id = id;
	req.count = count;
	BulkLoadFilesToWriteReply reply =
	    wait(doBulkLoadRequest(db, tenant, req, &BulkLoaderInterface::filesToWriteRequest));
	return Standalone<VectorRef<BulkLoadFileToWriteRef>>(reply.filePointers, reply.arena);
}

Future<Standalone<VectorRef<BulkLoadFileToWriteRef>>>
DatabaseContext::getBulkLoadFilesToWrite(BulkLoadID id, int count, Optional<Reference<Tenant>> tenant) {
	if (count <= 0) {
		throw unsupported_operation();
	}
	if (count >= CLIENT_KNOBS->BULK_LOAD_FILE_POINTER_TOO_MANY) {
		TraceEvent(SevWarn, "GetBulkLoadFilesToWriteLimitExceeded")
		    .detail("Count", count)
		    .detail("Limit", CLIENT_KNOBS->BULK_LOAD_FILE_POINTER_TOO_MANY);
		throw unsupported_operation();
	}
	if (BULKLOAD_DEBUG) {
		fmt::print("GetBulkLoadFilesToWrite {0}: {1}\n", id.printable(), count);
	}
	return getBulkLoadFilesToWriteActor(Reference<DatabaseContext>::addRef(this), id, count, tenant);
}

Future<Void> DatabaseContext::markBulkLoadFilesWritten(BulkLoadID id,
                                                       Standalone<VectorRef<BulkLoadWrittenFileRef>> files,
                                                       Optional<Reference<Tenant>> tenant) {
	if (files.empty()) {
		return Void();
	}
	if (files.size() >= CLIENT_KNOBS->BULK_LOAD_FILE_POINTER_TOO_MANY) {
		TraceEvent(SevWarn, "MarkBulkLoadFilesWrittenLimitExceeded")
		    .detail("Count", files.size())
		    .detail("Limit", CLIENT_KNOBS->BULK_LOAD_FILE_POINTER_TOO_MANY);
		throw unsupported_operation();
	}
	BulkLoadFilesWrittenRequest req;
	req.arena.dependsOn(files.arena());
	req.arena.dependsOn(id.arena());
	req.id = id;
	req.filesWritten = files;
	if (BULKLOAD_DEBUG) {
		fmt::print("MarkBulkLoadFilesWritten {0}: {1}\n", id.printable(), files.size());
	}
	return doBulkLoadRequest(
	    Reference<DatabaseContext>::addRef(this), tenant, req, &BulkLoaderInterface::filesWrittenRequest);
}

// TSS stubs required by load balance, but these requests are not to storage servers
template <>
bool TSS_doCompare(const BulkLoadFilesToWriteReply& src, const BulkLoadFilesToWriteReply& tss) {
	ASSERT(false);
	return true;
}

template <>
const char* TSS_mismatchTraceName(const BulkLoadFilesToWriteRequest& req) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const BulkLoadFilesToWriteRequest& req,
                       const BulkLoadFilesToWriteReply& src,
                       const BulkLoadFilesToWriteReply& tss) {
	ASSERT(false);
}

template <>
void TSSMetrics::recordLatency(const BulkLoadFilesToWriteRequest& req, double ssLatency, double tssLatency) {}

template <>
bool TSS_doCompare(const Void& src, const Void& tss) {
	ASSERT(false);
	return true;
}

template <>
const char* TSS_mismatchTraceName(const BulkLoadFilesWrittenRequest& req) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event, const BulkLoadFilesWrittenRequest& req, const Void& src, const Void& tss) {
	ASSERT(false);
}

template <>
void TSSMetrics::recordLatency(const BulkLoadFilesWrittenRequest& req, double ssLatency, double tssLatency) {}

template <>
const char* TSS_mismatchTraceName(const BulkLoadStartRequest& req) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event, const BulkLoadStartRequest& req, const Void& src, const Void& tss) {
	ASSERT(false);
}

template <>
void TSSMetrics::recordLatency(const BulkLoadStartRequest& req, double ssLatency, double tssLatency) {}

template <>
const char* TSS_mismatchTraceName(const BulkLoadStateChangeRequest& req) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event, const BulkLoadStateChangeRequest& req, const Void& src, const Void& tss) {
	ASSERT(false);
}

template <>
void TSSMetrics::recordLatency(const BulkLoadStateChangeRequest& req, double ssLatency, double tssLatency) {}

template <>
const char* TSS_mismatchTraceName(const BulkLoadWaitCompleteRequest& req) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event, const BulkLoadWaitCompleteRequest& req, const Void& src, const Void& tss) {
	ASSERT(false);
}

template <>
void TSSMetrics::recordLatency(const BulkLoadWaitCompleteRequest& req, double ssLatency, double tssLatency) {}