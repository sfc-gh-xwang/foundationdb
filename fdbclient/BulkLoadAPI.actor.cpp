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
#include "fdbclient/RangeLockAPI.actor.h"
#include "fdbclient/BulkLoad.h"

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
		if (!oldLoaderIDs.contains(newLoaders->getId(i))) {
			return true;
		}
	}
	return false;
}

bool requestCanRetry(const Error& e) {
	switch (e.code()) {
	case error_code_bulk_load_cannot_validate_file:
		return true;
	default:
		return false;
	};
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
			if (requestCanRetry(e)) {
				CODE_PROBE(true, "Bulk load request retrying on retryable error");
				// FIXME: add exponential backoff specifically in error case
				wait(delayJittered(1.0));
				// clear loaders to make sure we get new ones and restart request
				bulkLoaders.clear();
			} else {
				// FIXME: any errors to retry?
				TraceEvent("BulkLoadRequestError").errorUnsuppressed(e);
				throw e;
			}
		}
	}
}

BulkLoadID newBulkLoadID() {
	// for now just generate UID and convert to string - we assume no 128-bit UID collisions
	UID randomUID = deterministicRandom()->randomUniqueID();
	// TODO this is the same as granuleIDToCFKey - make common function?
	BinaryWriter wr(Unversioned());
	wr << randomUID;
	fmt::print("New Bulk Load Id : {}\n", BinaryReader::fromStringRef<UID>(wr.toValue(), Unversioned()).toString());
	return wr.toValue();
}

UID bulkLoadIdToUID(BulkLoadID const& id) {
	return BinaryReader::fromStringRef<UID>(id, Unversioned());
}

ACTOR Future<Version> doGRV(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			Version readVersion = wait(tr.getReadVersion());
			return readVersion;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<BulkLoadID> startBulkLoadExternalActor(Reference<DatabaseContext> db,
                                                    KeyRange range,
                                                    Optional<Reference<Tenant>> tenant) {
	// choose bulk load id on client side to guarantee idempotency for retransmits/duplication, and to not start extra
	// random bulk loads
	if (!CLIENT_KNOBS->ALLOW_BULK_LOADS) {
		throw unsupported_operation();
	}
	state BulkLoadID id = newBulkLoadID();
	loop {
		try {
			Version readVersion = wait(doGRV(Database(db)));
			BulkLoadStartRequest req;
			req.arena.dependsOn(range.arena());
			req.arena.dependsOn(id.arena());
			req.id = id;
			req.keyRange = range;
			req.startReadVersion = readVersion;
			if (BULKLOAD_DEBUG) {
				fmt::print("StartBulkLoadExternal {0}: begin [{1} - {2})\n",
				           id.printable(),
				           req.keyRange.begin.printable(),
				           req.keyRange.end.printable());
			}
			wait(success(doBulkLoadRequest(db, tenant, req, &BulkLoaderInterface::startRequest)));
			if (BULKLOAD_DEBUG) {
				fmt::print("StartBulkLoadExternal {0}: complete\n", id.printable());
			}
			return id;
		} catch (Error& e) {
			if (e.code() == error_code_range_lock_limit_exceeded) {
				CODE_PROBE(true, "Bulk load pending because too many range locks");
				wait(delayJittered(10.0));
			} else if (e.code() == error_code_bulk_load_create_too_old) {
				CODE_PROBE(true, "start bulk load create too old", probe::decoration::rare);
				wait(delayJittered(1.0));
			} else {
				throw e;
			}
			if (BULKLOAD_DEBUG) {
				fmt::print("StartBulkLoadExternal {0}: retrying \n", id.printable());
			}
		}
	}
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
	if (BULKLOAD_DEBUG) {
		fmt::print("AllFilesRegisteredExternal {0}: begin\n", id.printable());
	}
	return doBulkLoadRequest(
	    Reference<DatabaseContext>::addRef(this), tenant, req, &BulkLoaderInterface::stateChangeRequest);
}

Future<Void> DatabaseContext::abortBulkLoad(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	BulkLoadStateChangeRequest req;
	req.arena.dependsOn(id.arena());
	req.id = id;
	req.desired = BulkLoadStatus::State::ABORTED;
	if (BULKLOAD_DEBUG) {
		fmt::print("AbortExternal {0}: begin\n", id.printable());
	}
	return doBulkLoadRequest(
	    Reference<DatabaseContext>::addRef(this), tenant, req, &BulkLoaderInterface::stateChangeRequest);
}

Future<Void> DatabaseContext::cleanupBulkLoad(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	BulkLoadStateChangeRequest req;
	req.arena.dependsOn(id.arena());
	req.id = id;
	req.desired = BulkLoadStatus::State::CLEANUP;
	if (BULKLOAD_DEBUG) {
		fmt::print("CleanupExternal {0}: begin\n", id.printable());
	}
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

std::pair<BulkLoadIDKey, BulkLoadIDKey> getBulkLoadTenantRange(int64_t tenantId) {
	BulkLoadIDKey startKey = { tenantId, ""_sr };
	BulkLoadIDKey endKey = { tenantId + 1, ""_sr };
	return std::make_pair(startKey, endKey);
}

ACTOR Future<BulkLoadIDKey> getBulkLoadKey(Optional<Reference<Tenant>> tenant, BulkLoadID id) {
	state BulkLoadIDKey bulkLoadKey = { TenantInfo::INVALID_TENANT, id };
	if (tenant.present()) {
		wait(tenant.get()->ready());
		bulkLoadKey = { tenant.get()->id(), id };
	}
	return bulkLoadKey;
}

ACTOR Future<Void> startBulkLoadInternalActor(Reference<DatabaseContext> db,
                                              BulkLoadID id,
                                              KeyRange range,
                                              Version maxReadVersion,
                                              Optional<Reference<Tenant>> tenant) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state BulkLoadInfo info(range);
	state KeyRange physicalRange = range;
	state BulkLoadIDKey idKey = { TenantInfo::INVALID_TENANT, id };

	if (!CLIENT_KNOBS->ALLOW_BULK_LOADS) {
		throw unsupported_operation();
	}

	if (BULKLOAD_DEBUG) {
		fmt::print("StartBulkLoad {0}: begin\n", id.printable());
	}

	if (tenant.present()) {
		wait(tenant.get()->ready());
		physicalRange = range.withPrefix(tenant.get()->prefix());
		idKey = { tenant.get()->id(), id };
	}

	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);

			if (BULKLOAD_DEBUG) {
				fmt::print("StartBulkLoad {0}: loopstart\n", id.printable());
			}

			// explicitly check info map to handle idempotency/retransmits
			Optional<BulkLoadInfo> currentInfo = wait(BulkLoadMetadata::infoMap().get(tr, idKey));
			if (currentInfo.present()) {
				if (currentInfo.get().keyRange != info.keyRange) {
					throw bulk_load_range_conflict();
				}
				return Void();
			}

			// reject with error if read version > limit version (which is client's read version + X)
			if (tr->getReadVersion().get() >= maxReadVersion) {
				CODE_PROBE(true, "bulk load create rejected due to read version too old");
				throw bulk_load_create_too_old();
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

			// acquire range lock after check the range is empty
			wait(RangeLockAPI::lockRangesTransaction(tr, physicalRange.begin, physicalRange.end, bulkLoadIdToUID(id)));

			BulkLoadMetadata::statusMap().set(tr, idKey, BulkLoadStatus(BulkLoadStatus::State::STARTED));
			BulkLoadMetadata::versionMap().setVersionstamp(tr, idKey, Versionstamp(), 0);
			BulkLoadMetadata::infoMap().set(tr, idKey, info);

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

Future<Void> DatabaseContext::startBulkLoadInternal(BulkLoadID id,
                                                    KeyRange range,
                                                    Version maxReadVersion,
                                                    Optional<Reference<Tenant>> tenant) {
	return startBulkLoadInternalActor(Reference<DatabaseContext>::addRef(this), id, range, maxReadVersion, tenant);
}

ACTOR Future<Void> updateBulkLoadStateActor(Reference<DatabaseContext> db,
                                            BulkLoadID id,
                                            BulkLoadStatus::State desiredState,
                                            Optional<Reference<Tenant>> tenant) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state KeyRange physicalRange;
	ASSERT(desiredState != BulkLoadStatus::State::UNKNOWN);
	if (BULKLOAD_DEBUG) {
		fmt::print("UpdateBulkLoadState {0}: {1}\n", id.printable(), static_cast<int>(desiredState));
	}
	state BulkLoadIDKey bulkLoadKey = wait(getBulkLoadKey(tenant, id));
	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);

			if (BULKLOAD_DEBUG) {
				fmt::print("UpdateBulkLoadState {0}: get current\n", id.printable());
			}
			Optional<BulkLoadStatus> currentStatus = wait(BulkLoadMetadata::statusMap().get(tr, bulkLoadKey));
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

				// handle cleanup races and retransmits with starting a bulk load
				Optional<Versionstamp> v = wait(BulkLoadMetadata::versionMap().get(tr, bulkLoadKey));
				ASSERT(v.present());
				Version minReadVersion = v.get().version + CLIENT_KNOBS->BULK_LOAD_MAX_CREATE_VERSION_DIFFERENCE;
				// ensure we wait until any previous start bulk loads for the same id are guaranteed to have completed
				// or failed
				if (tr->getReadVersion().get() < minReadVersion) {
					CODE_PROBE(true,
					           "bulk load cleanup waiting to avoid conflicts with concurrent starts",
					           probe::decoration::rare);
					if (BULKLOAD_DEBUG) {
						fmt::print("UpdateBulkLoadState {0}: cleanup sleeping to avoid start conflicts\n",
						           id.printable());
					}
					int64_t versionDiff = minReadVersion - tr->getReadVersion().get();
					double timeToSleep = 1.0 + (1.0 * versionDiff) / CLIENT_KNOBS->VERSIONS_PER_SECOND;
					wait(delay(timeToSleep));
					// just for flow control
					throw bulk_load_create_too_old();
				}

				BulkLoadMetadata::statusMap().erase(tr, bulkLoadKey);
				BulkLoadMetadata::infoMap().erase(tr, bulkLoadKey);
				BulkLoadMetadata::versionMap().erase(tr, bulkLoadKey);
			} else {
				BulkLoadStatus newStatus = currentStatus.get();
				newStatus.state = desiredState;
				BulkLoadMetadata::statusMap().set(tr, bulkLoadKey, newStatus);

				// if cleaning up after abort or completed, clean up other metadata since it is no longer active
				if (desiredState == BulkLoadStatus::State::DONE ||
				    desiredState == BulkLoadStatus::State::ABORTED_COMPLETE) {
					// load bulk load info to get range for load
					Optional<BulkLoadInfo> info = wait(BulkLoadMetadata::infoMap().get(tr, bulkLoadKey));
					ASSERT(info.present());

					physicalRange = info.get().keyRange;
					if (bulkLoadKey.first != TenantInfo::INVALID_TENANT) {
						physicalRange = physicalRange.withPrefix(TenantAPI::idToPrefix(bulkLoadKey.first));
					}

					// fmt::print("UpdateBulkLoadState {0}: Checking range [{1} - {2}) in map\n", id.printable(),
					// physicalRange.begin.printable(), physicalRange.end.printable());

					// load and check range map for sanity check/validation and to get read conflict range
					Optional<BulkLoadMetadata::RangeMap::RangeValue> range =
					    wait(BulkLoadMetadata::rangeMap().getRangeForKey(tr, physicalRange.begin));
					ASSERT(range.present());
					ASSERT(range.get().range.begin == physicalRange.begin &&
					       range.get().range.end == physicalRange.end && range.get().value == id);

					wait(RangeLockAPI::unlockRangesTransaction(
					    tr, physicalRange.begin, physicalRange.end, bulkLoadIdToUID(id)));

					// set range to inactive when complete and clear info
					// FIXME: any way to make this coalescing?
					wait(BulkLoadMetadata::rangeMap().updateRange(
					    tr, physicalRange.begin, physicalRange.end, ""_sr, true));
				} else if (desiredState == BulkLoadStatus::State::STAGING_BLOB_DONE) {
					// clean up pointers once granules actually staged
					// FIXME: convert these 3 lines into common function?
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
			if (BULKLOAD_DEBUG) {
				fmt::print("UpdateBulkLoadState {0}: error {1}\n", id.printable(), e.name());
			}
			// flow control for not committing and getting a new read version after sleeping for cleanup conflicts
			if (e.code() == error_code_bulk_load_create_too_old) {
				tr->reset();
			} else {
				wait(tr->onError(e));
			}
		}
	}
}

Future<Void> DatabaseContext::updateBulkLoadState(BulkLoadID id,
                                                  BulkLoadStatus::State desiredState,
                                                  Optional<Reference<Tenant>> tenant) {
	// right now we don't need tenant, we will likely need it for authz though
	return updateBulkLoadStateActor(Reference<DatabaseContext>::addRef(this), id, desiredState, tenant);
}

ACTOR Future<Void> waitBulkLoadStateAtLeastActor(Reference<DatabaseContext> db,
                                                 BulkLoadID id,
                                                 BulkLoadStatus::State desiredState,
                                                 Optional<Reference<Tenant>> tenant) {
	ASSERT(desiredState != BulkLoadStatus::State::UNKNOWN);
	state Database cx(db);
	state BulkLoadIDKey bulkLoadKey = wait(getBulkLoadKey(tenant, id));
	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				SystemDBWriteLockedNow(db)->setOptions(tr);

				Optional<BulkLoadStatus> status = wait(BulkLoadMetadata::statusMap().get(tr, bulkLoadKey));

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

				if (st >= desiredState) {
					return Void();
				}

				state Future<Void> onChange = BulkLoadMetadata::statusMap().watch(tr, bulkLoadKey);
				wait(tr->commit());
				wait(onChange);
				tr->reset();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}
}

ACTOR Future<Void> cleanupBulkLoadInternalActor(Reference<DatabaseContext> db,
                                                BulkLoadID id,
                                                Optional<Reference<Tenant>> tenant) {
	// FIXME: could merge this into 1 transaction somehow if desired, either with custom code or just making
	// updateBulkLoadState ABORTED->ABORTED_COMPLETE handle this case?
	if (BULKLOAD_DEBUG) {
		fmt::print("CleanUpBulkLoad {0}: start\n", id.printable());
	}
	try {
		BulkLoadStatus startStatus = wait(db->getBulkLoadStatusInternal(id, tenant));
		if (BULKLOAD_DEBUG) {
			fmt::print("CleanUpBulkLoad {0}: status {1}\n", id.printable(), static_cast<int>(startStatus.state));
		}
		if (startStatus.state == BulkLoadStatus::State::ABORTED) {
			if (BULKLOAD_DEBUG) {
				fmt::print("CleanUpBulkLoad {0}: waiting abort complete\n", id.printable());
			}
			wait(waitBulkLoadStateAtLeastActor(db, id, BulkLoadStatus::State::ABORTED_COMPLETE, tenant));
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
	wait(updateBulkLoadStateActor(db, id, BulkLoadStatus::State::CLEANUP, tenant));
	if (BULKLOAD_DEBUG) {
		fmt::print("CleanUpBulkLoad {0}: done\n", id.printable());
	}
	return Void();
}

Future<Void> DatabaseContext::cleanupBulkLoadInternal(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	// right now we don't need tenant, we will likely need it for authz though
	return cleanupBulkLoadInternalActor(Reference<DatabaseContext>::addRef(this), id, tenant);
}

Future<Void> DatabaseContext::waitBulkLoadStateAtLeast(BulkLoadID id,
                                                       BulkLoadStatus::State desiredState,
                                                       Optional<Reference<Tenant>> tenant) {
	return waitBulkLoadStateAtLeastActor(Reference<DatabaseContext>::addRef(this), id, desiredState, tenant);
}

ACTOR Future<Standalone<VectorRef<BulkLoadIDRef>>> listBulkLoadsInternalActor(Reference<DatabaseContext> db,
                                                                              bool onlyActive,
                                                                              Optional<int64_t> tenantId) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state Standalone<VectorRef<BulkLoadIDRef>> values;

	if (BULKLOAD_DEBUG) {
		fmt::print("ListBulkLoads: begin\n");
	}

	state Optional<BulkLoadIDKey> startKey;
	state Optional<BulkLoadIDKey> endKey;
	if (tenantId.present()) {
		std::tie(startKey, endKey) = getBulkLoadTenantRange(tenantId.get());
	}
	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);

			if (BULKLOAD_DEBUG) {
				fmt::print("ListBulkLoads: get\n");
			}

			values = Standalone<VectorRef<BulkLoadIDRef>>();
			BulkLoadMetadata::StatusMap::RangeResultType result =
			    wait(BulkLoadMetadata::statusMap().getRange(tr, startKey, endKey, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!result.more);
			for (auto& it : result.results) {
				if (!onlyActive || (it.second.state != BulkLoadStatus::State::ABORTED &&
				                    it.second.state != BulkLoadStatus::State::ABORTED_COMPLETE &&
				                    it.second.state != BulkLoadStatus::State::DONE)) {
					values.push_back_deep(values.arena(), it.first.second);
				}
			}

			if (BULKLOAD_DEBUG) {
				fmt::print("ListBulkLoads: done ({0} result(s))\n", values.size());
			}

			return values;
		} catch (Error& e) {
			if (BULKLOAD_DEBUG) {
				fmt::print("ListBulkLoads: error {0}\n", e.name());
			}
			wait(tr->onError(e));
		}
	}
}

Future<Standalone<VectorRef<BulkLoadIDRef>>> DatabaseContext::listBulkLoadsInternal(bool onlyActive,
                                                                                    Optional<int64_t> tenantId) {
	return listBulkLoadsInternalActor(Reference<DatabaseContext>::addRef(this), onlyActive, tenantId);
}

ACTOR Future<Standalone<VectorRef<BulkLoadIDRef>>> listBulkLoadsExternalActor(Reference<DatabaseContext> db,
                                                                              bool onlyActive,
                                                                              Optional<Reference<Tenant>> tenant) {
	BulkLoadListRequest req;
	req.onlyActive = onlyActive;
	if (BULKLOAD_DEBUG) {
		fmt::print("ListExternal: begin\n");
	}
	BulkLoadListReply reply = wait(doBulkLoadRequest(db, tenant, req, &BulkLoaderInterface::listRequest));
	return Standalone<VectorRef<BulkLoadIDRef>>(reply.list, reply.arena);
}

Future<Standalone<VectorRef<BulkLoadIDRef>>> DatabaseContext::listBulkLoads(bool onlyActive,
                                                                            Optional<Reference<Tenant>> tenant) {
	return listBulkLoadsExternalActor(Reference<DatabaseContext>::addRef(this), onlyActive, tenant);
}

ACTOR Future<Standalone<VectorRef<BulkLoadIDRef>>> listAllBulkLoadsExternalActor(Reference<DatabaseContext> db,
                                                                                 bool onlyActive,
                                                                                 Optional<Reference<Tenant>> tenant) {
	BulkLoadListAllRequest req;
	req.onlyActive = onlyActive;
	if (BULKLOAD_DEBUG) {
		fmt::print("ListAllExternal: begin\n");
	}
	BulkLoadListReply reply = wait(doBulkLoadRequest(db, tenant, req, &BulkLoaderInterface::listAllRequest));
	return Standalone<VectorRef<BulkLoadIDRef>>(reply.list, reply.arena);
}

Future<Standalone<VectorRef<BulkLoadIDRef>>> DatabaseContext::listAllBulkLoads(bool onlyActive,
                                                                               Optional<Reference<Tenant>> tenant) {
	return listAllBulkLoadsExternalActor(Reference<DatabaseContext>::addRef(this), onlyActive, tenant);
}

ACTOR Future<BulkLoadStatus> getBulkLoadStatusInternalActor(Reference<DatabaseContext> db,
                                                            BulkLoadID id,
                                                            Optional<Reference<Tenant>> tenant) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state BulkLoadIDKey bulkLoadKey = wait(getBulkLoadKey(tenant, id));
	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);

			Optional<BulkLoadStatus> status = wait(BulkLoadMetadata::statusMap().get(tr, bulkLoadKey));
			if (!status.present()) {
				throw bulk_load_not_found();
			}
			return status.get();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

Future<BulkLoadStatus> DatabaseContext::getBulkLoadStatusInternal(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	return getBulkLoadStatusInternalActor(Reference<DatabaseContext>::addRef(this), id, tenant);
}

ACTOR Future<BulkLoadStatus> getBulkLoadStatusExternalActor(Reference<DatabaseContext> db,
                                                            BulkLoadID id,
                                                            Optional<Reference<Tenant>> tenant) {
	BulkLoadStatusRequest req;
	req.arena.dependsOn(id.arena());
	req.id = id;
	if (BULKLOAD_DEBUG) {
		fmt::print("StatusExternal {0}: begin\n", id.printable());
	}
	BulkLoadStatusReply reply = wait(doBulkLoadRequest(db, tenant, req, &BulkLoaderInterface::statusRequest));
	return reply.status;
}

Future<BulkLoadStatus> DatabaseContext::getBulkLoadStatus(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	return getBulkLoadStatusExternalActor(Reference<DatabaseContext>::addRef(this), id, tenant);
}

ACTOR Future<Version> getBulkLoadVersionActor(Reference<DatabaseContext> db,
                                              BulkLoadID id,
                                              Optional<Reference<Tenant>> tenant) {
	state Database cx(db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state BulkLoadIDKey bulkLoadKey = wait(getBulkLoadKey(tenant, id));
	loop {
		try {
			SystemDBWriteLockedNow(db)->setOptions(tr);

			Optional<Versionstamp> versionstamp = wait(BulkLoadMetadata::versionMap().get(tr, bulkLoadKey));
			if (!versionstamp.present()) {
				throw bulk_load_not_found();
			}
			return versionstamp.get().version;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

Future<Version> DatabaseContext::getBulkLoadVersion(BulkLoadID id, Optional<Reference<Tenant>> tenant) {
	return getBulkLoadVersionActor(Reference<DatabaseContext>::addRef(this), id, tenant);
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
bool TSS_doCompare(const BulkLoadListReply& src, const BulkLoadListReply& tss) {
	ASSERT(false);
	return true;
}

template <>
bool TSS_doCompare(const BulkLoadStatusReply& src, const BulkLoadStatusReply& tss) {
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
void TSS_traceMismatch(TraceEvent& event,
                       const BulkLoadListRequest& req,
                       const BulkLoadListReply& src,
                       const BulkLoadListReply& tss) {
	ASSERT(false);
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const BulkLoadListAllRequest& req,
                       const BulkLoadListReply& src,
                       const BulkLoadListReply& tss) {
	ASSERT(false);
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const BulkLoadStatusRequest& req,
                       const BulkLoadStatusReply& src,
                       const BulkLoadStatusReply& tss) {
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

template <>
const char* TSS_mismatchTraceName(const BulkLoadListRequest& req) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event, const BulkLoadListRequest& req, const Void& src, const Void& tss) {
	ASSERT(false);
}

template <>
void TSSMetrics::recordLatency(const BulkLoadListRequest& req, double ssLatency, double tssLatency) {}

template <>
const char* TSS_mismatchTraceName(const BulkLoadListAllRequest& req) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event, const BulkLoadListAllRequest& req, const Void& src, const Void& tss) {
	ASSERT(false);
}

template <>
void TSSMetrics::recordLatency(const BulkLoadListAllRequest& req, double ssLatency, double tssLatency) {}

template <>
const char* TSS_mismatchTraceName(const BulkLoadStatusRequest& req) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event, const BulkLoadStatusRequest& req, const Void& src, const Void& tss) {
	ASSERT(false);
}

template <>
void TSSMetrics::recordLatency(const BulkLoadStatusRequest& req, double ssLatency, double tssLatency) {}
