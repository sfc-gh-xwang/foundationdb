/*
 * BlobMigrator.actor.cpp
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
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <string>
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/Knobs.h"
#include "fdbserver/RestoreCommon.actor.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/StorageMetrics.actor.h"
#include "flow/CodeProbe.h"
#include "flow/Error.h"
#include "flow/network.h"
#include "flow/flow.h"
#include "flow/ActorCollection.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BulkLoadTracker.h"
#include "fdbclient/BulkLoadMetadata.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/BlobConnectionProvider.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BlobRestoreCommon.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/BlobMigratorInterface.h"
#include "fdbserver/Knobs.h"
#include "flow/genericactors.actor.h"
#include "fmt/core.h"

#include "flow/actorcompiler.h" // has to be last include

#define ENABLE_DEBUG_MG false

template <typename... T>
static inline void dprint(fmt::format_string<T...> fmt, T&&... args) {
	if (ENABLE_DEBUG_MG)
		fmt::print(fmt, std::forward<T>(args)...);
}

using GranuleSizeMap = KeyRangeMap<int64_t>;

// state for 1 bulk load in the blob migrator
struct BulkLoadMigratorState : ReferenceCounted<BulkLoadMigratorState>, NonCopyable {
	bool granuleSizesLoaded = false;
	GranuleSizeMap granuleSizes;
};

// BlobMigrator offers APIs to migrate data from blob storage to storage server. It implements a minimal set of
// StorageServerInterface APIs which are needed for DataDistributor to start data migration.
class BlobMigrator final : public NonCopyable, public ReferenceCounted<BlobMigrator>, public IStorageMetricsService {
public:
	BlobMigrator(BlobMigratorInterface interf, Reference<AsyncVar<ServerDBInfo> const> dbInfo)
	  : interf_(interf),
	    actors_(false),
	    dbInfo_(dbInfo) {
		db_ = openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True);
	}
	~BlobMigrator() {}

	// Start migration
	ACTOR static Future<Void> start(Reference<BlobMigrator> self) {
		wait(initialize(self));
		wait(serverLoop(self));
		return Void();
	}

private:
	// Initialize blob migrator to COPYING_DATA state.
	ACTOR static Future<Void> initialize(Reference<BlobMigrator> self) {
		state Reference<BlobRestoreController> controller = makeReference<BlobRestoreController>(self->db_, normalKeys);
		wait(store(self->isFullRestore_, BlobRestoreController::isRestoring(controller)));
		wait(BlobRestoreController::setLockOwner(controller, self->interf_.id()));
		if (!self->isFullRestore_) {
			BulkLoadTracker::Handler bulkLoadHandler = [self = self](const BulkLoadIDKey& id,
			                                                         const BulkLoadStatus::State& state) {
				return trackBulkLoad(self, id, state);
			};
			self->bulkLoadTracker_ = BulkLoadTracker(self->db_, bulkLoadHandler);
			self->addActor(self->bulkLoadTracker_.start());
			return Void();
		}

		loop {
			state BlobRestorePhase phase = wait(BlobRestoreController::currentPhase(controller));
			if (phase < BlobRestorePhase::LOADED_MANIFEST) {
				TraceEvent("BlobMigratorWaitingManifest", self->interf_.id()).detail("Phase", phase);
				wait(BlobRestoreController::onPhaseChange(controller, BlobRestorePhase::LOADED_MANIFEST));
				continue;
			}

			if (phase > BlobRestorePhase::COPIED_DATA) {
				CODE_PROBE(true, "Restart blob migrator after data copy");
				TraceEvent("BlobMigratorAlreadyCopied", self->interf_.id()).detail("Phase", phase);
				return Void();
			}

			auto db = SystemDBWriteLockedNow(self->db_.getReference());
			std::string url = wait(BlobGranuleRestoreConfig().manifestUrl().getD(db));
			Reference<BlobConnectionProvider> blobConn = BlobConnectionProvider::newBlobConnectionProvider(url);
			BlobGranuleRestoreVersionVector granules = wait(listBlobGranules(self->db_, self->dbInfo_, blobConn));
			self->blobGranules_ = granules;

			wait(buildGranuleSizesMap(self));

			TraceEvent("RestorableGranule", self->interf_.id()).detail("Size", self->blobGranules_.size());
			if (self->blobGranules_.empty()) {
				TraceEvent("EmptyBlobGranules", self->interf_.id()).log();
				CODE_PROBE(true, "Blob restore with no blob granules", probe::decoration::rare);
				wait(canRestore(self));
				wait(preloadApplyMutationsKeyVersionMap(self));
				wait(BlobRestoreController::setPhase(controller, APPLYING_MLOGS, self->interf_.id()));
				return Void();
			}

			// Start the data copy after manifest is loaded
			if (phase == BlobRestorePhase::LOADED_MANIFEST) {
				try {
					wait(canRestore(self));
					wait(preloadApplyMutationsKeyVersionMap(self));
					wait(prepare(self, normalKeys));
					wait(advanceVersion(self));
					wait(BlobRestoreController::setPhase(controller, COPYING_DATA, self->interf_.id()));
					self->addActor(logProgress(self));
					TraceEvent("BlobMigratorStartCopying", self->interf_.id()).log();
				} catch (Error& e) {
					TraceEvent("BlobMigratorStartCopyingError", self->interf_.id()).error(e);
					throw e;
				}
			} else if (phase == BlobRestorePhase::COPYING_DATA) {
				CODE_PROBE(true, "Restart blob migrator during data copy");
				try {
					wait(prepare(self, normalKeys));
					dprint("Replace storage server interface {}\n", self->interf_.ssi.id().toString());
					TraceEvent("ReplacedStorageInterface", self->interf_.id()).log();

					self->addActor(logProgress(self));
				} catch (Error& e) {
					TraceEvent("ReplacedStorageInterfaceError", self->interf_.id()).error(e);
					throw e;
				}
			} else if (phase == BlobRestorePhase::COPIED_DATA) {
				CODE_PROBE(true, "Restart blob migrator after data copy");
				self->addActor(logProgress(self));
			}
			return Void();
		}
	}

	// Prepare for data migration for given key range.
	ACTOR static Future<Void> prepare(Reference<BlobMigrator> self, KeyRangeRef keys) {
		state Future<ErrorOr<PrepareBlobRestoreReply>> replyFuture = Never();
		state Future<Void> dbInfoChange = Void();
		state Future<Void> delayTime = Void();
		state int retries = 0;
		state UID requestId;
		loop {
			choose {
				when(wait(dbInfoChange)) {
					// FIXME: don't redo request if DD id still the same
					if (self->dbInfo_->get().distributor.present()) {
						requestId = deterministicRandom()->randomUniqueID();
						replyFuture = timeout(self->dbInfo_->get().distributor.get().prepareBlobRestoreReq.tryGetReply(
						                          PrepareBlobRestoreRequest(requestId, self->interf_.ssi, keys)),
						                      SERVER_KNOBS->BLOB_MIGRATOR_PREPARE_TIMEOUT,
						                      ErrorOr<PrepareBlobRestoreReply>(timed_out()));
						dbInfoChange = Never();
						TraceEvent("BlobRestorePrepare", self->interf_.id())
						    .detail("State", "SendReq")
						    .detail("ReqId", requestId);
					} else {
						replyFuture = Never();
						dbInfoChange = self->dbInfo_->onChange();
						TraceEvent(SevWarn, "BlobRestorePrepare", self->interf_.id()).detail("State", "WaitDD");
					}
				}
				when(ErrorOr<PrepareBlobRestoreReply> reply = wait(replyFuture)) {
					if (reply.isError()) {
						TraceEvent("BlobRestorePrepare", self->interf_.id())
						    .error(reply.getError())
						    .detail("State", "Error")
						    .detail("ReqId", requestId);
						if (reply.getError().code() == error_code_restore_error) {
							throw restore_error();
						}
					} else if (reply.get().res == PrepareBlobRestoreReply::SUCCESS) {
						TraceEvent("BlobRestorePrepare", self->interf_.id())
						    .detail("State", "Success")
						    .detail("ReqId", requestId);
						return Void();
					} else {
						TraceEvent("BlobRestorePrepare", self->interf_.id())
						    .detail("State", "Failed")
						    .detail("ReqId", requestId)
						    .detail("Reply", reply.get().toString())
						    .detail("Retries", retries);
					}

					if (++retries > SERVER_KNOBS->BLOB_MIGRATOR_ERROR_RETRIES) {
						throw restore_error();
					}
					delayTime = delayJittered(10.0);
					dbInfoChange = Void();
				}
			}

			wait(delayTime);
		}
	}

	// Monitor migration progress periodically
	ACTOR static Future<Void> logProgress(Reference<BlobMigrator> self) {
		state Reference<BlobRestoreController> controller = makeReference<BlobRestoreController>(self->db_, normalKeys);
		loop {
			BlobRestorePhase phase = wait(BlobRestoreController::currentPhase(controller));
			if (phase > COPIED_DATA) {
				return Void();
			}
			bool done = wait(checkCopyProgress(self));
			if (done) {
				wait(BlobRestoreController::setPhase(controller, COPIED_DATA, self->interf_.id()));
				wait(waitForPendingDataMovements(self));
				wait(BlobRestoreController::setPhase(controller, APPLYING_MLOGS, self->interf_.id()));
				TraceEvent("BlobMigratorCopied", self->interf_.id()).log();
				return Void();
			}
			wait(delay(SERVER_KNOBS->BLOB_MIGRATOR_CHECK_INTERVAL));
		}
	}

	// Wait until all pending data movements are done. Data movement starts earlier may still potentially
	// read data from blob, which may cause race with applying mutation logs.
	// FIXME: make this not re-check ranges it already knows are complete!
	ACTOR static Future<Void> waitForPendingDataMovements(Reference<BlobMigrator> self) {
		loop {
			bool pending = wait(checkPendingDataMovements(self, normalKeys));
			TraceEvent("BlobMigratorCheckPendingMovement", self->interf_.id()).detail("Pending", pending);
			if (!pending)
				return Void();
			wait(delay(SERVER_KNOBS->BLOB_MIGRATOR_CHECK_INTERVAL));
		}
	}

	// Check if there is any pending data movement

	ACTOR static Future<bool> checkPendingDataMovements(Reference<BlobMigrator> self, KeyRange range) {
		state Transaction tr(self->db_);
		state Key begin = range.begin;

		loop {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				state RangeReadResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				state std::vector<UID> src;
				state std::vector<UID> dest;
				state UID srcId;
				state UID destId;
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
				while (begin < range.end) {
					state RangeResult keyServers = wait(krmGetRanges(&tr,
					                                                 keyServersPrefix,
					                                                 KeyRangeRef(begin, range.end),
					                                                 SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
					                                                 SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES));
					state int i = 0;
					for (; i < keyServers.size() - 1; ++i) {
						state KeyValueRef it = keyServers[i];
						decodeKeyServersValue(UIDtoTagMap, it.value, src, dest, srcId, destId);
						if (!dest.empty()) {
							return true;
						}
					}
					begin = keyServers.back().key;
				}
				return false;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Check key ranges that are copied. Return true if all ranges are done
	ACTOR static Future<bool> checkCopyProgress(Reference<BlobMigrator> self) {
		state Reference<BlobRestoreController> controller = makeReference<BlobRestoreController>(self->db_, normalKeys);
		state Transaction tr(self->db_);
		loop {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				// Get key ranges that are still owned by the migrator. Those ranges are
				// incompleted migrations
				state KeyRange currentKeys = normalKeys;
				state Key begin = normalKeys.begin;
				state RangeReadResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				state std::vector<UID> src;
				state std::vector<UID> dest;
				state UID srcId;
				state UID destId;
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
				state int64_t incompleted = 0;
				while (begin < normalKeys.end) {
					state RangeResult keyServers = wait(krmGetRanges(&tr,
					                                                 keyServersPrefix,
					                                                 KeyRangeRef(begin, normalKeys.end),
					                                                 SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
					                                                 SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES));
					state int i = 0;
					for (; i < keyServers.size() - 1; ++i) {
						state KeyValueRef it = keyServers[i];
						decodeKeyServersValue(UIDtoTagMap, it.value, src, dest, srcId, destId);

						if (std::find_if(src.begin(), src.end(), BlobMigratorInterface::isBlobMigrator) == src.end() &&
						    dest.empty()) {
							continue; // not owned by blob migrator
						}

						state KeyRangeRef range(it.key, keyServers[i + 1].key);
						int64_t bytes = sizeInBytes(self, range);
						if (bytes == 0) {
							// set a non-zero value even if it's an empty range, so that we don't
							// move to next phase early
							bytes = 1;
						}
						dprint("   incompleted {}, size: {}\n", range.toString(), bytes);

						incompleted += bytes;
					}
					begin = keyServers.end()[-1].key;
				}

				int64_t total = sizeInBytes(self);
				incompleted = std::min(incompleted, total);
				state int progress = (total - incompleted) * 100 / total;
				state bool done = incompleted == 0;
				dprint("Migration progress :{}%. done {}\n", progress, done);
				TraceEvent("BlobMigratorProgress", self->interf_.id()).detail("Progress", progress);
				wait(BlobRestoreController::setProgress(controller, progress, self->interf_.id()));
				return done;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Advance version, so that future commits will have a larger version than the restored data
	ACTOR static Future<Void> advanceVersion(Reference<BlobMigrator> self) {
		state Transaction tr(self->db_);
		loop {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				Version current = wait(tr.getRawReadVersion());
				Version expected = maxVersion(self);
				if (current <= expected) {
					tr.set(minRequiredCommitVersionKey, BinaryWriter::toValue(expected + 1, Unversioned()));
					dprint("Advance version from {} to {}\n", current, expected);
					TraceEvent("AdvanceVersion", self->interf_.id()).detail("From", current).detail("To", expected);
					wait(tr.commit());
				} else {
					dprint("Skip advancing version {}. current {}\n", expected, current);
					TraceEvent("SkipAdvanceVersion", self->interf_.id()).detail("From", current).detail("To", expected);
				}
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Check if we need to apply mutation logs. If all granules have data up to targetVersion, we don't need to
	// apply mutation logs
	static bool needApplyLogs(Reference<BlobMigrator> self, Version targetVersion) {
		for (auto& granule : self->blobGranules_) {
			if (granule.version < targetVersion) {
				// at least one granule doesn't have data up to target version, so we'll need to apply mutation logs
				return true;
			}
		}
		return false;
	}

	// SOMEDAY: i wonder if it's faster to build this in random order or sequential?
	ACTOR static Future<Void> buildGranuleSizesMap(Reference<BlobMigrator> self) {
		self->granuleSizes_.insert(normalKeys, 0);
		state int i;
		for (i = 0; i < self->blobGranules_.size(); i++) {
			auto& it = self->blobGranules_[i];
			self->granuleSizes_.insert(it.keyRange, it.sizeInBytes);
			if (i % 1000 == 999) {
				wait(yield());
			}
		}
		return Void();
	}

	// Check if we can restore based on blob granule data and mutation logs.
	ACTOR static Future<Void> canRestore(Reference<BlobMigrator> self) {
		try {
			// check last version in mutation logs
			state std::string mlogsUrl = wait(
			    BlobGranuleRestoreConfig().mutationLogsUrl().getD(SystemDBWriteLockedNow(self->db_.getReference())));

			self->mlogsUrl_ = KeyRef(mlogsUrl);
			state Reference<IBackupContainer> bc = IBackupContainer::openContainer(mlogsUrl, {}, {});
			state double beginTs = now();
			BackupDescription desc = wait(bc->describeBackup(true));
			TraceEvent("DescribeBackupLatency", self->interf_.id()).detail("Seconds", now() - beginTs);

			if (!desc.contiguousLogEnd.present()) {
				TraceEvent("InvalidMutationLogs", self->interf_.id()).log();
				throw blob_restore_missing_logs();
			}
			if (!desc.minLogBegin.present()) {
				TraceEvent("InvalidMutationLogs", self->interf_.id()).log();
				throw blob_restore_missing_logs();
			}

			state Version minLogVersion = desc.minLogBegin.get();
			state Version maxLogVersion = desc.contiguousLogEnd.get() - 1;
			Reference<BlobRestoreController> restoreController =
			    makeReference<BlobRestoreController>(self->db_, normalKeys);
			state Version targetVersion =
			    wait(BlobRestoreController::getTargetVersion(restoreController, maxLogVersion));
			self->targetVersion_ = targetVersion;
			if (targetVersion < maxLogVersion) {
				if (!needApplyLogs(self, targetVersion)) {
					TraceEvent("SkipMutationLogs", self->interf_.id()).detail("TargetVersion", targetVersion);
					dprint("Skip mutation logs as all granules are at version {}\n", targetVersion);
					return Void();
				}
			}

			if (targetVersion < minLogVersion) {
				TraceEvent("MissingMutationLogs", self->interf_.id())
				    .detail("MinLogVersion", minLogVersion)
				    .detail("TargetVersion", maxLogVersion);
				throw blob_restore_missing_logs();
			}

			if (targetVersion > maxLogVersion) {
				TraceEvent("SkipTargetVersion", self->interf_.id())
				    .detail("MaxLogVersion", maxLogVersion)
				    .detail("TargetVersion", targetVersion);
			}

			// restore to target version
			self->mlogRestoreRanges_.clear();
			self->mlogRestoreBeginVersions_.clear();
			for (auto& granule : self->blobGranules_) {
				if (granule.version < minLogVersion) {
					TraceEvent("MissingMutationLogs", self->interf_.id())
					    .detail("Granule", granule.granuleID)
					    .detail("GranuleVersion", granule.version)
					    .detail("MinLogVersion", minLogVersion)
					    .detail("MaxLogVersion", maxLogVersion)
					    .detail("TargetVersion", targetVersion);
					dprint("Granule {} version {} is out of the log range {} - {}. restore target version {}\n",
					       granule.granuleID.toString(),
					       granule.version,
					       minLogVersion,
					       maxLogVersion,
					       targetVersion);
					throw blob_restore_missing_logs();
				}

				if (granule.version > maxLogVersion) {
					TraceEvent("GranuleHasMoreData", self->interf_.id())
					    .detail("Granule", granule.granuleID)
					    .detail("GranuleVersion", granule.version)
					    .detail("MinLogVersion", minLogVersion)
					    .detail("MaxLogVersion", maxLogVersion)
					    .detail("TargetVersion", targetVersion);
				}

				// no need to apply mutation logs if granule is already on that version
				if (granule.version < targetVersion) {
					self->mlogRestoreRanges_.push_back(self->mlogRestoreRanges_.arena(), granule.keyRange);
					// Blob granule ends at granule.version(inclusive), so we need to apply mutation logs
					// after granule.version(exclusive).
					self->mlogRestoreBeginVersions_.push_back(self->mlogRestoreBeginVersions_.arena(), granule.version);
					TraceEvent("ApplyMutationLogVersion", self->interf_.id())
					    .detail("GID", granule.granuleID)
					    .detail("Ver", granule.version);
				} else {
					TraceEvent("GranuleHasMoreData", self->interf_.id())
					    .detail("Granule", granule.granuleID)
					    .detail("GranuleVersion", granule.version)
					    .detail("TargetVersion", targetVersion);
				}
			}

			// Apply muation logs for system backup ranges after manifest version
			Version manifestVersion = wait(getManifestVersion(self->db_));
			for (auto& range : getSystemBackupRanges()) {
				self->mlogRestoreRanges_.push_back(self->mlogRestoreRanges_.arena(), range);
				self->mlogRestoreBeginVersions_.push_back(self->mlogRestoreBeginVersions_.arena(), manifestVersion);
			}

			Optional<RestorableFileSet> restoreSet = wait(
			    bc->getRestoreSet(maxLogVersion, self->mlogRestoreRanges_, OnlyApplyMutationLogs::True, minLogVersion));
			if (!restoreSet.present()) {
				TraceEvent("InvalidMutationLogs", self->interf_.id())
				    .detail("MinLogVersion", minLogVersion)
				    .detail("MaxLogVersion", maxLogVersion);
				throw blob_restore_corrupted_logs();
			}
			TraceEvent("BlobMigratorCanRestore", self->interf_.id()).log();
		} catch (Error& e) {
			state Error err = e;
			Reference<BlobRestoreController> controller = makeReference<BlobRestoreController>(self->db_, normalKeys);
			wait(BlobRestoreController::setError(controller, e.what()));
			TraceEvent("BlobMigratorCanRestoreCheckError", self->interf_.id()).error(err);
			throw err;
		}
		return Void();
	}

	ACTOR static Future<Void> preloadApplyMutationsKeyVersionMap(Reference<BlobMigrator> self) {
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(self->db_));
		state UID uid;
		// Init applyMutationsKeyVersionMap, applyMutationsKeyVersionCount, applyMutationsEnd, applyMutationsBegin
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				UID uid_ = wait(BlobGranuleRestoreConfig().uid().getD(tr));
				ASSERT(uid_.isValid());
				uid = uid_;

				Key mapStart = uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid);
				tr->clear(KeyRangeRef(mapStart, strinc(mapStart)));
				int64_t startCount = 0;
				tr->set(uidPrefixKey(applyMutationsKeyVersionCountRange.begin, uid),
				        StringRef((uint8_t*)&startCount, 8));
				tr->set(mapStart, BinaryWriter::toValue<Version>(invalidVersion, Unversioned()));

				tr->clear(uidPrefixKey(applyMutationsEndRange.begin, uid));
				tr->clear(uidPrefixKey(applyMutationsBeginRange.begin, uid));

				// If this is an incremental restore, we need to set the applyMutationsMapPrefix
				// to the earliest log version so no mutations are missed
				Version beginVersion = self->targetVersion_;
				if (!self->mlogRestoreBeginVersions_.empty()) {
					beginVersion = *std::min_element(self->mlogRestoreBeginVersions_.begin(),
					                                 self->mlogRestoreBeginVersions_.end());
				}
				BlobGranuleRestoreConfig().beginVersion().set(tr, beginVersion);

				Value versionEncoded = BinaryWriter::toValue(beginVersion, Unversioned());
				Key prefix = uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid);
				wait(krmSetRange(tr, prefix, allKeys, versionEncoded));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		// Update applyMutationsKeyVersionMap
		state int i;
		state int stepSize = SERVER_KNOBS->BLOB_RESTORE_LOAD_KEY_VERSION_MAP_STEP_SIZE;
		for (i = 0; i < self->mlogRestoreRanges_.size(); i += stepSize) {
			int end = std::min(i + stepSize, self->mlogRestoreRanges_.size());
			wait(preloadApplyMutationsKeyVersionMap(
			    self->db_, uid, self->mlogRestoreRanges_, self->mlogRestoreBeginVersions_, i, end));
		}
		TraceEvent("PreloadApplyMutationsKeyVersionMap", uid).detail("Size", self->mlogRestoreRanges_.size());
		return Void();
	}

	ACTOR static Future<Void> preloadApplyMutationsKeyVersionMap(Database cx,
	                                                             UID uid,
	                                                             Standalone<VectorRef<KeyRangeRef>> ranges,
	                                                             Standalone<VectorRef<Version>> versions,
	                                                             int start,
	                                                             int end) {
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				state int i;
				for (i = start; i < end; ++i) {
					Version version = versions[i];
					if (version == invalidVersion) {
						version = 0;
					}
					Value versionEncoded = BinaryWriter::toValue(version, Unversioned());
					Key prefix = uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid);
					wait(krmSetRangeCoalescing(tr, prefix, ranges[i], allKeys, versionEncoded));
				}
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	// Main server loop
	ACTOR static Future<Void> serverLoop(Reference<BlobMigrator> self) {
		self->addActor(waitFailureServer(self->interf_.waitFailure.getFuture()));
		self->addActor(handleRequest(self));
		self->addActor(handleUnsupportedRequest(self));
		self->addActor(serveStorageMetricsRequests(self.getPtr(), self->interf_.ssi));
		loop {
			try {
				choose {
					when(HaltBlobMigratorRequest req = waitNext(self->interf_.haltBlobMigrator.getFuture())) {
						dprint("Stopping blob migrator {}\n", self->interf_.id().toString());
						req.reply.send(Void());
						TraceEvent("BlobMigratorHalted", self->interf_.id()).detail("ReqID", req.requesterID);
						break;
					}
					when(wait(self->actors_.getResult())) {}
				}
			} catch (Error& e) {
				dprint("Unexpected serverLoop error {}\n", e.what());
				throw;
			}
		}
		self->actors_.clear(true);
		dprint("Stopped blob migrator {}\n", self->interf_.id().toString());
		return Void();
	}

	// Handle StorageServerInterface APIs
	ACTOR static Future<Void> handleRequest(Reference<BlobMigrator> self) {
		state StorageServerInterface ssi = self->interf_.ssi;
		loop {
			try {
				choose {
					when(GetShardStateRequest req = waitNext(ssi.getShardState.getFuture())) {
						dprint("Handle GetShardStateRequest\n");
						Version version = maxVersion(self);
						GetShardStateReply rep(version, version);
						req.reply.send(rep); // return empty shards
					}
					when(ReplyPromise<KeyValueStoreType> reply = waitNext(ssi.getKeyValueStoreType.getFuture())) {
						dprint("Handle KeyValueStoreType\n");
						reply.send(KeyValueStoreType::MEMORY);
					}
				}
			} catch (Error& e) {
				dprint("Unexpected blob migrator request error {}\n", e.what());
				throw;
			}
		}
	}

	// Handle StorageServerInterface APIs that are not supported. Simply log and return error
	ACTOR static Future<Void> handleUnsupportedRequest(Reference<BlobMigrator> self) {
		state StorageServerInterface ssi = self->interf_.ssi;
		loop {
			try {
				choose {
					when(StorageQueuingMetricsRequest req = waitNext(ssi.getQueuingMetrics.getFuture())) {
						self->addActor(processStorageQueuingMetricsRequest(req));
					}
					when(GetKeyValuesStreamRequest req = waitNext(ssi.getKeyValuesStream.getFuture())) {
						dprint("Unsupported GetKeyValuesStreamRequest\n");
						req.reply.sendError(unsupported_operation());
					}
					when(GetKeyRequest req = waitNext(ssi.getKey.getFuture())) {
						dprint("Unsupported GetKeyRequest\n");
						req.reply.sendError(unsupported_operation());
					}
					when(GetKeyValuesRequest req = waitNext(ssi.getKeyValues.getFuture())) {
						dprint("Unsupported GetKeyValuesRequest {} - {} @ {}\n",
						       req.begin.getKey().printable(),
						       req.end.getKey().printable(),
						       req.version);
						// A temp fix to send back broken promise error so that fetchKey can switch to another
						// storage server. We should remove the storage server interface after
						// restore is done
						req.reply.sendError(broken_promise());
					}
					when(GetValueRequest req = waitNext(ssi.getValue.getFuture())) {
						dprint("Unsupported GetValueRequest\n");
						req.reply.sendError(unsupported_operation());
					}
					when(GetCheckpointRequest req = waitNext(ssi.checkpoint.getFuture())) {
						dprint("Unsupported GetCheckpoint \n");
						req.reply.sendError(unsupported_operation());
					}
					when(FetchCheckpointRequest req = waitNext(ssi.fetchCheckpoint.getFuture())) {
						dprint("Unsupported FetchCheckpointRequest\n");
						req.reply.sendError(unsupported_operation());
					}
					when(UpdateCommitCostRequest req = waitNext(ssi.updateCommitCostRequest.getFuture())) {
						// dprint("Unsupported UpdateCommitCostRequest\n");
						req.reply.sendError(unsupported_operation());
					}
					when(FetchCheckpointKeyValuesRequest req = waitNext(ssi.fetchCheckpointKeyValues.getFuture())) {
						dprint("Unsupported FetchCheckpointKeyValuesRequest\n");
						req.reply.sendError(unsupported_operation());
					}
				}
			} catch (Error& e) {
				dprint("Unexpected request handling error {}\n", e.what());
				throw;
			}
		}
	}

	ACTOR static Future<Void> waitMetricsTenantAwareImpl(Reference<BlobMigrator> self, WaitMetricsRequest req) {
		state WaitMetricsRequest waitMetricsRequest = req;
		state StorageMetrics metrics;
		metrics.bytes = sizeInBytes(self, waitMetricsRequest.keys);
		if (waitMetricsRequest.min.allLessOrEqual(metrics) && metrics.allLessOrEqual(waitMetricsRequest.max)) {
			wait(delay(SERVER_KNOBS->STORAGE_METRIC_TIMEOUT));
		}
		waitMetricsRequest.reply.send(metrics);
		return Void();
	}

	ACTOR static Future<Void> processStorageQueuingMetricsRequest(StorageQueuingMetricsRequest req) {
		// dprint("Unsupported StorageQueuingMetricsRequest\n");
		//  FIXME get rid of this delay. it's a temp solution to avoid starvaion scheduling of DD
		//  processes
		wait(delay(1));
		req.reply.sendError(unsupported_operation());
		return Void();
	}

	ACTOR static Future<Void> checkStillLoading(Transaction* tr, BulkLoadIDKey idKey) {
		Optional<BulkLoadStatus> status = wait(BulkLoadMetadata::statusMap().get(tr, idKey));
		if (status.present() && (status.get().state == BulkLoadStatus::State::ABORTED ||
		                         status.get().state == BulkLoadStatus::State::ABORTED_COMPLETE)) {
			throw bulk_load_aborted();
		} else if (!status.present() || (status.get().state != BulkLoadStatus::State::STAGING_BLOB_DONE &&
		                                 status.get().state != BulkLoadStatus::State::STAGING_DDPREPARED)) {
			throw bulk_load_invalid_state_transition();
		}
		return Void();
	}

	ACTOR static Future<Void> bulkLoadGetGranuleSizes(Reference<BlobMigrator> self,
	                                                  Reference<BulkLoadMigratorState> loadState,
	                                                  KeyRange physicalRange,
	                                                  BulkLoadIDKey idKey) {

		state Transaction tr(self->db_);
		state Standalone<VectorRef<KeyRangeRef>> granules;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

				wait(checkStillLoading(&tr, idKey));
				// FIXME: this might have issues for big ranges! might need to break up
				wait(store(granules, tr.getBlobGranuleRanges(physicalRange, 1000000)));

				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		ASSERT(granules.size() >= 1);
		ASSERT(granules.front().begin == physicalRange.begin);
		ASSERT(granules.back().end == physicalRange.end);
		state int i;

		for (i = 0; i < granules.size() - 1; i++) {
			ASSERT(granules[i].end == granules[i + 1].begin);
		}

		state bool checkLoad = true;
		tr.reset();
		for (i = 0; i < granules.size(); i++) {
			loop {
				try {
					if (checkLoad) {
						wait(checkStillLoading(&tr, idKey));
					}

					Optional<GranuleHistory> history = wait(getLatestGranuleHistory(&tr, granules[i]));
					ASSERT(history.present());
					state KeyRange fileRange = blobGranuleFileKeyRangeFor(history->value.granuleID);
					RangeReadResult r = wait(tr.getRange(fileRange, 2));
					ASSERT(r.size() == 1);

					Standalone<StringRef> filename;
					int64_t offset;
					int64_t length;
					int64_t fullFileLength;
					int64_t logicalSize;
					Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta;

					std::tie(filename, offset, length, fullFileLength, logicalSize, cipherKeysMeta) =
					    decodeBlobGranuleFileValue(r[0].value);

					loadState->granuleSizes.insert(granules[i], logicalSize);
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
					checkLoad = true;
				}
			}
		}

		loadState->granuleSizesLoaded = true;
		return Void();
	}

	ACTOR static Future<Void> migrateBulkLoad(Reference<BlobMigrator> self,
	                                          Reference<BulkLoadMigratorState> loadState,
	                                          KeyRange physicalRange,
	                                          BulkLoadIDKey idKey,
	                                          Optional<Reference<Tenant>> tenant) {

		// in parallel
		state Future<Void> loadGranuleSizes = bulkLoadGetGranuleSizes(
		    self, loadState, physicalRange, idKey); // use summarize call to get granule snapshot sizes

		dprint("BlobMigrator: TrackLoad {0}: preparing load\n", idKey.second.printable());
		wait(prepare(self, physicalRange));
		dprint("BlobMigrator: TrackLoad {0}: marking load prepared\n", idKey.second.printable());

		wait(self->db_->updateBulkLoadState(idKey.second, BulkLoadStatus::State::STAGING_DDPREPARED, tenant));

		dprint("BlobMigrator: TrackLoad {0}: marked load prepared\n", idKey.second.printable());
		wait(loadGranuleSizes);

		state double sleepTime = 2.0;

		dprint("BlobMigrator: TrackLoad {0}: waiting movement complete\n", idKey.second.printable());
		// wait for data movements away from migrator to finish
		loop {
			bool pending = wait(checkPendingDataMovements(self, physicalRange));
			if (!pending) {
				break;
			}

			wait(delayJittered(sleepTime));
			sleepTime = std::min(sleepTime + 1.0, 60.0);
		}

		dprint("BlobMigrator: TrackLoad {0}: marking load done\n", idKey.second.printable());

		// once move complete, mark done
		wait(self->db_->updateBulkLoadState(idKey.second, BulkLoadStatus::State::DONE, tenant));

		return Void();
	}

	ACTOR static Future<Void> trackBulkLoad(Reference<BlobMigrator> self,
	                                        BulkLoadIDKey idKey,
	                                        BulkLoadStatus::State state) {
		dprint("BlobMigrator: TrackLoad {0}: start\n", idKey.second.printable());
		// TODO: handle
		if (state >= BulkLoadStatus::State::DONE || state == BulkLoadStatus::State::ABORTED ||
		    state == BulkLoadStatus::State::ABORTED_COMPLETE) {
			dprint("BlobMigrator: TrackLoad {0}: already complete\n", idKey.second.printable());
			return Void();
		}

		if (!SERVER_KNOBS->BULK_LOAD_SS_MOVE) {
			return Void();
		}

		state Optional<KeyRange> physicalRange;

		try {
			state Optional<Reference<Tenant>> tenant;
			if (idKey.first != TenantInfo::INVALID_TENANT) {
				state Optional<TenantMapEntry> entry =
				    wait(TenantAPI::tryGetTenant(self->db_.getReference(), idKey.first));
				if (entry.present()) {
					tenant = makeReference<Tenant>(self->db_, entry.get());
				} else {
					tenant = makeReference<Tenant>(self->db_, idKey.first);
				}
			}

			if (state < BulkLoadStatus::State::STAGING_BLOB_DONE) {
				wait(self->db_->waitBulkLoadStateAtLeast(
				    idKey.second, BulkLoadStatus::State::STAGING_BLOB_DONE, tenant));
			}

			// load actual key range of bulk load
			state Transaction tr(self->db_);
			loop {
				try {
					Optional<BulkLoadInfo> info = wait(BulkLoadMetadata::infoMap().get(&tr, idKey));
					if (!info.present()) {
						throw bulk_load_not_found();
					}

					physicalRange = info.get().keyRange;
					if (idKey.first != TenantInfo::INVALID_TENANT) {
						physicalRange = physicalRange->withPrefix(TenantAPI::idToPrefix(idKey.first));
					}
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}

			Reference<BulkLoadMigratorState> loadState = makeReference<BulkLoadMigratorState>();
			self->bulkLoadState_.insert(physicalRange.get(), loadState);

			state Future<Void> waitAborted =
			    self->db_->waitBulkLoadStateAtLeast(idKey.second, BulkLoadStatus::State::DONE, tenant);
			state Future<Void> migrateLoad = migrateBulkLoad(self, loadState, physicalRange.get(), idKey, tenant);
			wait(waitAborted || migrateLoad);
			CODE_PROBE(true, "Blob migrator finished a bulk load");
			dprint("BlobMigrator: TrackLoad {0}: completed\n", idKey.second.printable());
		} catch (Error& e) {
			dprint("BlobMigrator: TrackLoad {0}: error {1}\n", idKey.second.printable(), e.name());
			if (e.code() != error_code_bulk_load_not_found && e.code() != error_code_bulk_load_aborted) {
				// If bulk load aborted/cleaned up, stop tracking it
				throw e;
			}
		}
		// clear state when this actor exits if we had set it
		if (physicalRange.present()) {
			self->bulkLoadState_.insert(physicalRange.get(), Reference<BulkLoadMigratorState>());
		}
		return Void();
	}

	static GranuleSizeMap& getGranuleSizeMap(Reference<BlobMigrator> self, KeyRangeRef range) {
		if (self->isFullRestore_) {
			return self->granuleSizes_;
		} else {
			// assume this range is part of exactly 1 bulk load
			auto loadState = self->bulkLoadState_.rangeContaining(range.begin);
			// FIXME: handle errors if this isn't true? throw wrong_shard_server maybe?
			ASSERT(loadState.cvalue().isValid() && loadState.range().contains(range) &&
			       loadState.cvalue()->granuleSizesLoaded);
			return loadState.cvalue()->granuleSizes;
		}
	}

	// Return total storage size in bytes for migration
	static int64_t sizeInBytes(Reference<BlobMigrator> self) { return sizeInBytes(self, normalKeys); }

	// Return storage size in bytes for given key range
	static int64_t sizeInBytes(Reference<BlobMigrator> self, KeyRangeRef range) {
		GranuleSizeMap& granuleSizes = getGranuleSizeMap(self, range);
		int64_t bytes = 0;
		auto queryRanges = granuleSizes.intersectingRanges(range);
		for (auto& it : queryRanges) {
			bytes += it.value();
		}
		return bytes;
	}

	// Return max version for all blob granules
	static Version maxVersion(Reference<BlobMigrator> self) {
		Version max = 0;
		for (auto granule : self->blobGranules_) {
			max = std::max(granule.version, max);
		}
		return max;
	}

public: // Methods for IStorageMetricsService
	void addActor(Future<Void> future) override { actors_.add(future); }

	Future<Void> getSplitPoints(SplitRangeRequest const& req) override {
		dprint("Unsupported SplitRangeRequest\n");
		req.reply.sendError(broken_promise());
		return Void();
	}

	Future<Void> waitMetricsTenantAware(const WaitMetricsRequest& req) override {
		Reference<BlobMigrator> self = Reference<BlobMigrator>::addRef(this);
		return waitMetricsTenantAwareImpl(self, req);
	}

	void getStorageMetrics(const GetStorageMetricsRequest& req) override {
		StorageMetrics metrics;
		Reference<BlobMigrator> self = Reference<BlobMigrator>::addRef(this);
		metrics.bytes = sizeInBytes(self);
		GetStorageMetricsReply resp;
		resp.load = metrics;
		resp.available = StorageMetrics();
		resp.capacity = StorageMetrics();
		resp.bytesInputRate = 0;
		resp.versionLag = 0;
		resp.lastUpdate = now();
		req.reply.send(resp);
	}

	// This API is used by DD to figure out split points for data movement.
	void getSplitMetrics(const SplitMetricsRequest& req) override {
		Reference<BlobMigrator> self = Reference<BlobMigrator>::addRef(this);
		GranuleSizeMap& granuleSizes = getGranuleSizeMap(self, req.keys);
		SplitMetricsReply rep;
		int64_t bytes = 0; // number of bytes accumulated for current split

		auto queryRanges = granuleSizes.intersectingRanges(req.keys);
		for (auto& granule : queryRanges) {
			bytes += granule.cvalue();
			if (bytes < req.limits.bytes) {
				continue;
			}
			// Add a split point if the key range exceeds expected minimal size in bytes
			rep.splits.push_back_deep(rep.splits.arena(), granule.end());
			bytes = 0;
			// Limit number of splits in single response for fast RPC processing
			if (rep.splits.size() > SERVER_KNOBS->SPLIT_METRICS_MAX_ROWS) {
				CODE_PROBE(true, "Blob Migrator SplitMetrics API has more", probe::decoration::rare);
				TraceEvent("BlobMigratorSplitMetricsContinued", self->interf_.id())
				    .detail("Range", req.keys)
				    .detail("Splits", rep.splits.size());
				rep.more = true;
				break;
			}
		}
		req.reply.send(rep);
	}

	void getHotRangeMetrics(const ReadHotSubRangeRequest& req) override {
		ReadHotSubRangeReply emptyReply;
		req.reply.send(emptyReply);
	}

	template <class Reply>
	void sendErrorWithPenalty(const ReplyPromise<Reply>& promise, const Error& err, double) {
		promise.sendError(err);
	}

private:
	Database db_;
	BlobGranuleRestoreVersionVector blobGranules_;
	GranuleSizeMap granuleSizes_;
	BlobMigratorInterface interf_;
	ActorCollection actors_;
	FileBackupAgent backupAgent_;
	BulkLoadTracker bulkLoadTracker_;
	bool isFullRestore_;
	CoalescedKeyRangeMap<Reference<BulkLoadMigratorState>> bulkLoadState_;

	Key mlogsUrl_;
	Version targetVersion_;
	Standalone<VectorRef<KeyRangeRef>> mlogRestoreRanges_;
	Standalone<VectorRef<Version>> mlogRestoreBeginVersions_;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo_;
};

// Main entry point
ACTOR Future<Void> blobMigrator(BlobMigratorInterface interf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	TraceEvent("StartBlobMigrator", interf.id()).detail("Interface", interf.id().toString());
	dprint("Starting blob migrator {}\n", interf.id().toString());
	state Reference<BlobMigrator> self = makeReference<BlobMigrator>(interf, dbInfo);
	try {
		wait(BlobMigrator::start(self));
	} catch (Error& e) {
		dprint("Unexpected migrator error {}\n", e.what());
		TraceEvent("BlobMigratorError", interf.id()).error(e);
	}
	return Void();
}
