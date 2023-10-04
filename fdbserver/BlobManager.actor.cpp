/*
 * BlobManager.actor.cpp
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
#include <ctime>
#include <limits>
#include <sstream>
#include <queue>
#include <vector>
#include <unordered_map>

#include "fdbclient/BackupContainer.h"
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/ServerKnobs.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/CodeProbe.h"
#include "flow/FastRef.h"
#include "flow/genericactors.actor.h"
#include "flow/serialize.h"
#include "fmt/format.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobGranuleRequest.actor.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/BulkLoad.h"
#include "fdbclient/BulkLoadMetadata.h"
#include "fdbclient/BulkLoadTracker.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tuple.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BlobRestoreCommon.h"
#include "fdbserver/BlobManagerInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/BlobGranuleValidation.actor.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/ExclusionTracker.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/DebugTrace.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include

/*
 * The Blob Manager is responsible for managing range granules, and recruiting and monitoring Blob Workers.
 */

#define BM_DEBUG false
#define BM_PURGE_DEBUG false
#define BM_BULKLOAD_DEBUG false

namespace {
void handleClientBlobRange(int64_t epoch,
                           KeyRangeMap<bool>* knownBlobRanges,
                           Arena& ar,
                           VectorRef<KeyRangeRef>* rangesToAdd,
                           VectorRef<KeyRangeRef>* rangesToRemove,
                           KeyRef rangeStart,
                           KeyRef rangeEnd,
                           bool rangeActive) {
	if (BM_DEBUG) {
		fmt::print(
		    "db range [{0} - {1}): {2}\n", rangeStart.printable(), rangeEnd.printable(), rangeActive ? "T" : "F");
	}
	KeyRange keyRange(KeyRangeRef(rangeStart, rangeEnd));
	auto allRanges = knownBlobRanges->intersectingRanges(keyRange);
	for (auto& r : allRanges) {
		if (r.value() != rangeActive) {
			KeyRef overlapStart = (r.begin() > keyRange.begin) ? r.begin() : keyRange.begin;
			KeyRef overlapEnd = (keyRange.end < r.end()) ? keyRange.end : r.end();
			KeyRangeRef overlap(overlapStart, overlapEnd);
			if (rangeActive) {
				if (BM_DEBUG) {
					fmt::print("BM {0} Adding client range [{1} - {2})\n",
					           epoch,
					           overlapStart.printable().c_str(),
					           overlapEnd.printable().c_str());
				}
				rangesToAdd->push_back_deep(ar, overlap);
			} else {
				if (BM_DEBUG) {
					fmt::print("BM {0} Removing client range [{1} - {2})\n",
					           epoch,
					           overlapStart.printable().c_str(),
					           overlapEnd.printable().c_str());
				}
				rangesToRemove->push_back_deep(ar, overlap);
			}
		}
	}
	knownBlobRanges->insert(keyRange, rangeActive);
}

void updateClientBlobRanges(int64_t epoch,
                            KeyRangeMap<bool>* knownBlobRanges,
                            RangeResult dbBlobRanges,
                            Arena& ar,
                            VectorRef<KeyRangeRef>* rangesToAdd,
                            VectorRef<KeyRangeRef>* rangesToRemove) {
	if (BM_DEBUG) {
		fmt::print("Updating {0} client blob ranges", dbBlobRanges.size() / 2);
		for (int i = 0; i < dbBlobRanges.size() - 1; i += 2) {
			fmt::print("  [{0} - {1})", dbBlobRanges[i].key.printable(), dbBlobRanges[i + 1].key.printable());
		}
		printf("\n");
	}
	// essentially do merge diff of current known blob ranges and new ranges, to assign new ranges to
	// workers and revoke old ranges from workers

	// basically, for any range that is set in results that isn't set in ranges, assign the range to the
	// worker. for any range that isn't set in results that is set in ranges, revoke the range from the
	// worker. and, update ranges to match results as you go

	// SOMEDAY: could change this to O(N) instead of O(NLogN) by doing a sorted merge instead of requesting the
	// intersection for each insert, but this operation is pretty infrequent so it's probably not necessary
	if (dbBlobRanges.size() == 0) {
		// special case. Nothing in the DB, reset knownBlobRanges and revoke all existing ranges from workers
		handleClientBlobRange(
		    epoch, knownBlobRanges, ar, rangesToAdd, rangesToRemove, normalKeys.begin, normalKeys.end, false);
	} else {
		if (dbBlobRanges[0].key > normalKeys.begin) {
			handleClientBlobRange(
			    epoch, knownBlobRanges, ar, rangesToAdd, rangesToRemove, normalKeys.begin, dbBlobRanges[0].key, false);
		}
		for (int i = 0; i < dbBlobRanges.size() - 1; i++) {
			if (dbBlobRanges[i].key >= normalKeys.end) {
				if (BM_DEBUG) {
					fmt::print("Found invalid blob range start {0}\n", dbBlobRanges[i].key.printable());
				}
				break;
			}
			bool active = isBlobRangeActive(dbBlobRanges[i].value);
			if (active) {
				if (BM_DEBUG) {
					fmt::print("BM {0} sees client range [{1} - {2})\n",
					           epoch,
					           dbBlobRanges[i].key.printable(),
					           dbBlobRanges[i + 1].key.printable());
				}
			}
			KeyRef endKey = dbBlobRanges[i + 1].key;
			if (endKey > normalKeys.end) {
				if (BM_DEBUG) {
					fmt::print("Removing system keyspace from blob range [{0} - {1})\n",
					           dbBlobRanges[i].key.printable(),
					           endKey.printable());
				}
				endKey = normalKeys.end;
			}
			handleClientBlobRange(
			    epoch, knownBlobRanges, ar, rangesToAdd, rangesToRemove, dbBlobRanges[i].key, endKey, active);
		}
		if (dbBlobRanges[dbBlobRanges.size() - 1].key < normalKeys.end) {
			handleClientBlobRange(epoch,
			                      knownBlobRanges,
			                      ar,
			                      rangesToAdd,
			                      rangesToRemove,
			                      dbBlobRanges[dbBlobRanges.size() - 1].key,
			                      normalKeys.end,
			                      false);
		}
	}
	knownBlobRanges->coalesce(normalKeys);
}

void getRanges(std::vector<std::pair<KeyRangeRef, bool>>& results, KeyRangeMap<bool>& knownBlobRanges) {
	if (BM_DEBUG) {
		printf("Getting ranges:\n");
	}
	auto allRanges = knownBlobRanges.ranges();
	for (auto& r : allRanges) {
		results.emplace_back(r.range(), r.value());
		if (BM_DEBUG) {
			fmt::print("  [{0} - {1}): {2}\n", r.begin().printable(), r.end().printable(), r.value() ? "T" : "F");
		}
	}
}

struct RangeAssignmentData {
	AssignRequestType type;

	RangeAssignmentData() : type(AssignRequestType::Normal) {}
	RangeAssignmentData(AssignRequestType type) : type(type) {}
};

struct RangeRevokeData {
	bool dispose;

	RangeRevokeData() {}
	RangeRevokeData(bool dispose) : dispose(dispose) {}
};

struct RangeAssignment {
	bool isAssign;
	KeyRange keyRange;
	Optional<UID> worker;
	Optional<std::pair<UID, Error>> previousFailure;

	// I tried doing this with a union and it was just kind of messy
	Optional<RangeAssignmentData> assign;
	Optional<RangeRevokeData> revoke;
};

// SOMEDAY: track worker's reads/writes eventually
// FIXME: namespace?
struct BlobWorkerInfo {
	int numGranulesAssigned;
	int recentGranulesAssigned;
	Promise<Void> signalDead;

	BlobWorkerInfo(int numGranulesAssigned = 0, int recentGranulesAssigned = 0)
	  : numGranulesAssigned(numGranulesAssigned),
	    recentGranulesAssigned(recentGranulesAssigned) {}
};

// recover is when the BM assigns an ambiguously owned range on recovery
// merge is when the BM initiated a merge candidate for the range
// split is when the BM initiated a split check for the range
enum BoundaryEvalType { UNKNOWN, RECOVER, MERGE, SPLIT };

struct BoundaryEvaluation {
	int64_t epoch;
	int64_t seqno;
	BoundaryEvalType type;
	Future<Void> inProgress;
	int64_t originalEpoch;
	int64_t originalSeqno;

	BoundaryEvaluation() : epoch(0), seqno(0), type(UNKNOWN), originalEpoch(0), originalSeqno(0) {}
	BoundaryEvaluation(int64_t epoch,
	                   int64_t seqno,
	                   BoundaryEvalType type,
	                   int64_t originalEpoch,
	                   int64_t originalSeqno)
	  : epoch(epoch),
	    seqno(seqno),
	    type(type),
	    originalEpoch(originalEpoch),
	    originalSeqno(originalSeqno) {
		ASSERT(type != UNKNOWN);
	}

	bool operator==(const BoundaryEvaluation& other) const {
		return epoch == other.epoch && seqno == other.seqno && type == other.type;
	}

	bool operator<(const BoundaryEvaluation& other) {
		// if (epoch, seqno) don't match, go by (epoch, seqno)
		if (epoch == other.epoch && seqno == other.seqno) {
			return type < other.type;
		}
		return epoch < other.epoch || (epoch == other.epoch && seqno < other.seqno);
	}

	bool isOlderThanOriginal(const BoundaryEvaluation& other) {
		return originalEpoch < other.originalEpoch ||
		       (originalEpoch == other.originalEpoch && originalSeqno < other.originalSeqno);
	}

	std::string toString() const {
		return fmt::format("{0} @ ({1}, {2})",
		                   type == BoundaryEvalType::UNKNOWN ? "unknown"
		                                                     : (type == BoundaryEvalType::MERGE ? "merge" : "split"),
		                   epoch,
		                   seqno);
	}
};

struct PurgeRangeDAGNode : ReferenceCounted<PurgeRangeDAGNode>, NonCopyable {
	KeyRange range;
	Key purgeKey;
	Version purgeVersion;
	bool force;
	Future<Void> purgeFuture;
	UID nodeID;
	std::vector<Reference<PurgeRangeDAGNode>> replacedParents;
	// FIXME : add alloc tracking to determine if we leak nodes

	PurgeRangeDAGNode()
	  : range(normalKeys),
	    purgeKey(""_sr),
	    purgeVersion(-1),
	    force(false),
	    purgeFuture(Future<Void>(Void())),
	    nodeID(deterministicRandom()->randomUniqueID()) {}

	PurgeRangeDAGNode(KeyRangeRef range, KeyRef purgeKey, Version purgeVersion, bool force)
	  : range(range),
	    purgeKey(purgeKey),
	    purgeVersion(purgeVersion),
	    force(force),
	    purgeFuture(Never()),
	    nodeID(deterministicRandom()->randomUniqueID()) {}

	~PurgeRangeDAGNode() { purgeFuture.cancel(); }
};

struct BlobManagerStats {
	CounterCollection cc;

	Counter granuleSplits;
	Counter granuleWriteHotSplits;
	Counter granuleMerges;
	Counter ccGranulesChecked;
	Counter ccRowsChecked;
	Counter ccBytesChecked;
	Counter ccMismatches;
	Counter ccTimeouts;
	Counter ccErrors;
	Counter purgesProcessed;
	Counter granulesFullyPurged;
	Counter granulesPartiallyPurged;
	Counter filesPurged;
	Counter purgeLatestReads;
	Counter purgeDAGReads;
	Counter granuleStreamReplies;
	Counter granuleStreamRepliesIgnored;
	Counter granulesHitMedianLimit;
	Counter cantSplitAlignment;
	Counter noopPurges;

	Counter bulkLoadsStaged;
	Counter bulkLoadStagingErrors;
	Counter bulkLoadAbortsCleanedUp;
	Counter bulkLoadUnusedFilesCleanedUp;

	Future<Void> logger;
	int64_t activeMerges;
	int64_t blockedAssignments;

	Version lastFlushVersion;
	Version lastMLogTruncationVersion;
	int64_t lastManifestSeqNo;
	int64_t lastManifestDumpTs;
	int64_t manifestSizeInBytes;

	ActiveCounter<int> activeGranuleSplitChecks;
	ActiveCounter<int> activePurges;
	ActiveCounter<int> blockedPurges;
	ActiveCounter<int> awaitingCleanupPurges;
	ActiveCounter<int> activeBulkLoads;

	// Current stats maintained for a given blob worker process
	explicit BlobManagerStats(UID id,
	                          double interval,
	                          int64_t epoch,
	                          std::unordered_map<UID, BlobWorkerInterface>* workers,
	                          std::map<Key, bool>* mergeHardBoundaries,
	                          std::unordered_map<Key, BlobGranuleMergeBoundary>* mergeBoundaries,
	                          AsyncVar<int>* recruitingStream)
	  : cc("BlobManagerStats", id.toString()),
	    granuleSplits("GranuleSplits", cc),
	    granuleWriteHotSplits("GranuleWriteHotSplits", cc),
	    granuleMerges("GranuleMerges", cc),
	    ccGranulesChecked("CCGranulesChecked", cc),
	    ccRowsChecked("CCRowsChecked", cc),
	    ccBytesChecked("CCBytesChecked", cc),
	    ccMismatches("CCMismatches", cc),
	    ccTimeouts("CCTimeouts", cc),
	    ccErrors("CCErrors", cc),
	    purgesProcessed("PurgesProcessed", cc),
	    granulesFullyPurged("GranulesFullyPurged", cc),
	    granulesPartiallyPurged("GranulesPartiallyPurged", cc),
	    filesPurged("FilesPurged", cc),
	    purgeLatestReads("PurgeLatestReads", cc),
	    purgeDAGReads("PurgeDAGReads", cc),
	    granuleStreamReplies("GranuleStreamReplies", cc),
	    granuleStreamRepliesIgnored("GranuleStreamRepliesIgnored", cc),
	    granulesHitMedianLimit("GranulesHitMedianLimit", cc),
	    cantSplitAlignment("CantSplitAlignment", cc),
	    noopPurges("NoOpPurges", cc),
	    bulkLoadsStaged("BulkLoadsStaged", cc),
	    bulkLoadStagingErrors("BulkLoadStagingErrors", cc),
	    bulkLoadAbortsCleanedUp("BulkLoadAbortsCleanedUp", cc),
	    bulkLoadUnusedFilesCleanedUp("BulkLoadUnusedPointersCleanedUp", cc),
	    activeMerges(0),
	    blockedAssignments(0),
	    lastFlushVersion(0),
	    lastMLogTruncationVersion(0),
	    lastManifestSeqNo(0),
	    lastManifestDumpTs(0),
	    manifestSizeInBytes(0),
	    activeGranuleSplitChecks(0),
	    activePurges(0),
	    blockedPurges(0),
	    awaitingCleanupPurges(0),
	    activeBulkLoads(0) {
		specialCounter(cc, "WorkerCount", [workers]() { return workers->size(); });
		specialCounter(cc, "Epoch", [epoch]() { return epoch; });
		specialCounter(cc, "ActiveMerges", [this]() { return this->activeMerges; });
		specialCounter(cc, "HardBoundaries", [mergeHardBoundaries]() { return mergeHardBoundaries->size(); });
		specialCounter(cc, "SoftBoundaries", [mergeBoundaries]() { return mergeBoundaries->size(); });
		specialCounter(cc, "BlockedAssignments", [this]() { return this->blockedAssignments; });
		specialCounter(cc, "LastFlushVersion", [this]() { return this->lastFlushVersion; });
		specialCounter(cc, "LastMLogTruncationVersion", [this]() { return this->lastMLogTruncationVersion; });
		specialCounter(cc, "LastManifestSeqNo", [this]() { return this->lastManifestSeqNo; });
		specialCounter(cc, "LastManifestDumpTs", [this]() { return this->lastManifestDumpTs; });
		specialCounter(cc, "ManifestSizeInBytes", [this]() { return this->manifestSizeInBytes; });
		specialCounter(
		    cc, "ActiveGranuleSplitChecks", [this]() -> int { return this->activeGranuleSplitChecks.getValue(); });
		specialCounter(cc, "ActivePurges", [this]() -> int { return this->activePurges.getValue(); });
		specialCounter(cc, "BlockedPurges", [this]() -> int { return this->blockedPurges.getValue(); });
		specialCounter(cc, "AwaitingCleanupPurges", [this]() -> int { return this->awaitingCleanupPurges.getValue(); });
		specialCounter(
		    cc, "ActiveBlobWorkerRecruitments", [recruitingStream]() -> int { return recruitingStream->get(); });
		specialCounter(cc, "ActiveBulkLoads", [this]() -> int { return this->activeBulkLoads.getValue(); });
		logger = cc.traceCounters("BlobManagerMetrics", id, interval, "BlobManagerMetrics");
	}
};

enum MergeCandidateState {
	MergeCandidateUnknown,
	MergeCandidateCannotMerge,
	MergeCandidateCanMerge,
	MergeCandidateMerging
};

// The current merge algorithm, skipping just granules that will be merge-eligible on the next pass, but not
// their neighbors, is optimal for guaranteeing merges to make progress where possible, with decently
// optimal but not globally optimal merge behavior.
// Alternative algorithms include not doing a two-pass consideration at all and immediately considering
// all merge candidates, which guarantees the most progress but pretty much guarantees undesirably
// suboptimal merge decisions, because of the time variance of granules becoming merge candidates. Or,
// also skipping adjacent eligible granules in addition to the one that will be eligible next pass,
// which ensures optimally large merges in a future pass, but adds decent delay to doing the merge. Or,
// smarter considering of merge candidates adjacent to the one that will be eligible next pass
// (depending on whether potential future merges with adjacent ones could include this candidate), which
// would be the best of both worlds, but would add a decent amount of code complexity.
struct MergeCandidateInfo {
	MergeCandidateState st;
	UID granuleID;
	Version startVersion;
	// This is if this candidate has been seen by the merge checker before.
	bool seen;

	MergeCandidateInfo() : st(MergeCandidateUnknown), startVersion(invalidVersion), seen(false) {}

	MergeCandidateInfo(MergeCandidateState st) : st(st), startVersion(invalidVersion), seen(false) {
		ASSERT(st != MergeCandidateCanMerge);
	}
	MergeCandidateInfo(UID granuleID, Version startVersion)
	  : st(MergeCandidateCanMerge),
	    granuleID(granuleID),
	    startVersion(startVersion),
	    seen(false) {}

	bool canMerge() const { return st == MergeCandidateCanMerge; }

	bool mergeEligible() const { return st == MergeCandidateCanMerge && seen; }
};

struct BlobGranuleSplitPoints {
	Standalone<VectorRef<KeyRef>> keys;
	std::unordered_map<Key, BlobGranuleMergeBoundary> boundaries;
};

struct BlobManagerData : NonCopyable, ReferenceCounted<BlobManagerData> {
	UID id;
	Database db;
	Optional<Key> dcId;
	PromiseStream<Future<Void>> addActor;
	Promise<Void> doLockCheck;

	Reference<BlobConnectionProvider> bstore;

	std::unordered_map<UID, BlobWorkerInterface> workersById;
	std::unordered_map<UID, BlobWorkerInfo> workerStats; // mapping between workerID -> workerStats
	std::deque<UID> recentBWAssignments;
	std::unordered_set<NetworkAddress> workerAddresses;
	std::unordered_set<UID> deadWorkers;
	std::unordered_map<UID, UID> workerAffinities;
	KeyRangeMap<UID> workerAssignments;
	KeyRangeActorMap assignsInProgress;
	KeyRangeMap<BoundaryEvaluation> boundaryEvaluations;
	KeyRangeMap<bool> knownBlobRanges;
	BGTenantMap tenantData;
	KeyRangeMap<MergeCandidateInfo> mergeCandidates; // granule range to granule id + start version.
	KeyRangeMap<Version> activeGranuleMerges; // range map of active granule merges, because range in boundaryEval
	                                          // doesn't correspond to merge range. invalidVersion is no merge,
	                                          // 0 is no merge version determined yet
	// TODO: consider switching to an iterator approach.
	std::map<Key, bool> mergeHardBoundaries;
	std::unordered_map<Key, BlobGranuleMergeBoundary> mergeBoundaries;
	CoalescedKeyRangeMap<bool> forcePurgingRanges;
	KeyRangeMap<Reference<PurgeRangeDAGNode>> latestActivePurges;
	std::deque<Key> completedPurgeKeys;

	FlowLock concurrentMergeChecks;

	AsyncTrigger startRecruiting;
	Debouncer restartRecruiting;
	std::set<NetworkAddress> recruitingLocalities; // the addrs of the workers being recruited on
	AsyncVar<int> recruitingStream;
	ExclusionTracker exclusionTracker;

	BlobManagerStats stats;

	Promise<Void> foundBlobWorkers;
	Promise<Void> doneRecovering;
	Promise<Void> loadedClientRanges;
	AsyncVar<int64_t> loadedClientRangesRound;

	int64_t epoch;
	int64_t seqNo = 1;
	int64_t manifestDumperSeqNo = 1;
	bool enableManifestEncryption = false;
	AsyncTrigger backupTrigger;
	AsyncTrigger manifestCompletitionTrigger;

	Promise<Void> iAmReplaced;

	bool isFullRestoreMode = false;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;

	BlobManagerData(UID id,
	                Reference<AsyncVar<ServerDBInfo> const> dbInfo,
	                Database db,
	                Optional<Key> dcId,
	                int64_t epoch)
	  : id(id),
	    db(db),
	    dcId(dcId),
	    knownBlobRanges(false, normalKeys.end),
	    tenantData(BGTenantMap(dbInfo)),
	    mergeCandidates(MergeCandidateInfo(MergeCandidateUnknown), normalKeys.end),
	    activeGranuleMerges(invalidVersion, normalKeys.end),
	    forcePurgingRanges(false, normalKeys.end),
	    latestActivePurges(makeReference<PurgeRangeDAGNode>(), normalKeys.end),
	    concurrentMergeChecks(SERVER_KNOBS->BLOB_MANAGER_CONCURRENT_MERGE_CHECKS),
	    restartRecruiting(SERVER_KNOBS->DEBOUNCE_RECRUITING_DELAY),
	    recruitingStream(0),
	    exclusionTracker(db),
	    stats(id,
	          SERVER_KNOBS->WORKER_LOGGING_INTERVAL,
	          epoch,
	          &workersById,
	          &mergeHardBoundaries,
	          &mergeBoundaries,
	          &recruitingStream),
	    loadedClientRangesRound(0),
	    epoch(epoch),
	    dbInfo(dbInfo) {}

	// only initialize blob store if actually needed
	void initBStore() {
		if (!bstore.isValid() && SERVER_KNOBS->BG_METADATA_SOURCE != "tenant") {
			if (BM_DEBUG) {
				fmt::print("BM {} constructing backup container from {}\n", epoch, SERVER_KNOBS->BG_URL.c_str());
			}
			bstore = BlobConnectionProvider::newBlobConnectionProvider(SERVER_KNOBS->BG_URL);
			if (BM_DEBUG) {
				fmt::print("BM {} constructed backup container\n", epoch);
			}
		}
	}

	bool isMergeActive(const KeyRangeRef& range) {
		auto ranges = activeGranuleMerges.intersectingRanges(range);
		for (auto& it : ranges) {
			if (it.value() != invalidVersion) {
				return true;
			}
		}
		return false;
	}

	Version activeMergeVersion(const KeyRangeRef& range) {
		auto ranges = activeGranuleMerges.intersectingRanges(range);
		Version v = invalidVersion;
		for (auto& it : ranges) {
			v = std::max(v, it.cvalue());
		}
		return v;
	}

	// FIXME: is it possible for merge/split/re-merge to call this with same range but a different granule id or
	// startVersion? Unlikely but could cause weird history problems
	void setMergeCandidate(const KeyRangeRef& range, UID granuleID, Version startVersion) {
		// if this granule is not an active granule, it can't be merged
		auto gIt = workerAssignments.rangeContaining(range.begin);
		if (gIt->begin() != range.begin || gIt->end() != range.end) {
			CODE_PROBE(true, "non-active granule reported merge eligible, ignoring", probe::decoration::rare);
			if (BM_DEBUG) {
				fmt::print(
				    "BM {0} Ignoring Merge Candidate [{1} - {2}): range mismatch with active granule [{3} - {4})\n",
				    epoch,
				    range.begin.printable(),
				    range.end.printable(),
				    gIt->begin().printable(),
				    gIt->end().printable());
			}
			return;
		}
		// Want this to be idempotent. If a granule was already reported as merge-eligible, we want to use the existing
		// merge and mergeNow state.
		auto it = mergeCandidates.rangeContaining(range.begin);

		if (it->begin() == range.begin && it.end() == range.end) {
			if (it->cvalue().st != MergeCandidateCanMerge) {
				// same range, just update
				it->value() = MergeCandidateInfo(granuleID, startVersion);
			} else {
				// else no-op, but validate data
				ASSERT(granuleID == it->cvalue().granuleID);
				ASSERT(startVersion == it->cvalue().startVersion);
			}
		} else if (it->cvalue().st != MergeCandidateMerging) {
			mergeCandidates.insert(range, MergeCandidateInfo(granuleID, startVersion));
		}
	}

	void setMergeCandidate(const KeyRangeRef& range, MergeCandidateState st) {
		ASSERT(st != MergeCandidateCanMerge);
		mergeCandidates.insert(range, MergeCandidateInfo(st));
	}

	void clearMergeCandidate(const KeyRangeRef& range) { setMergeCandidate(range, MergeCandidateCannotMerge); }

	bool isForcePurging(const KeyRangeRef& range) {
		auto ranges = forcePurgingRanges.intersectingRanges(range);
		for (auto& it : ranges) {
			if (it.value()) {
				return true;
			}
		}
		return false;
	}

	bool maybeInjectTargetedRestart() {
		// inject a BW restart at most once per test
		if (g_network->isSimulated() && !g_simulator->speedUpSimulation &&
		    now() > g_simulator->injectTargetedBMRestartTime && iAmReplaced.canBeSet()) {
			CODE_PROBE(true, "Injecting BM targeted restart");
			TraceEvent("SimBMInjectTargetedRestart", id).log();
			g_simulator->injectTargetedBMRestartTime = std::numeric_limits<double>::max();
			iAmReplaced.send(Void());
			return true;
		}
		return false;
	}
};

// Helper function for alignKeys().
// This attempts to do truncation and compares with the last key in splitPoints.keys.
static void alignKeyBoundary(Reference<BlobManagerData> bmData,
                             Reference<GranuleTenantData> tenantData,
                             KeyRef key,
                             int offset,
                             bool snowtramAware,
                             BlobGranuleSplitPoints& splitPoints) {
	Standalone<VectorRef<KeyRef>>& keys = splitPoints.keys;
	std::unordered_map<Key, BlobGranuleMergeBoundary>& boundaries = splitPoints.boundaries;
	KeyRef alignedKey = key;
	Tuple t, t2;

	if (!offset) {
		keys.push_back_deep(keys.arena(), alignedKey);
		return;
	}

	// If this is tenant aware code.
	if (tenantData.isValid()) {
		alignedKey = alignedKey.removePrefix(tenantData->entry.prefix);
	}
	try {
		// if snowtram aware, tuple must parse and not be truncated, but maintain non-snowtram-aware case for backwards
		// compatibility
		t = Tuple::unpackUserType(alignedKey, !snowtramAware);
		if (snowtramAware) {
			bool snowtramKey = t.isSnowtramKey();
			if (snowtramKey && t.isSnowtramDataOrIndexSpace()) {
				CODE_PROBE(true, "snowtram-aware alignKeys found snowtram key to truncate");
				ASSERT(offset == 1);
			} else {
				CODE_PROBE(snowtramKey, "found snowtram key in non-data/index space");
				// if not valid snowtram key in the data or index space, don't truncate
				// We expect TST entries to hit this, but if any user key does that's bad
				CODE_PROBE(true, "snowtram-aware alignKeys found non-snowtram key, skipping truncate");
				offset = t.size();
			}
		}
		if (t.size() > offset) {
			t2 = t.subTuple(0, t.size() - offset);
			alignedKey = t2.pack();
		}
	} catch (Error& e) {
		if (e.code() != error_code_invalid_tuple_data_type) {
			throw;
		}
	}
	if (tenantData.isValid()) {
		alignedKey = alignedKey.withPrefix(tenantData->entry.prefix, keys.arena());
	}

	// Only add the alignedKey if it's larger than the last key. If it's the same, drop the split if not allowed.
	if (alignedKey <= keys.back()) {
		if (SERVER_KNOBS->BG_ENABLE_SPLIT_TRUNCATED) {
			// Set split boundary.
			BlobGranuleMergeBoundary boundary = { /*buddy=*/true };
			boundaries[key] = boundary;
			keys.push_back_deep(keys.arena(), key);
		} // else drop the split
	} else {
		keys.push_back_deep(keys.arena(), alignedKey);
	}
}

ACTOR Future<BlobGranuleSplitPoints> alignKeys(Reference<BlobManagerData> bmData,
                                               KeyRange granuleRange,
                                               Standalone<VectorRef<KeyRef>> splits) {
	state BlobGranuleSplitPoints splitPoints;

	state int offset = SERVER_KNOBS->BG_KEY_TUPLE_TRUNCATE_OFFSET;
	if (offset <= 0) {
		splitPoints.keys = splits;
		return splitPoints;
	}

	splitPoints.keys.push_back_deep(splitPoints.keys.arena(), splits.front());

	state int idx = 1;
	state Reference<GranuleTenantData> tenantData;
	state int retryCount = 0;
	wait(store(tenantData, bmData->tenantData.getDataForGranule(granuleRange, false)));
	while (SERVER_KNOBS->BG_METADATA_SOURCE == "tenant" && !tenantData.isValid()) {
		// this is a bit of a hack, but if we know this range is supposed to have a tenant, and it doesn't, just wait
		retryCount++;
		TraceEvent(retryCount <= 10 ? SevDebug : SevWarn, "BlobManagerUnknownTenantAlignKeys", bmData->id)
		    .suppressFor(5.0)
		    .detail("Epoch", bmData->epoch)
		    .detail("Granule", granuleRange)
		    .detail("Retries", retryCount);
		wait(delay(1.0));
		wait(store(tenantData, bmData->tenantData.getDataForGranule(granuleRange, false)));
	}
	for (; idx < splits.size() - 1; idx++) {
		alignKeyBoundary(bmData, tenantData, splits[idx], offset, SERVER_KNOBS->BG_KEY_SNOWTRAM_AWARE, splitPoints);
	}

	splitPoints.keys.push_back_deep(splitPoints.keys.arena(), splits.back());

	return splitPoints;
}

// Find the next full db key for each split point
ACTOR Future<Standalone<VectorRef<KeyRef>>> nextFullKeys(Reference<BlobManagerData> bmData,
                                                         Standalone<VectorRef<KeyRef>> keys) {
	state Standalone<VectorRef<KeyRef>> result;
	result.push_back_deep(result.arena(), keys.front()); // keep the begin key as is

	state Transaction tr = Transaction(bmData->db);
	state int i = 1;
	for (; i < keys.size() - 1; ++i) {
		loop {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				RangeReadResult nextKeyRes =
				    wait(tr.getRange(firstGreaterOrEqual(keys[i]), lastLessThan(keys[i + 1]), GetRangeLimits(1)));
				if (nextKeyRes.size() == 0) {
					break;
				}
				result.push_back_deep(result.arena(), nextKeyRes[0].key);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
	result.push_back_deep(result.arena(), keys.back()); // keep the end key as is
	return result;
}

// split recursively in the middle to guarantee roughly equal splits across different parts of key space
static void downsampleSplit(const Standalone<VectorRef<KeyRef>>& splits,
                            Standalone<VectorRef<KeyRef>>& out,
                            int startIdx,
                            int endIdx,
                            int remaining) {
	ASSERT(endIdx - startIdx >= remaining);
	ASSERT(remaining >= 0);
	if (remaining == 0) {
		return;
	}
	if (endIdx - startIdx == remaining) {
		out.append(out.arena(), splits.begin() + startIdx, remaining);
	} else {
		int mid = (startIdx + endIdx) / 2;
		int startCount = (remaining - 1) / 2;
		int endCount = remaining - startCount - 1;
		// ensure no infinite recursion
		ASSERT(mid != endIdx);
		ASSERT(mid + 1 != startIdx);
		downsampleSplit(splits, out, startIdx, mid, startCount);
		out.push_back(out.arena(), splits[mid]);
		downsampleSplit(splits, out, mid + 1, endIdx, endCount);
	}
}

ACTOR Future<BlobGranuleSplitPoints> splitRange(Reference<BlobManagerData> bmData,
                                                KeyRange range,
                                                bool writeHot,
                                                bool initialSplit,
                                                Optional<int> maxSplitPoints = Optional<int>()) {
	state BlobGranuleSplitPoints splitPoints;
	try {
		if (BM_DEBUG) {
			fmt::print("Splitting new range [{0} - {1}): {2}\n",
			           range.begin.printable(),
			           range.end.printable(),
			           writeHot ? "hot" : "normal");
		}
		DebugBMTraceEvent(SevDebug, "BMSplitRangeBegin", bmData->id).detail("Range", range);
		state StorageMetrics estimated = wait(bmData->db->getStorageMetrics(range, CLIENT_KNOBS->TOO_MANY));

		DebugBMTraceEvent(SevDebug, "BMSplitRangeGotEstimate", bmData->id)
		    .detail("Range", range)
		    .detail("EstimatedBytes", estimated.bytes);

		if (BM_DEBUG) {
			fmt::print("Estimated bytes for [{0} - {1}): {2}\n",
			           range.begin.printable(),
			           range.end.printable(),
			           estimated.bytes);
		}

		int64_t splitThreshold = SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES;
		if (!initialSplit) {
			// If we have X MB target granule size, we want to do the initial split to split up into X MB chunks.
			// However, if we already have a granule that we are evaluating for split, if we split it as soon as it is
			// larger than X MB, we will end up with 2 X/2 MB granules.
			// To ensure an average size of X MB, we split granules at 4/3*X, so that they range between 2/3*X and
			// 4/3*X, averaging X
			splitThreshold = (splitThreshold * 4) / 3;
		}
		// if write-hot, we want to be able to split smaller, but not infinitely. Allow write-hot granules to be 3x
		// smaller
		// TODO knob?
		// TODO: re-evaluate after we have granule merging?
		if (writeHot) {
			splitThreshold /= 3;
		}
		CODE_PROBE(writeHot, "Change feed write hot split");
		if (estimated.bytes > splitThreshold) {
			// only split on bytes and write rate
			state StorageMetrics splitMetrics;
			splitMetrics.bytes = SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES;
			splitMetrics.bytesWrittenPerKSecond = SERVER_KNOBS->SHARD_SPLIT_BYTES_PER_KSEC;
			if (writeHot) {
				splitMetrics.bytesWrittenPerKSecond =
				    std::min(splitMetrics.bytesWrittenPerKSecond, estimated.bytesWrittenPerKSecond / 2);
				splitMetrics.bytesWrittenPerKSecond =
				    std::max(splitMetrics.bytesWrittenPerKSecond, SERVER_KNOBS->SHARD_MIN_BYTES_PER_KSEC);
			}
			splitMetrics.iosPerKSecond = splitMetrics.infinity;
			splitMetrics.bytesReadPerKSecond = splitMetrics.infinity;

			state PromiseStream<Key> resultStream;
			state Standalone<VectorRef<KeyRef>> keys;
			// SplitMetrics.bytes / 3 as min split size because of same splitThreshold logic above.
			// Estimated metrics doesn't work well when split metrics request spans multiple shards, since estimate is
			// for the whole range. Pass empty metrics instead of *estimated*, since it's already incorporated somewhat
			// in splitMetrics, and storage will trust its local byte sample over the estimate.
			state Future<Void> streamFuture = bmData->db->splitStorageMetricsStream(
			    resultStream, range, splitMetrics, StorageMetrics(), splitMetrics.bytes / 3);
			loop {
				try {
					Key k = waitNext(resultStream.getFuture());
					keys.push_back_deep(keys.arena(), k);
				} catch (Error& e) {
					if (e.code() != error_code_end_of_stream) {
						throw;
					}
					break;
				}
			}

			if (keys.size() > 2) {
				// We only need to align the keys if there is a proposed split.
				state Standalone<VectorRef<KeyRef>> fullKeys = wait(nextFullKeys(bmData, keys));
				BlobGranuleSplitPoints _splitPoints = wait(alignKeys(bmData, range, fullKeys));
				splitPoints = _splitPoints;

				// Enforce max split fanout for performance reasons. This mainly happens when a blob worker is behind.
				if (maxSplitPoints.present() && splitPoints.keys.size() > maxSplitPoints.get()) {
					// downsample full keys and re-align
					CODE_PROBE(true, "downsampling granule split because fanout too high");
					Standalone<VectorRef<KeyRef>> downsampledKeys;
					downsampledKeys.arena().dependsOn(fullKeys.arena());
					downsampledKeys.push_back(downsampledKeys.arena(), fullKeys.front());

					// since we include start + end boundaries here, only need maxSplitPoints - 2 split boundaries to
					// produce maxSplitPoints - 1 granules
					downsampleSplit(fullKeys, downsampledKeys, 1, fullKeys.size() - 1, maxSplitPoints.get() - 2);

					downsampledKeys.push_back(downsampledKeys.arena(), fullKeys.back());
					ASSERT(downsampledKeys.size() == maxSplitPoints.get());
					if (BM_DEBUG) {
						fmt::print("Downsampled split [{0} - {1}) from {2} -> {3} granules\n",
						           range.begin.printable(),
						           range.end.printable(),
						           splitPoints.keys.size() - 1,
						           maxSplitPoints.get() - 1);
					}

					wait(store(splitPoints, alignKeys(bmData, range, downsampledKeys)));
					ASSERT(splitPoints.keys.size() <= maxSplitPoints.get());
				}
			} else {
				splitPoints.keys = keys;
			}

			if (keys.size() > 2 && splitPoints.keys.size() == 2) {
				++bmData->stats.cantSplitAlignment;
				CODE_PROBE(true, "BM can't split granule because of key alignment");
				TraceEvent(keys.size() > 4 ? SevWarn : SevDebug, "BlobManagerCantSplitLargeAfterAlignment")
				    .detail("OriginalGranules", keys.size() - 1)
				    .detail("Bytes", estimated.bytes)
				    .detail("Range", range);
			}

			ASSERT(splitPoints.keys.size() >= 2);
			ASSERT(splitPoints.keys.front() == range.begin);
			ASSERT(splitPoints.keys.back() == range.end);
			DebugBMTraceEvent(SevDebug, "BMSplitRangeSplit", bmData->id)
			    .detail("Range", range)
			    .detail("NewRanges", splitPoints.keys.size() - 1);
			return splitPoints;
		} else {
			CODE_PROBE(writeHot, "Not splitting write-hot because granules would be too small");
			if (BM_DEBUG) {
				printf("Not splitting range\n");
			}
			DebugBMTraceEvent(SevDebug, "BMSplitRangeNotSplitting", bmData->id).detail("Range", range);
			splitPoints.keys.push_back_deep(splitPoints.keys.arena(), range.begin);
			splitPoints.keys.push_back_deep(splitPoints.keys.arena(), range.end);
			return splitPoints;
		}
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}
		// SplitStorageMetrics explicitly has a SevError if it gets an error, so no errors should propagate here
		TraceEvent(SevError, "BlobManagerUnexpectedErrorSplitRange", bmData->id)
		    .error(e)
		    .detail("Epoch", bmData->epoch);
		ASSERT_WE_THINK(false);

		// if not simulation, kill the BM
		if (bmData->iAmReplaced.canBeSet()) {
			bmData->iAmReplaced.sendError(e);
		}
		throw e;
	}
}

// Picks a worker with the fewest number of already assigned ranges.
// If there is a tie, picks one such worker at random.
ACTOR Future<UID> pickWorkerForAssign(Reference<BlobManagerData> bmData,
                                      Optional<std::pair<UID, Error>> previousFailure) {
	// wait until there are BWs to pick from
	loop {
		state bool wasZeroWorkers = false;
		while (bmData->workerStats.size() == 0) {
			wasZeroWorkers = true;
			CODE_PROBE(true, "BM wants to assign range, but no workers available");
			if (BM_DEBUG) {
				fmt::print("BM {0} waiting for blob workers before assigning granules\n", bmData->epoch);
			}
			DebugBMTraceEvent(SevDebug, "BMAssignNoWorkers", bmData->id)
			    .suppressFor(10.0)
			    .detail("Epoch", bmData->epoch);
			bmData->restartRecruiting.trigger();
			wait(bmData->recruitingStream.onChange() || bmData->foundBlobWorkers.getFuture());
		}
		if (wasZeroWorkers) {
			// Add a bit of delay. If we were at zero workers, don't immediately assign all granules to the first worker
			// we recruit
			wait(delay(0.1));
		}
		if (bmData->workerStats.size() != 0) {
			break;
		}
		// if in the post-zero workers delay, we went back down to zero workers, re-loop
	}

	int minGranulesAssigned = INT_MAX;

	// because lowest number of granules worker(s) might not exactly have the lowest memory for various reasons, if we
	// got blob_worker_full as the error last time, sometimes just pick a random worker that wasn't the last one we
	// tried
	if (bmData->workerStats.size() >= 2 && previousFailure.present() &&
	    previousFailure.get().second.code() == error_code_blob_worker_full && deterministicRandom()->coinflip()) {
		CODE_PROBE(true, "randomly picking worker due to blob_worker_full");
		std::vector<UID> randomWorkers;
		randomWorkers.reserve(bmData->workerStats.size());
		for (auto& it : bmData->workerStats) {
			if (it.first != previousFailure.get().first) {
				randomWorkers.push_back(it.first);
			}
		}
		ASSERT(!randomWorkers.empty());
		int randomIdx = deterministicRandom()->randomInt(0, randomWorkers.size());
		if (BM_DEBUG) {
			fmt::print("picked worker {0} randomly since previous attempt got blob_worker_full\n",
			           randomWorkers[randomIdx].toString().substr(0, 5));
		}

		return randomWorkers[randomIdx];
	}

	// recent granules assigned
	std::vector<int> medianCalc;
	// UID, total granules assigned, recent granules assigned
	std::vector<std::tuple<UID, int, int>> eligibleWorkers;

	Optional<int> excludeIfRecentOver;
	for (auto const& worker : bmData->workerStats) {
		UID currId = worker.first;

		// if previous attempt failed and that worker is still present, ignore it
		if (bmData->workerStats.size() >= 2 && previousFailure.present() && previousFailure.get().first == currId) {
			continue;
		}

		eligibleWorkers.push_back({ currId, worker.second.numGranulesAssigned, worker.second.recentGranulesAssigned });
		medianCalc.push_back(worker.second.recentGranulesAssigned);
	}

	if (SERVER_KNOBS->BLOB_MANAGER_ENABLE_MEDIAN_ASSIGNMENT_LIMITING && medianCalc.size() > 1 &&
	    bmData->recentBWAssignments.size() >=
	        SERVER_KNOBS->BLOB_MANAGER_MEDIAN_ASSIGNMENT_MIN_SAMPLES_PER_WORKER * bmData->workerStats.size()) {
		CODE_PROBE(true, "blob manager enabling median assignment limiting");
		// FIXME: make more efficient with quick select
		std::sort(medianCalc.begin(), medianCalc.end());
		// round down in case of even number of workers to be more conservative
		int medianIdx = (medianCalc.size() - 1) / 2;
		// protect against bad knob values
		double multiplyFactor = std::max(1.0, SERVER_KNOBS->BLOB_MANAGER_MEDIAN_ASSIGNMENT_ALLOWANCE);
		excludeIfRecentOver = std::max(1, (int)(multiplyFactor * medianCalc[medianIdx]));
	}

	std::vector<UID> finalEligibleWorkers;
	bool anyOverLimit = false;
	for (auto& it : eligibleWorkers) {
		UID currId = std::get<0>(it);
		int granulesAssigned = std::get<1>(it);
		int recentGranulesAssigned = std::get<2>(it);

		if (excludeIfRecentOver.present() && recentGranulesAssigned > excludeIfRecentOver.get()) {
			anyOverLimit = true;
			continue;
		}

		if (granulesAssigned <= minGranulesAssigned) {
			if (granulesAssigned < minGranulesAssigned) {
				finalEligibleWorkers.clear();
				minGranulesAssigned = granulesAssigned;
			}
			finalEligibleWorkers.emplace_back(currId);
		}
	}

	if (anyOverLimit) {
		CODE_PROBE(true, "BM excluding BW due to median assignment algorithm");
		++bmData->stats.granulesHitMedianLimit;
	}

	// pick a random worker out of the eligible workers
	ASSERT(finalEligibleWorkers.size() > 0);
	int idx = deterministicRandom()->randomInt(0, finalEligibleWorkers.size());
	if (BM_DEBUG) {
		fmt::print("picked worker {0}, which has a minimal number ({1}) of granules assigned\n",
		           finalEligibleWorkers[idx].toString().substr(0, 5),
		           minGranulesAssigned);
	}

	return finalEligibleWorkers[idx];
}

// circular dependency between handleRangeAssign and doRangeAssignment
static Future<Void> handleRangeAssign(Reference<BlobManagerData> bmData,
                                      RangeAssignment assignment,
                                      bool* didAssign = nullptr);

ACTOR Future<Void> doRangeAssignment(Reference<BlobManagerData> bmData,
                                     RangeAssignment assignment,
                                     Optional<UID> workerID,
                                     int64_t epoch,
                                     int64_t seqNo) {
	state bool blockedWaitingForWorker = false;
	// WorkerId is set, except in case of assigning to any worker. Then we pick the worker to assign to in here
	try {
		// inject delay into range assignments
		if (BUGGIFY_WITH_PROB(0.05)) {
			wait(delay(deterministicRandom()->random01()));
		} else {
			// otherwise, do delay(0) to ensure rest of code in calling handleRangeAssign runs, before this function can
			// recursively call handleRangeAssign on error
			wait(delay(0.0));
		}
		if (!workerID.present()) {
			ASSERT(assignment.isAssign && assignment.assign.get().type != AssignRequestType::Continue);

			blockedWaitingForWorker = true;
			if (!assignment.previousFailure.present()) {
				// if not already blocked, now blocked
				++bmData->stats.blockedAssignments;
			}

			UID _workerId = wait(pickWorkerForAssign(bmData, assignment.previousFailure));
			if (BM_DEBUG) {
				fmt::print("Chose BW {0} for seqno {1} in BM {2}\n", _workerId.toString(), seqNo, bmData->epoch);
			}
			workerID = _workerId;

			if (SERVER_KNOBS->BLOB_MANAGER_ENABLE_MEDIAN_ASSIGNMENT_LIMITING) {
				// this worker is guaranteed to be in the map because we just picked it from the map in the most recent
				// now()
				bmData->recentBWAssignments.push_back(workerID.get());
				bmData->workerStats[workerID.get()].recentGranulesAssigned++;

				if (bmData->recentBWAssignments.size() >=
				    SERVER_KNOBS->BLOB_MANAGER_MEDIAN_ASSIGNMENT_MAX_SAMPLES_PER_WORKER * bmData->workerStats.size()) {
					UID workerIdToPop = bmData->recentBWAssignments.front();
					bmData->recentBWAssignments.pop_front();
					// worker could no longer exist now
					auto it = bmData->workerStats.find(workerIdToPop);
					if (it != bmData->workerStats.end()) {
						it->second.recentGranulesAssigned--;
					}
				}
			}

			// We don't have to check for races with an overlapping assignment because it would insert over us in the
			// actor map, cancelling this actor before it got here
			bmData->workerAssignments.insert(assignment.keyRange, workerID.get());

			if (bmData->workerStats.count(workerID.get())) {
				bmData->workerStats[workerID.get()].numGranulesAssigned += 1;
			}

			if (!assignment.previousFailure.present()) {
				// if only blocked waiting for worker, now not blocked
				--bmData->stats.blockedAssignments;
			}
		}
	} catch (Error& e) {
		if (assignment.previousFailure.present() || blockedWaitingForWorker) {
			--bmData->stats.blockedAssignments;
		}
		throw e;
	}

	if (BM_DEBUG) {
		fmt::print("BM {0} {1} range [{2} - {3}) @ ({4}, {5}) to {6}\n",
		           bmData->epoch,
		           assignment.isAssign ? "assigning" : "revoking",
		           assignment.keyRange.begin.printable(),
		           assignment.keyRange.end.printable(),
		           epoch,
		           seqNo,
		           workerID.get().toString());
	}

	try {
		if (assignment.isAssign) {
			ASSERT(assignment.assign.present());
			ASSERT(!assignment.revoke.present());

			AssignBlobRangeRequest req;
			req.keyRange = KeyRangeRef(StringRef(req.arena, assignment.keyRange.begin),
			                           StringRef(req.arena, assignment.keyRange.end));
			req.managerEpoch = epoch;
			req.managerSeqno = seqNo;
			req.type = assignment.assign.get().type;

			// if that worker isn't alive anymore, add the range back into the stream
			if (bmData->workersById.count(workerID.get()) == 0) {
				throw no_more_servers();
			}
			state Future<Void> assignFuture = bmData->workersById[workerID.get()].assignBlobRangeRequest.getReply(req);

			if (BUGGIFY) {
				// wait for request to actually send
				wait(delay(0));
				if (bmData->maybeInjectTargetedRestart()) {
					throw blob_manager_replaced();
				}
			}

			wait(assignFuture);

			if (assignment.previousFailure.present()) {
				// previous assign failed and this one succeeded
				--bmData->stats.blockedAssignments;
			}

			return Void();
		} else {
			ASSERT(!assignment.assign.present());
			ASSERT(assignment.revoke.present());

			RevokeBlobRangeRequest req;
			req.keyRange = KeyRangeRef(StringRef(req.arena, assignment.keyRange.begin),
			                           StringRef(req.arena, assignment.keyRange.end));
			req.managerEpoch = epoch;
			req.managerSeqno = seqNo;
			req.dispose = assignment.revoke.get().dispose;

			// if that worker isn't alive anymore, this is a noop
			if (bmData->workersById.count(workerID.get())) {
				Future<Void> workerIsDead = Never();
				if (req.dispose) {
					auto it = bmData->workerStats.find(workerID.get());
					ASSERT(it != bmData->workerStats.end());
					workerIsDead = it->second.signalDead.getFuture();
				}
				choose {
					when(wait(bmData->workersById[workerID.get()].revokeBlobRangeRequest.getReply(req))) {}
					when(wait(workerIsDead)) {}
				}
			} else {
				return Void();
			}
		}
	} catch (Error& e) {
		TraceEvent("BlobManagerErrorDoRangeAssignment", bmData->id)
		    .errorUnsuppressed(e)
		    .suppressFor(10.0)
		    .detail("Epoch", bmData->epoch)
		    .detail("Range", assignment.keyRange)
		    .detail("IsAssign", assignment.isAssign)
		    .detail("IsBlocked", assignment.previousFailure.present());
		if (assignment.previousFailure.present()) {
			// previous assign failed, consider it unblocked if it's not a retriable error
			--bmData->stats.blockedAssignments;
		}
		state Error e2 = e;
		if (e.code() == error_code_operation_cancelled) {
			throw;
		}
		if (e.code() == error_code_blob_manager_replaced) {
			if (bmData->iAmReplaced.canBeSet()) {
				bmData->iAmReplaced.send(Void());
			}
			return Void();
		}
		if (e.code() == error_code_granule_assignment_conflict) {
			// Another blob worker already owns the range, don't retry.
			// And, if it was us that send the request to another worker for this range, this actor should have been
			// cancelled. So if it wasn't, it's likely that the conflict is from a new blob manager. Trigger the lock
			// check to make sure, and die if so.
			if (BM_DEBUG) {
				fmt::print("BM {0} got conflict assigning [{1} - {2}) to worker {3}, ignoring\n",
				           bmData->epoch,
				           assignment.keyRange.begin.printable(),
				           assignment.keyRange.end.printable(),
				           workerID.get().toString());
			}
			if (bmData->doLockCheck.canBeSet()) {
				bmData->doLockCheck.send(Void());
			}
			return Void();
		}

		if (e.code() != error_code_broken_promise && e.code() != error_code_no_more_servers &&
		    e.code() != error_code_blob_worker_full) {
			TraceEvent(SevWarn, "BlobManagerUnexpectedErrorDoRangeAssignment", bmData->id)
			    .error(e)
			    .detail("Epoch", bmData->epoch);
			ASSERT_WE_THINK(false);
			if (bmData->iAmReplaced.canBeSet()) {
				bmData->iAmReplaced.sendError(e);
			}
			throw;
		}

		if (e.code() == error_code_no_more_servers && assignment.isAssign &&
		    assignment.assign.get().type == AssignRequestType::Continue) {
			// If the BW the BM was telling to continue is already dead, just drop this as we already reassigned the
			// granule
			TraceEvent("BMDroppingContinueAssignment", bmData->id)
			    .detail("Epoch", bmData->epoch)
			    .detail("Seqno", seqNo);
			CODE_PROBE(true, "BM no more servers on continue", probe::decoration::rare);
			return Void();
		}

		// this assign failed and we will retry, consider it blocked until it successfully retries
		if (assignment.isAssign) {
			++bmData->stats.blockedAssignments;
		}

		if (e.code() == error_code_blob_worker_full) {
			CODE_PROBE(true, "blob worker too full");
			ASSERT(assignment.isAssign);
			try {
				if (assignment.previousFailure.present() &&
				    assignment.previousFailure.get().second.code() == error_code_blob_worker_full) {
					// if previous assignment also failed due to blob_worker_full, multiple workers are full, so wait
					// even longer
					CODE_PROBE(true, "multiple blob workers too full");
					wait(delayJittered(10.0));
				} else {
					wait(delayJittered(1.0)); // wait a bit before retrying
				}
			} catch (Error& e) {
				--bmData->stats.blockedAssignments;
				throw;
			}
		}

		CODE_PROBE(true, "BM retrying range assign");

		// We use reliable delivery (getReply), so the broken_promise means the worker is dead, and we may need to retry
		// somewhere else
		if (assignment.isAssign) {
			if (BM_DEBUG) {
				fmt::print("BM got error {0} assigning range [{1} - {2}) to worker {3}, requeueing\n",
				           e2.name(),
				           assignment.keyRange.begin.printable(),
				           assignment.keyRange.end.printable(),
				           workerID.get().toString());
			}

			DebugBMTraceEvent(SevDebug, "BMAssignReQueue", bmData->id)
			    .suppressFor(10.0)
			    .detail("Epoch", bmData->epoch)
			    .detail("Range", assignment.keyRange);

			// re-send revoke to queue to handle range being un-assigned from that worker before the new one
			RangeAssignment revokeOld;
			revokeOld.isAssign = false;
			revokeOld.worker = workerID;
			revokeOld.keyRange = assignment.keyRange;
			revokeOld.revoke = RangeRevokeData(false);

			handleRangeAssign(bmData, revokeOld);

			// send assignment back to queue as is, clearing designated worker if present
			// if we failed to send continue to the worker we thought owned the shard, it should be retried
			// as a normal assign
			ASSERT(assignment.assign.present());
			assignment.assign.get().type = AssignRequestType::Normal;
			assignment.worker.reset();
			std::pair<UID, Error> failure = { workerID.get(), e2 };
			assignment.previousFailure = failure;
			handleRangeAssign(bmData, assignment);
		} else {
			if (BM_DEBUG) {
				fmt::print("BM got error revoking range [{0} - {1}) from worker",
				           assignment.keyRange.begin.printable(),
				           assignment.keyRange.end.printable());
			}

			if (assignment.revoke.get().dispose) {
				if (BM_DEBUG) {
					printf(", retrying for dispose\n");
				}
				// send assignment back to queue as is, clearing designated worker if present
				assignment.worker.reset();
				handleRangeAssign(bmData, assignment);
				//
			} else {
				if (BM_DEBUG) {
					printf(", ignoring\n");
				}
			}
		}
	}
	return Void();
}

static Future<Void> handleRangeIsAssign(Reference<BlobManagerData> bmData,
                                        RangeAssignment assignment,
                                        int64_t seqNo,
                                        bool* didAssign) {
	// Ensure range isn't currently assigned anywhere, and there is only 1 intersecting range
	auto currentAssignments = bmData->workerAssignments.intersectingRanges(assignment.keyRange);
	int count = 0;
	UID workerId;
	Future<Void> result = Future<Void>(Void());
	if (didAssign) {
		*didAssign = false;
	}

	for (auto i = currentAssignments.begin(); i != currentAssignments.end(); ++i) {
		if (assignment.assign.get().type == AssignRequestType::Continue) {
			ASSERT(assignment.worker.present());
			if (i.range() != assignment.keyRange || i.cvalue() != assignment.worker.get()) {
				CODE_PROBE(true, "BM assignment out of date", probe::decoration::rare);
				if (BM_DEBUG) {
					fmt::print("Out of date re-assign for ({0}, {1}). Assignment must have changed while "
					           "checking split.\n  Reassign: [{2} - {3}): {4}\n  Existing: [{5} - {6}): {7}\n",
					           bmData->epoch,
					           seqNo,
					           assignment.keyRange.begin.printable(),
					           assignment.keyRange.end.printable(),
					           assignment.worker.get().toString().substr(0, 5),
					           i.begin().printable(),
					           i.end().printable(),
					           i.cvalue().toString().substr(0, 5));
				}
				DebugBMTraceEvent(SevDebug, "BMAssignOutOfDate", bmData->id)
				    .suppressFor(10.0)
				    .detail("Epoch", bmData->epoch)
				    .detail("ReassignRange", assignment.keyRange)
				    .detail("ReassignWorker", assignment.worker.get())
				    .detail("ExistingRange", KeyRangeRef(i.begin(), i.end()))
				    .detail("ExistingWorker", i.cvalue());
				return result;
			}
		}
		count++;
	}
	ASSERT(count == 1);

	bool forcePurging = bmData->isForcePurging(assignment.keyRange);

	if (forcePurging && assignment.previousFailure.present()) {
		--bmData->stats.blockedAssignments;
	}
	if (assignment.worker.present() && assignment.worker.get().isValid()) {
		if (BM_DEBUG) {
			fmt::print("BW {0} already chosen for seqno {1} in BM {2}\n",
			           assignment.worker.get().toString(),
			           seqNo,
			           bmData->id.toString());
		}
		workerId = assignment.worker.get();

		bmData->workerAssignments.insert(assignment.keyRange, workerId);

		// If we know about the worker and this is not a continue, then this is a new range for the worker
		if (assignment.assign.get().type == AssignRequestType::Continue) {
			// if it is a continue, don't cancel an in-flight re-assignment. Send to actor collection instead of
			// assignsInProgress
			result = doRangeAssignment(bmData, assignment, workerId, bmData->epoch, seqNo);
			bmData->addActor.send(result);
		} else {
			if (!forcePurging) {
				result = doRangeAssignment(bmData, assignment, workerId, bmData->epoch, seqNo);
				// FIXME: not handling error propagation correctly here
				bmData->assignsInProgress.insert(assignment.keyRange, result);
			}
			if (bmData->workerStats.count(workerId)) {
				bmData->workerStats[workerId].numGranulesAssigned += 1;
			}
		}
	} else {
		// Ensure the key boundaries are updated before we pick a worker
		bmData->workerAssignments.insert(assignment.keyRange, UID());
		ASSERT(assignment.assign.get().type != AssignRequestType::Continue);
		if (!forcePurging) {
			result = doRangeAssignment(bmData, assignment, Optional<UID>(), bmData->epoch, seqNo);
			// FIXME: not handling error propagation correctly here
			bmData->assignsInProgress.insert(assignment.keyRange, result);
		}
	}
	if (didAssign) {
		*didAssign = true;
	}
	return result;
}

static Future<Void> handleRangeIsRevoke(Reference<BlobManagerData> bmData,
                                        RangeAssignment assignment,
                                        int64_t seqNo,
                                        bool* didAssign) {
	Future<Void> result = Future<Void>(Void());
	if (didAssign) {
		*didAssign = false;
	}

	if (assignment.worker.present()) {
		// revoke this specific range from this specific worker. Either part of recovery or failing a worker
		if (bmData->workerStats.count(assignment.worker.get())) {
			bmData->workerStats[assignment.worker.get()].numGranulesAssigned -= 1;
		}
		// if this revoke matches the worker assignment state, mark the range as unassigned
		auto existingRange = bmData->workerAssignments.rangeContaining(assignment.keyRange.begin);
		if (existingRange.range() == assignment.keyRange && existingRange.cvalue() == assignment.worker.get()) {
			bmData->workerAssignments.insert(assignment.keyRange, UID());
		}
		result = doRangeAssignment(bmData, assignment, assignment.worker.get(), bmData->epoch, seqNo);
		bmData->addActor.send(result);
	} else {
		auto currentAssignments = bmData->workerAssignments.intersectingRanges(assignment.keyRange);
		for (auto& it : currentAssignments) {
			// ensure range doesn't truncate existing ranges
			if (it.begin() < assignment.keyRange.begin || it.end() > assignment.keyRange.end) {
				// the only case where this is ok is on startup when a BM is revoking old granules after reading
				// knownBlobRanges and seeing that some are no longer present.
				auto knownRanges = bmData->knownBlobRanges.intersectingRanges(it.range());
				bool inKnownBlobRanges = false;
				for (auto& r : knownRanges) {
					if (r.value()) {
						inKnownBlobRanges = true;
						break;
					}
				}
				bool forcePurging = bmData->isForcePurging(it.range());
				if (it.cvalue() != UID() || (inKnownBlobRanges && !forcePurging)) {
					fmt::print("Assignment [{0} - {1}): {2} truncates range [{3} - {4}) ({5}, {6})\n",
					           assignment.keyRange.begin.printable(),
					           assignment.keyRange.end.printable(),
					           it.cvalue().toString().substr(0, 5),
					           it.begin().printable(),
					           it.end().printable(),
					           inKnownBlobRanges,
					           forcePurging);
					// assert on condition again to make assertion failure better than "false"
					ASSERT(it.cvalue() == UID() && (!inKnownBlobRanges || forcePurging));
				}
			}

			// It is fine for multiple disjoint sub-ranges to have the same sequence number since they were part
			// of the same logical change

			if (bmData->workerStats.count(it.value())) {
				bmData->workerStats[it.value()].numGranulesAssigned -= 1;
			}

			if (didAssign) {
				*didAssign = true;
			}
			// revoke the range for the worker that owns it, not the worker specified in the revoke
			Future<Void> partRevoke = doRangeAssignment(bmData, assignment, it.value(), bmData->epoch, seqNo);
			result = result && partRevoke;
			bmData->addActor.send(partRevoke);
		}
		bmData->workerAssignments.insert(assignment.keyRange, UID());
	}

	bmData->assignsInProgress.cancel(assignment.keyRange);
	return result;
}

static Future<Void> handleRangeAssign(Reference<BlobManagerData> bmData, RangeAssignment assignment, bool* didAssign) {
	int64_t seqNo = bmData->seqNo;
	bmData->seqNo++;

	// modify the in-memory assignment data structures, and send request off to worker
	if (assignment.isAssign) {
		return handleRangeIsAssign(bmData, assignment, seqNo, didAssign);
	} else {
		return handleRangeIsRevoke(bmData, assignment, seqNo, didAssign);
	}
}

ACTOR Future<Void> checkManagerLock(Transaction* tr, Reference<BlobManagerData> bmData) {
	ValueReadResult currentLockValue = wait(tr->get(blobManagerEpochKey));
	ASSERT(currentLockValue.present());
	int64_t currentEpoch = decodeBlobManagerEpochValue(currentLockValue.get());
	if (currentEpoch != bmData->epoch) {
		if (BM_DEBUG) {
			fmt::print(
			    "BM {0} found new epoch {1} > {2} in lock check\n", bmData->id.toString(), currentEpoch, bmData->epoch);
		}
		if (bmData->iAmReplaced.canBeSet()) {
			bmData->iAmReplaced.send(Void());
		}

		throw blob_manager_replaced();
	}

	return Void();
}

ACTOR Future<Void> checkManagerLock(Reference<ReadYourWritesTransaction> tr, Reference<BlobManagerData> bmData) {
	wait(checkManagerLock(&(tr->getTransaction()), bmData));
	return Void();
}

ACTOR Future<BlobGranuleSplitPoints> getSplitPointsFromWrittenPointers(
    BulkLoadMetadata::WrittenFilePointersMap::RangeResultType const* writtenPointers,
    std::vector<int>* sortedToOriginalIdx,
    int64_t tenantId,
    KeyRangeRef range) {

	state BlobGranuleSplitPoints splitPoints;
	state KeyRangeMap<int> writtenPointerIdxByRange;
	writtenPointerIdxByRange.insert(range, -1);
	state int i;
	for (i = 0; i < writtenPointers->results.size(); i++) {
		KeyRange r = writtenPointers->results[i].second.keyRange;
		if (tenantId != TenantInfo::INVALID_TENANT) {
			r = r.withPrefix(TenantAPI::idToPrefix(tenantId));
		}
		writtenPointerIdxByRange.insert(r, i);
		if (i % 1000 == 999 || BUGGIFY_WITH_PROB(0.01)) {
			wait(yield());
		}
	}

	// fill gaps in key ranges for granule boundaries
	sortedToOriginalIdx->reserve(writtenPointers->results.size());
	auto ranges = writtenPointerIdxByRange.intersectingRanges(range);
	splitPoints.keys.push_back_deep(splitPoints.keys.arena(), range.begin);
	bool skippedFirst = false;
	for (auto& it : ranges) {
		if (it.value() == -1) {
			continue;
		}
		if (!skippedFirst) {
			skippedFirst = true;
		} else {
			splitPoints.keys.push_back_deep(splitPoints.keys.arena(), it.begin());
		}
		sortedToOriginalIdx->push_back(it.value());
		// FIXME: it'd be great to yield here if possible
	}
	splitPoints.keys.push_back_deep(splitPoints.keys.arena(), range.end);
	ASSERT_EQ(sortedToOriginalIdx->size(), writtenPointers->results.size());
	ASSERT_EQ(splitPoints.keys.size(), writtenPointers->results.size() + 1);

	return splitPoints;
}

// set up metadata for one granule using fileID as granuleID and feed create version as the snapshot version
ACTOR Future<Void> bulkLoadCreateGranuleMetadata(Reference<BlobManagerData> bmData,
                                                 Reference<ReadYourWritesTransaction> tr,
                                                 BulkLoadMetadata::FilePointerKey fileKey,
                                                 BulkLoadMetadata::WrittenFilePointerRef writtenPtr,
                                                 Version snapshotVersion,
                                                 Key bulkLoadID,
                                                 KeyRange expandedKeyRange) {
	state Key lockKey = blobGranuleLockKeyFor(expandedKeyRange);
	ValueReadResult alreadyExists = wait(tr->get(lockKey));
	if (alreadyExists.present()) {
		return Void();
	}

	// lock key
	state UID granuleID = bulkLoadIdToUID(fileKey.second);
	tr->set(lockKey, blobGranuleLockValueFor(bmData->epoch, bmData->seqNo, granuleID));

	// history DAG entry
	Standalone<BlobGranuleHistoryValue> historyValue;
	historyValue.granuleID = granuleID;
	tr->set(blobGranuleHistoryKeyFor(expandedKeyRange, snapshotVersion), blobGranuleHistoryValueFor(historyValue));

	// file pointer
	Key snapshotFileKey = blobGranuleFileKeyFor(granuleID, snapshotVersion, 'S');
	Key snapshotFileValue = blobGranuleFileValueFor(writtenPtr.createdPointer.filename.toString(),
	                                                0,
	                                                writtenPtr.physicalSize,
	                                                writtenPtr.physicalSize,
	                                                writtenPtr.logicalSize,
	                                                writtenPtr.createdPointer.cipherKeysMeta);
	tr->set(snapshotFileKey, snapshotFileValue);

	return Void();
}

ACTOR Future<Void> bulkLoadDestroyGranuleMetadata(Reference<BlobManagerData> bmData,
                                                  Transaction* tr,
                                                  BulkLoadMetadata::FilePointerKey fileKey,
                                                  BulkLoadMetadata::WrittenFilePointerRef writtenPtr,
                                                  KeyRange expandedKeyRange) {
	state UID granuleID = bulkLoadIdToUID(fileKey.second);
	Key cfKey = granuleIDToCFKey(granuleID);

	// feed created before granule metadata - always destroy even if no granule metadata
	DebugBulkLoadTraceEvent(SevDebug, "BulkLoadDestroyGranule", bmData->id)
	    .detail("BulkLoadID", fileKey.first)
	    .detail("CFKey", cfKey)
	    .detail("Range", expandedKeyRange);
	wait(updateChangeFeed(tr, cfKey, ChangeFeedStatus::CHANGE_FEED_DESTROY, expandedKeyRange));

	Optional<GranuleHistory> history = wait(getLatestGranuleHistory(tr, expandedKeyRange));
	if (!history.present()) {
		// assume already cleaned up
		return Void();
	}

	tr->clear(blobGranuleHistoryKeyFor(expandedKeyRange, history->version));
	tr->clear(blobGranuleLockKeyFor(expandedKeyRange));
	// FIXME: disable verification eventually?
	state KeyRange fileRange = blobGranuleFileKeyRangeFor(granuleID);
	RangeReadResult r = wait(tr->getRange(fileRange, 2));
	// keep debugging in, just in case this happenes again
	if (g_network->isSimulated() && r.size() != 1) {
		fmt::print("ERROR: granule [{0} - {1}) has {2} files!:\n",
		           expandedKeyRange.begin.printable(),
		           expandedKeyRange.end.printable(),
		           r.size());
		for (auto& it : r) {
			UID gid;
			Version version;
			uint8_t fileType;
			Standalone<StringRef> filename;
			int64_t offset;
			int64_t length;
			int64_t fullFileLength;
			int64_t logicalSize;
			Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta;

			std::tie(gid, version, fileType) = decodeBlobGranuleFileKey(it.key);
			std::tie(filename, offset, length, fullFileLength, logicalSize, cipherKeysMeta) =
			    decodeBlobGranuleFileValue(it.value);
			fmt::print("  {0}) {1}\n", version, filename.printable());
		}
	}

	ASSERT(r.size() == 1);
	tr->clear(fileRange);

	return Void();
}

// FIXME: unit test some of this stuff?
ACTOR Future<BlobGranuleSplitPoints> bulkLoadInitialSplitRange(Reference<BlobManagerData> bmData,
                                                               KeyRange rangeRemaining,
                                                               BulkLoadID bulkLoadID,
                                                               KeyRange originalRange) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);

	state BlobGranuleSplitPoints splitPoints;
	state Optional<BulkLoadIDKey> idKey;

	DebugBulkLoadTraceEvent(SevDebug, "BulkLoadInitialSplit", bmData->id)
	    .detail("BulkLoadID", bulkLoadID)
	    .detail("Stage", "Init")
	    .detail("RangeRemaining", rangeRemaining)
	    .detail("OriginalRange", originalRange);

	state BulkLoadMetadata::WrittenFilePointersMap::RangeResultType writtenPointers;
	state std::pair<BulkLoadMetadata::FilePointerKey, BulkLoadMetadata::FilePointerKey> idRange =
	    BulkLoadMetadata::allFilePointersForLoadRange(bulkLoadID);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			// try to find tenant id + bulk load id and check that it still exists.
			// would have been easier if bulk load range map also had tenant id
			state Optional<BulkLoadStatus::State> currentState;
			if (!idKey.present()) {
				// either no tenant, or with tenant prefix
				state BulkLoadIDKey candidateIDKey = { TenantInfo::INVALID_TENANT, bulkLoadID };
				Optional<BulkLoadStatus> noTenantStatus = wait(BulkLoadMetadata::statusMap().get(tr, candidateIDKey));

				if (noTenantStatus.present()) {
					idKey = candidateIDKey;
					currentState = noTenantStatus->state;
				} else {
					if (TenantAPI::withinSingleTenant(originalRange)) {
						int64_t tenantId = TenantAPI::extractTenantIdFromKeyRef(originalRange.begin);
						candidateIDKey = { tenantId, bulkLoadID };
						Optional<BulkLoadStatus> withTenantStatus =
						    wait(BulkLoadMetadata::statusMap().get(tr, candidateIDKey));
						if (withTenantStatus.present()) {
							idKey = candidateIDKey;
							currentState = withTenantStatus->state;
						}
					}
				}
				if (!idKey.present()) {
					CODE_PROBE(true, "cannot find bulk load id registered for blob manager blobbify");
					// must have been aborted then cleaned up, but, weird
					TraceEvent(SevWarn, "BulkLoadBlobbifyingGone", bmData->id)
					    .detail("Epoch", bmData->epoch)
					    .detail("BulkLoadID", bulkLoadID.printable());
					throw bulk_load_invalid_state_transition();
				}

				DebugBulkLoadTraceEvent(SevDebug, "BulkLoadInitialSplit", bmData->id)
				    .detail("BulkLoadID", bulkLoadID)
				    .detail("Stage", "FoundIDKey")
				    .detail("TenantId", idKey->first);
			} else {
				// still check bulk load status on retry to ensure load is active once we know idkey
				Optional<BulkLoadStatus> updatedStatus = wait(BulkLoadMetadata::statusMap().get(tr, idKey.get()));
				if (!updatedStatus.present()) {
					CODE_PROBE(true, "bulk load no longer present after retry", probe::decoration::rare);
					// must have been aborted then cleaned up, but, weird
					TraceEvent(SevWarn, "BulkLoadBlobbifyingGoneOnRetry", bmData->id)
					    .detail("Epoch", bmData->epoch)
					    .detail("BulkLoadID", bulkLoadID.printable());
					throw bulk_load_invalid_state_transition();
				}
				currentState = updatedStatus->state;
			}
			// if idkey present still need to check that we're in staging blob validated
			ASSERT(idKey.present());
			ASSERT(currentState.present());
			if (currentState.get() != BulkLoadStatus::State::STAGING_BLOB_VALIDATED) {
				CODE_PROBE(true, "cannot find bulk load id registered for blob manager blobbify");
				// must have been aborted then cleaned up, but, weird
				DebugBulkLoadTraceEvent(SevDebug, "BulkLoadInitialSplit", bmData->id)
				    .detail("BulkLoadID", bulkLoadID)
				    .detail("Stage", "LoadPointersError")
				    .detail("State", static_cast<int>(currentState.get()))
				    .detail("TenantId", idKey->first);
				throw bulk_load_invalid_state_transition();
			}

			wait(store(writtenPointers,
			           BulkLoadMetadata::writtenFilePointersMap().getRange(
			               tr, idRange.first, idRange.second, CLIENT_KNOBS->TOO_MANY)));
			ASSERT(!writtenPointers.more);

			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	DebugBulkLoadTraceEvent(SevDebug, "BulkLoadInitialSplit", bmData->id)
	    .detail("BulkLoadID", bulkLoadID)
	    .detail("Stage", "Start")
	    .detail("RangeRemaining", rangeRemaining)
	    .detail("OriginalRange", originalRange)
	    .detail("Files", writtenPointers.results.size())
	    .detail("TenantId", idKey->first);

	CODE_PROBE(rangeRemaining != originalRange, "bulk load initial split picking up partial job from previous BM");

	// need to create granule metadata for each granule. a previous BM may already have done some but this is
	// idempotent
	state int startIdx = 0;
	state int batchSize = 100;
	if (BUGGIFY_WITH_PROB(0.1)) {
		batchSize = deterministicRandom()->randomInt(1, 10);
	}

	state std::vector<int> sortedToOriginalIdx;
	wait(store(splitPoints,
	           getSplitPointsFromWrittenPointers(&writtenPointers, &sortedToOriginalIdx, idKey->first, originalRange)));
	// skip part previous BM already did, if present
	while (startIdx < splitPoints.keys.size() && splitPoints.keys[startIdx] < rangeRemaining.begin) {
		startIdx++;
		if (startIdx % 1000 == 999 || BUGGIFY_WITH_PROB(0.001)) {
			wait(yield());
		}
	}
	ASSERT_LT(startIdx, splitPoints.keys.size());
	ASSERT_EQ(rangeRemaining.begin, splitPoints.keys[startIdx]);
	ASSERT_EQ(startIdx > 0, rangeRemaining != originalRange);

	if (BM_BULKLOAD_DEBUG) {
		fmt::print("InitialSplit for load ({0}):\n", writtenPointers.results.size());
		for (auto& it : splitPoints.keys) {
			fmt::print("  {0}\n", it.printable());
		}
	}

	state int i;

	loop {
		// for each batch
		state Version feedCreateVersion;
		tr->reset();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				// for each txn, check manager lock and that load is still staging
				wait(checkManagerLock(tr, bmData));
				Optional<BulkLoadStatus> status = wait(BulkLoadMetadata::statusMap().get(tr, idKey.get()));
				// since we cached info, on a re-read it could be non-present if bulk load finished by us or someone
				// else
				if (!status.present() || status.get().state != BulkLoadStatus::State::STAGING_BLOB_VALIDATED) {
					int stateInt = status.present() ? static_cast<int>(status->state) : -1;
					DebugBulkLoadTraceEvent(SevDebug, "BulkLoadInitialSplit", bmData->id)
					    .detail("BulkLoadID", bulkLoadID)
					    .detail("Stage", "CreateFeedError")
					    .detail("State", stateInt)
					    .detail("TenantId", idKey.get().first);
					throw bulk_load_invalid_state_transition();
				}
				// need to create change feeds as of version <= snapshot version, which means load file version has to
				// be after that for feed
				std::vector<Future<Void>> feedCreates;
				feedCreates.reserve(batchSize);

				for (i = 0; i < batchSize && startIdx + i < writtenPointers.results.size(); i++) {
					int sortedIdx = startIdx + i;
					int originalIdx = sortedToOriginalIdx[sortedIdx];
					auto& fileKey = writtenPointers.results[originalIdx].first;
					KeyRange expandedKeyRange =
					    KeyRangeRef(splitPoints.keys[sortedIdx], splitPoints.keys[sortedIdx + 1]);
					Key cfKey = granuleIDToCFKey(bulkLoadIdToUID(fileKey.second));
					DebugBulkLoadTraceEvent(SevDebug, "BulkLoadInitialSplit", bmData->id)
					    .detail("BulkLoadID", bulkLoadID)
					    .detail("Stage", "CreateFeed")
					    .detail("CFKey", cfKey)
					    .detail("Range", expandedKeyRange);
					feedCreates.push_back(
					    updateChangeFeed(tr, cfKey, ChangeFeedStatus::CHANGE_FEED_CREATE, expandedKeyRange));
				}
				wait(waitForAll(feedCreates));

				feedCreateVersion = tr->getReadVersion().get();
				wait(tr->commit());
				Version committedVersion = tr->getCommittedVersion();
				// committed version will be -1 if we did only reads, which can happen if we got commit unknown result
				// but the previous round actually completed (because all of the feed creates are no-op). In that case,
				// the read version already had the feeds visible and created as of that point, so we can use that as
				// the start version instead
				feedCreateVersion = std::max(feedCreateVersion, committedVersion);
				break;
			} catch (Error& e) {
				DebugBulkLoadTraceEvent(SevDebug, "BulkLoadInitialSplit", bmData->id)
				    .errorUnsuppressed(e)
				    .detail("BulkLoadID", bulkLoadID)
				    .detail("Stage", "ErrorCreatingChangeFeeds")
				    .detail("UpThrough", startIdx + batchSize);
				wait(tr->onError(e));
			}
		}

		DebugBulkLoadTraceEvent(SevDebug, "BulkLoadInitialSplit", bmData->id)
		    .detail("BulkLoadID", bulkLoadID)
		    .detail("Stage", "CreatedChangeFeeds")
		    .detail("CommitVersion", feedCreateVersion)
		    .detail("StartIdx", startIdx)
		    .detail("NewStartIdx", startIdx + batchSize)
		    .detail("Total", writtenPointers.results.size());
		ASSERT(feedCreateVersion >= 0);

		if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
			wait(delay(0)); // should be cancelled
			ASSERT(false);
		}

		tr->reset();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				// for each txn, check manager lock and that load is still staging
				wait(checkManagerLock(tr, bmData));
				Optional<BulkLoadStatus> status = wait(BulkLoadMetadata::statusMap().get(tr, idKey.get()));
				// since we cached info, on a re-read it could be non-present if bulk load finished by us or someone
				// else
				if (!status.present() || status.get().state != BulkLoadStatus::State::STAGING_BLOB_VALIDATED) {
					int stateInt = status.present() ? static_cast<int>(status->state) : -1;
					DebugBulkLoadTraceEvent(SevDebug, "BulkLoadInitialSplit", bmData->id)
					    .detail("BulkLoadID", bulkLoadID)
					    .detail("Stage", "CreateGranulesError")
					    .detail("State", stateInt)
					    .detail("TenantId", idKey.get().first);
					throw bulk_load_invalid_state_transition();
				}

				KeyRange batchRange =
				    KeyRangeRef(splitPoints.keys[startIdx],
				                splitPoints.keys[std::min<int>(startIdx + batchSize, writtenPointers.results.size())]);
				wait(krmSetRange(tr, blobGranuleMappingKeys.begin, batchRange, blobGranuleMappingValueFor(UID())));

				std::vector<Future<Void>> granuleMetadataCreates;
				granuleMetadataCreates.reserve(batchSize);
				for (i = 0; i < batchSize && startIdx + i < writtenPointers.results.size(); i++) {
					int sortedIdx = startIdx + i;
					int originalIdx = sortedToOriginalIdx[sortedIdx];
					KeyRange expandedKeyRange =
					    KeyRangeRef(splitPoints.keys[sortedIdx], splitPoints.keys[sortedIdx + 1]);
					granuleMetadataCreates.push_back(
					    bulkLoadCreateGranuleMetadata(bmData,
					                                  tr,
					                                  writtenPointers.results[originalIdx].first,
					                                  writtenPointers.results[originalIdx].second,
					                                  feedCreateVersion,
					                                  bulkLoadID,
					                                  expandedKeyRange));
					if (i != 0) {
						tr->set(splitPoints.keys[sortedIdx].withPrefix(blobGranuleMappingKeys.begin),
						        blobGranuleMappingValueFor(UID()));
					}
				}

				wait(waitForAll(granuleMetadataCreates));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				DebugBulkLoadTraceEvent(SevDebug, "BulkLoadInitialSplit", bmData->id)
				    .errorUnsuppressed(e)
				    .detail("BulkLoadID", bulkLoadID)
				    .detail("Stage", "ErrorCreatingGranules")
				    .detail("UpThrough", startIdx + batchSize);
				wait(tr->onError(e));
			}
		}

		DebugBulkLoadTraceEvent(SevDebug, "BulkLoadInitialSplit", bmData->id)
		    .detail("BulkLoadID", bulkLoadID)
		    .detail("Stage", "CreatedGranules")
		    .detail("UpThrough", startIdx + batchSize);

		startIdx += batchSize;

		if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
			wait(delay(0)); // should be cancelled
			ASSERT(false);
		}

		if (startIdx >= writtenPointers.results.size()) {
			break;
		}
	}

	DebugBulkLoadTraceEvent(SevDebug, "BulkLoadInitialSplit", bmData->id)
	    .detail("BulkLoadID", bulkLoadID)
	    .detail("Stage", "Done");

	return splitPoints;
}

ACTOR Future<Void> writeInitialGranuleMapping(Reference<BlobManagerData> bmData, BlobGranuleSplitPoints splitPoints) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	// don't do too many in one transaction
	state int i = 0;
	state int transactionChunkSize = BUGGIFY ? deterministicRandom()->randomInt(2, 5) : 1000;
	while (i < splitPoints.keys.size() - 1) {
		CODE_PROBE(i > 0, "multiple transactions for large granule split");
		tr->reset();
		state int j = 0;
		loop {
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(checkManagerLock(tr, bmData));
				// Instead of doing a krmSetRange for each granule, because it does a read-modify-write, we do one
				// krmSetRange for the whole batch, and then just individual sets for each intermediate boundary This
				// does one read per transaction instead of N serial reads per transaction
				state int endIdx = std::min(i + transactionChunkSize, (int)(splitPoints.keys.size() - 1));
				wait(krmSetRange(tr,
				                 blobGranuleMappingKeys.begin,
				                 KeyRangeRef(splitPoints.keys[i], splitPoints.keys[endIdx]),
				                 blobGranuleMappingValueFor(UID())));
				for (j = 0; i + j < endIdx; j++) {
					if (splitPoints.boundaries.count(splitPoints.keys[i + j])) {
						tr->set(blobGranuleMergeBoundaryKeyFor(splitPoints.keys[i + j]),
						        blobGranuleMergeBoundaryValueFor(splitPoints.boundaries[splitPoints.keys[i + j]]));
					}
					tr->set(splitPoints.keys[i + j].withPrefix(blobGranuleMappingKeys.begin),
					        blobGranuleMappingValueFor(UID()));
				}
				wait(tr->commit());

				// Update BlobGranuleMergeBoundary in-memory state.
				for (int k = i; k < i + j; k++) {
					KeyRef beginKey = splitPoints.keys[k];
					if (splitPoints.boundaries.count(beginKey)) {
						bmData->mergeBoundaries[beginKey] = splitPoints.boundaries[beginKey];
					}
				}
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
				j = 0;
			}
		}
		i += j;
	}
	if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
		wait(delay(0)); // should be cancelled
		ASSERT(false);
	}
	return Void();
}

ACTOR Future<Void> initialSplitRange(Reference<BlobManagerData> bmData, KeyRange range) {
	state Transaction tr(bmData->db);
	state Optional<BulkLoadID> bulkLoadID;
	state Optional<KeyRange> originalRange;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<BulkLoadMetadata::RangeMap::RangeValue> bulkLoadRange =
			    wait(BulkLoadMetadata::rangeMap().getRangeForKey(&tr, range.begin));
			if (!bulkLoadRange.present() || bulkLoadRange->value.empty()) {
				break;
			}
			// we go from start to end of the range, so the previous BM may have done some of it
			ASSERT_GE(range.begin, bulkLoadRange->range.begin);
			ASSERT_LT(range.begin, bulkLoadRange->range.end);
			ASSERT_EQ(range.end, bulkLoadRange->range.end);

			bulkLoadID = bulkLoadRange->value;
			originalRange = KeyRangeRef(bulkLoadRange->range.begin, bulkLoadRange->range.end);
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	state BlobGranuleSplitPoints splitPoints;
	if (bulkLoadID.present()) {
		wait(store(splitPoints, bulkLoadInitialSplitRange(bmData, range, bulkLoadID.get(), originalRange.get())));
	} else {
		DebugBulkLoadTraceEvent(SevDebug, "InitialSplitNoBulkLoad").detail("Range", range);

		if (bmData->isForcePurging(range)) {
			CODE_PROBE(true, "ignoring granule initial split because force purging");
			if (BM_DEBUG) {
				fmt::print("BM {0} Split client range [{1} - {2}) because force purging:\n",
				           bmData->epoch,
				           range.begin.printable(),
				           range.end.printable());
			}
			return Void();
		}

		// handle normal non-bulk load case
		wait(store(splitPoints, splitRange(bmData, range, false, true)));
		// Write to DB BEFORE sending assign requests, so that if manager dies before/during, new manager
		// picks up the same ranges
		wait(writeInitialGranuleMapping(bmData, splitPoints));
	}

	if (BM_DEBUG) {
		fmt::print("BM {0} Split client range [{1} - {2}) into {3} ranges:\n",
		           bmData->epoch,
		           splitPoints.keys[0].printable(),
		           splitPoints.keys[splitPoints.keys.size() - 1].printable(),
		           splitPoints.keys.size() - 1);
	}

	DebugBMTraceEvent(SevDebug, "ClientBlobRangeSplit", bmData->id)
	    .suppressFor(10.0)
	    .detail("Epoch", bmData->epoch)
	    .detail("Range", KeyRangeRef(splitPoints.keys.front(), splitPoints.keys.back()))
	    .detail("Count", splitPoints.keys.size() - 1);

	for (int i = 0; i < splitPoints.keys.size() - 1; i++) {
		KeyRange range = KeyRange(KeyRangeRef(splitPoints.keys[i], splitPoints.keys[i + 1]));
		// only add the client range if this is the first BM or it's not already assigned
		if (BM_DEBUG) {
			fmt::print("    [{0} - {1})\n", range.begin.printable().c_str(), range.end.printable().c_str());
		}

		RangeAssignment ra;
		ra.isAssign = true;
		ra.keyRange = range;
		ra.assign = RangeAssignmentData(); // type=normal
		handleRangeAssign(bmData, ra);
	}
	return Void();
}

ACTOR Future<Void> loadTenantMap(Reference<ReadYourWritesTransaction> tr, Reference<BlobManagerData> bmData) {
	state KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenantResults;
	wait(store(tenantResults,
	           TenantMetadata::tenantMap().getRange(tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1)));
	ASSERT(tenantResults.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER && !tenantResults.more);

	bmData->tenantData.addTenants(tenantResults.results);

	return Void();
}

ACTOR Future<Void> monitorTenants(Reference<BlobManagerData> bmData) {
	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(loadTenantMap(tr, bmData));

				state Future<Void> watchChange = TenantMetadata::lastTenantModification().watch(tr);
				wait(tr->commit());
				wait(watchChange);
				tr->reset();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}
}

// FIXME: better way to load tenant mapping?
ACTOR Future<Void> monitorClientRanges(Reference<BlobManagerData> bmData) {
	state Optional<Value> lastChangeKeyValue;
	state bool needToCoalesce = bmData->epoch > 1;
	state bool firstLoad = true;
	state Version lastChangeLogVersion = -1;

	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);

		if (BM_DEBUG) {
			printf("Blob manager checking for range updates\n");
		}
		state VectorRef<KeyRangeRef> rangesToAdd;
		state VectorRef<KeyRangeRef> rangesToRemove;
		state Arena ar;
		state ValueReadResult ckvBegin;
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				// read change key at this point along with data
				wait(store(ckvBegin, tr->get(blobRangeChangeKey)));

				if (firstLoad || !SERVER_KNOBS->BG_USE_BLOB_RANGE_CHANGE_LOG) {
					// FIXME: enable this to read across multiple transactions in next release
					state RangeResult results = wait(krmGetRanges(tr,
					                                              blobRangeKeys.begin,
					                                              KeyRange(normalKeys),
					                                              CLIENT_KNOBS->TOO_MANY,
					                                              GetRangeLimits::BYTE_LIMIT_UNLIMITED));
					ASSERT_WE_THINK(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);
					if (results.more || results.size() >= CLIENT_KNOBS->TOO_MANY) {
						TraceEvent(SevError, "BlobManagerTooManyClientRanges", bmData->id)
						    .detail("Epoch", bmData->epoch)
						    .detail("ClientRanges", results.size() - 1);
						wait(delay(600));
						if (bmData->iAmReplaced.canBeSet()) {
							bmData->iAmReplaced.sendError(internal_error());
						}
						throw internal_error();
					}

					// TODO better way to do this!
					bmData->mergeHardBoundaries.clear();
					for (auto& it : results) {
						bmData->mergeHardBoundaries[it.key] = true;
					}
					ar.dependsOn(results.arena());

					updateClientBlobRanges(
					    bmData->epoch, &bmData->knownBlobRanges, results, ar, &rangesToAdd, &rangesToRemove);

					if (needToCoalesce) {
						// recovery has granules instead of known ranges in here. We need to do so to identify any parts
						// of known client ranges the last manager didn't finish blob-ifying. To coalesce the map, we
						// simply override known ranges with the current DB ranges after computing rangesToAdd +
						// rangesToRemove
						needToCoalesce = false;

						for (int i = 0; i < results.size() - 1; i++) {
							bool active = isBlobRangeActive(results[i].value);
							bmData->knownBlobRanges.insert(KeyRangeRef(results[i].key, results[i + 1].key), active);
						}
					}

					// version of lowest read of the krm space
					if (SERVER_KNOBS->BG_USE_BLOB_RANGE_CHANGE_LOG) {
						lastChangeLogVersion = tr->getReadVersion().get();
					}
				} else {
					// read change log
					// re-reading stuff already in the map should be idempotent
					KeyRange readRange =
					    KeyRangeRef(blobRangeChangeLogReadKeyFor(lastChangeLogVersion + 1), blobRangeChangeLogKeys.end);
					state RangeReadResult changeLog = wait(tr->getRange(readRange, CLIENT_KNOBS->TOO_MANY));

					ASSERT_WE_THINK(!changeLog.more && changeLog.size() < CLIENT_KNOBS->TOO_MANY);
					if (changeLog.more || changeLog.size() >= CLIENT_KNOBS->TOO_MANY) {
						TraceEvent(SevError, "BlobManagerTooManyClientRangeChanges", bmData->id)
						    .detail("Epoch", bmData->epoch)
						    .detail("ClientRangeChanges", changeLog.size());
						wait(delay(600));
						if (bmData->iAmReplaced.canBeSet()) {
							bmData->iAmReplaced.sendError(internal_error());
						}
						throw internal_error();
					}

					ar.dependsOn(changeLog.arena());

					state int i;
					for (i = 0; i < changeLog.size(); i++) {
						Standalone<BlobRangeChangeLogRef> v = decodeBlobRangeChangeLogValue(changeLog[i].value);
						KeyRangeRef range = v.range & normalKeys;
						bool active = isBlobRangeActive(v.value);
						if (BM_DEBUG) {
							fmt::print("DBG: BM {0} got range [{1} - {2}): {3} from change log @ {4}\n",
							           bmData->epoch,
							           range.begin.printable(),
							           range.end.printable(),
							           active ? "T" : "F",
							           tr->getReadVersion().get());
						}
						handleClientBlobRange(bmData->epoch,
						                      &bmData->knownBlobRanges,
						                      ar,
						                      &rangesToAdd,
						                      &rangesToRemove,
						                      range.begin,
						                      range.end,
						                      active);

						if (active) {
							bmData->mergeHardBoundaries[range.begin] = true;
							bmData->mergeHardBoundaries[range.end] = true;
						} else {
							// clear any merge boundaries between ranges if this was a group unblobbify
							auto it = bmData->mergeHardBoundaries.lower_bound(keyAfter(range.begin));
							while (it != bmData->mergeHardBoundaries.end() && it->first < range.end) {
								it = bmData->mergeHardBoundaries.erase(it);
							}
						}

						wait(yield());
					}

					lastChangeLogVersion = tr->getReadVersion().get();
				}
				break;
			} catch (Error& e) {
				if (BM_DEBUG) {
					fmt::print("Blob manager got error looking for range updates {}\n", e.name());
				}
				wait(tr->onError(e));
			}
		}

		state std::vector<Future<Void>> splitFutures;
		// Divide new ranges up into equal chunks by using SS byte sample
		for (KeyRangeRef range : rangesToAdd) {
			TraceEvent("ClientBlobRangeAdded", bmData->id).detail("Range", range);
			// add client range as known "granule" until we determine initial split, in case a purge or
			// unblobbify comes in before we finish splitting

			// TODO can remove validation eventually
			auto r = bmData->workerAssignments.intersectingRanges(range);
			for (auto& it : r) {
				ASSERT(it.cvalue() == UID());
			}
			bmData->workerAssignments.insert(range, UID());

			// start initial split for range
			splitFutures.push_back(initialSplitRange(bmData, range));
		}

		for (KeyRangeRef range : rangesToRemove) {
			TraceEvent("ClientBlobRangeRemoved", bmData->id).detail("Range", range);
			if (BM_DEBUG) {
				fmt::print("BM Got range to revoke [{0} - {1})\n", range.begin.printable(), range.end.printable());
			}

			RangeAssignment ra;
			ra.isAssign = false;
			ra.keyRange = range;
			ra.revoke = RangeRevokeData(true); // dispose=true
			handleRangeAssign(bmData, ra);
		}

		if (firstLoad) {
			bmData->loadedClientRanges.send(Void());
			firstLoad = false;
		}
		bmData->loadedClientRangesRound.set(bmData->loadedClientRangesRound.get() + 1);

		for (auto& f : splitFutures) {
			try {
				wait(f);
			} catch (Error& e) {
				if (e.code() == error_code_bulk_load_invalid_state_transition) {
					// load aborted while blobbifiyng, ignore
					CODE_PROBE(true, "bulk load aborted while blobbifying");
					continue;
				}
				throw e;
			}
		}

		// the version of the ranges we processed is the one read alongside the ranges
		lastChangeKeyValue = ckvBegin;
		state Future<Void> watchFuture;

		loop {
			try {
				// do a new transaction, check for change in change key, watch if none
				tr->reset();
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				ValueReadResult ckvEnd = wait(tr->get(blobRangeChangeKey));

				// clean up unreadable parts of change log (use 2* max read life versions because i'm paranoid)
				Version changeLogCleanupVersion =
				    tr->getReadVersion().get() - 2 * SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS;
				// guard if version is tiny and we underflowed, or if query took longer than 2* mvcc window, we don't
				// want to delete stuff we haven't read
				changeLogCleanupVersion = std::max<Version>(0, std::min(lastChangeLogVersion, changeLogCleanupVersion));
				KeyRange cleanupKeyRange =
				    KeyRangeRef(blobRangeChangeLogKeys.begin, blobRangeChangeLogReadKeyFor(changeLogCleanupVersion));
				if (ckvEnd == lastChangeKeyValue) {
					watchFuture = tr->watch(blobRangeChangeKey); // watch for change in key
					// FIXME: clean up blob range change log to stuff that isn't readable
					tr->clear(cleanupKeyRange);
					wait(checkManagerLock(tr, bmData));
					wait(tr->commit());
					if (BM_DEBUG) {
						fmt::print("Blob manager done processing client ranges @ {0}, awaiting update\n",
						           tr->getCommittedVersion());
					}
				} else {
					// still clean up blob range change log sometimes even if it's always changing and we wouldn't
					// otherwise commit this transaction
					if (deterministicRandom()->random01() < 0.2) {
						tr->clear(cleanupKeyRange);
						wait(checkManagerLock(tr, bmData));
						wait(tr->commit());
						if (BM_DEBUG) {
							fmt::print("Blob manager done processing client ranges @ {0}, retrying\n",
							           tr->getCommittedVersion());
						}
					}
					watchFuture = Future<Void>(Void()); // restart immediately
				}

				wait(watchFuture);
				break;
			} catch (Error& e) {
				if (BM_DEBUG) {
					fmt::print("Blob manager got error waiting for new range updates {}\n", e.name());
				}
				wait(tr->onError(e));
			}
		}
	}
}

ACTOR Future<Void> reevaluateInitialSplit(Reference<BlobManagerData> bmData,
                                          UID currentWorkerId,
                                          KeyRange granuleRange,
                                          UID granuleID,
                                          int64_t epoch,
                                          int64_t seqno,
                                          Key proposedSplitKey) {
	CODE_PROBE(true, "BM re-evaluating initial split too big");
	if (BM_DEBUG) {
		fmt::print("BM {0} re-evaluating initial split [{1} - {2}) too big from {3} @ ({4}, {5})\n",
		           bmData->epoch,
		           granuleRange.begin.printable(),
		           granuleRange.end.printable(),
		           currentWorkerId.toString().substr(0, 5),
		           epoch,
		           seqno);
		fmt::print("Proposed split (2):\n");
		fmt::print("    {0}\n", granuleRange.begin.printable());
		fmt::print("    {0}\n", proposedSplitKey.printable());
		fmt::print("    {0}\n", granuleRange.end.printable());
	}
	TraceEvent("BMCheckInitialSplitTooBig", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("Granule", granuleRange)
	    .detail("ProposedSplitKey", proposedSplitKey);
	// calculate new split targets speculatively assuming split is too large and current worker still owns it
	ASSERT(granuleRange.begin < proposedSplitKey);
	ASSERT(proposedSplitKey < granuleRange.end);
	state Future<BlobGranuleSplitPoints> fSplitFirst =
	    splitRange(bmData, KeyRangeRef(granuleRange.begin, proposedSplitKey), false, true);
	state Future<BlobGranuleSplitPoints> fSplitSecond =
	    splitRange(bmData, KeyRangeRef(proposedSplitKey, granuleRange.end), false, true);

	state Standalone<VectorRef<KeyRef>> newRanges;

	BlobGranuleSplitPoints splitFirst = wait(fSplitFirst);
	ASSERT(splitFirst.keys.size() >= 2);
	ASSERT(splitFirst.keys.front() == granuleRange.begin);
	ASSERT(splitFirst.keys.back() == proposedSplitKey);
	Standalone<VectorRef<KeyRef>> splitFirstFullKeys = wait(nextFullKeys(bmData, splitFirst.keys));
	for (int i = 0; i < splitFirstFullKeys.size(); i++) {
		newRanges.push_back_deep(newRanges.arena(), splitFirstFullKeys[i]);
	}

	BlobGranuleSplitPoints splitSecond = wait(fSplitSecond);
	ASSERT(splitSecond.keys.size() >= 2);
	ASSERT(splitSecond.keys.front() == proposedSplitKey);
	ASSERT(splitSecond.keys.back() == granuleRange.end);
	Standalone<VectorRef<KeyRef>> splitSecondFullKeys = wait(nextFullKeys(bmData, splitSecond.keys));
	// i=1 to skip proposedSplitKey, since above already added it
	for (int i = 1; i < splitSecondFullKeys.size(); i++) {
		newRanges.push_back_deep(newRanges.arena(), splitSecondFullKeys[i]);
	}

	if (BM_DEBUG) {
		fmt::print("Re-evaluated split ({0}):\n", newRanges.size());
		for (auto& it : newRanges) {
			fmt::print("    {0}\n", it.printable());
		}
	}

	// redo key alignment on full set of split points
	// FIXME: only need to align propsedSplitKey in the middle
	state BlobGranuleSplitPoints finalSplit = wait(alignKeys(bmData, granuleRange, newRanges));

	if (BM_DEBUG) {
		fmt::print("Aligned split ({0}):\n", finalSplit.keys.size());
		for (auto& it : finalSplit.keys) {
			fmt::print("    {0}{1}\n", it.printable(), finalSplit.boundaries.count(it) ? " *" : "");
		}
	}

	ASSERT(finalSplit.keys.size() > 2);

	// Check lock to see if lock is still the specified epoch and seqno, and there are no files for the granule.
	// If either of these are false, some other worker now has the granule. if there are files, it already succeeded at
	// a split. if not, and it fails too, it will retry and get back here
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	state Key lockKey = blobGranuleLockKeyFor(granuleRange);
	state bool retried = false;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			// make sure we're still manager when this transaction gets committed
			wait(checkManagerLock(tr, bmData));

			ForcedPurgeState purgeState = wait(getForcePurgedState(&tr->getTransaction(), granuleRange));
			if (purgeState != ForcedPurgeState::NonePurged) {
				CODE_PROBE(true, "Initial Split Re-evaluate stopped because of force purge", probe::decoration::rare);
				TraceEvent("GranuleSplitReEvalCancelledForcePurge", bmData->id)
				    .detail("Epoch", bmData->epoch)
				    .detail("GranuleRange", granuleRange);

				// destroy already created change feed from worker so it doesn't leak
				wait(updateChangeFeed(&tr->getTransaction(),
				                      granuleIDToCFKey(granuleID),
				                      ChangeFeedStatus::CHANGE_FEED_DESTROY,
				                      granuleRange));

				wait(tr->commit());

				return Void();
			}

			// this adds a read conflict range, so if another granule concurrently commits a file, we will retry and see
			// that
			KeyRange range = blobGranuleFileKeyRangeFor(granuleID);
			RangeReadResult granuleFiles = wait(tr->getRange(range, 1));
			if (!granuleFiles.empty()) {
				CODE_PROBE(true, "split too big was eventually solved by another worker", probe::decoration::rare);
				if (BM_DEBUG) {
					fmt::print("BM {0} re-evaluating initial split [{1} - {2}) too big: solved by another worker\n",
					           bmData->epoch,
					           granuleRange.begin.printable(),
					           granuleRange.end.printable());
				}
				return Void();
			}

			ValueReadResult prevLockValue = wait(tr->get(lockKey));
			ASSERT(prevLockValue.present());
			std::tuple<int64_t, int64_t, UID> prevOwner = decodeBlobGranuleLockValue(prevLockValue.get());
			int64_t prevOwnerEpoch = std::get<0>(prevOwner);
			int64_t prevOwnerSeqno = std::get<1>(prevOwner);
			UID prevGranuleID = std::get<2>(prevOwner);
			if (prevOwnerEpoch != epoch || prevOwnerSeqno != seqno || prevGranuleID != granuleID) {
				if (retried && prevOwnerEpoch == bmData->epoch && prevGranuleID == granuleID &&
				    prevOwnerSeqno == std::numeric_limits<int64_t>::max()) {
					// owner didn't change, last iteration of this transaction just succeeded but threw an error.
					CODE_PROBE(true, "split too big adjustment succeeded after retry");
					break;
				}
				CODE_PROBE(true, "split too big was since moved to another worker", probe::decoration::rare);
				if (BM_DEBUG) {
					fmt::print("BM {0} re-evaluating initial split [{1} - {2}) too big: moved to another worker\n",
					           bmData->epoch,
					           granuleRange.begin.printable(),
					           granuleRange.end.printable());
					fmt::print("Epoch: Prev {0}, Cur {1}\n", prevOwnerEpoch, epoch);
					fmt::print("Seqno: Prev {0}, Cur {1}\n", prevOwnerSeqno, seqno);
					fmt::print("GranuleID: Prev {0}, Cur {1}\n",
					           prevGranuleID.toString().substr(0, 6),
					           granuleID.toString().substr(0, 6));
				}
				return Void();
			}

			if (prevOwnerEpoch > bmData->epoch) {
				if (BM_DEBUG) {
					fmt::print("BM {0} found a higher epoch {1} for granule lock of [{2} - {3})\n",
					           bmData->epoch,
					           prevOwnerEpoch,
					           granuleRange.begin.printable(),
					           granuleRange.end.printable());
				}

				if (bmData->iAmReplaced.canBeSet()) {
					bmData->iAmReplaced.send(Void());
				}
				return Void();
			}

			// The lock check above *should* handle this, but just be sure, also make sure that this granule wasn't
			// already split in the granule mapping
			RangeResult existingRanges = wait(
			    krmGetRanges(tr, blobGranuleMappingKeys.begin, granuleRange, 3, GetRangeLimits::BYTE_LIMIT_UNLIMITED));
			if (existingRanges.size() > 2 || existingRanges.more) {
				CODE_PROBE(true, "split too big was already re-split", probe::decoration::rare);
				if (BM_DEBUG) {
					fmt::print("BM {0} re-evaluating initial split [{1} - {2}) too big: already split\n",
					           bmData->epoch,
					           granuleRange.begin.printable(),
					           granuleRange.end.printable());
					for (auto& it : existingRanges) {
						fmt::print("  {0}\n", it.key.printable());
					}
				}
				return Void();
			}

			// Set lock to max value for this manager, so other reassignments can't race with this transaction
			// and existing owner can't modify it further.
			tr->set(lockKey, blobGranuleLockValueFor(bmData->epoch, std::numeric_limits<int64_t>::max(), granuleID));

			// set new ranges
			state int i;
			for (i = 0; i < finalSplit.keys.size() - 1; i++) {
				wait(krmSetRange(tr,
				                 blobGranuleMappingKeys.begin,
				                 KeyRangeRef(finalSplit.keys[i], finalSplit.keys[i + 1]),
				                 blobGranuleMappingValueFor(UID())));
				if (finalSplit.boundaries.count(finalSplit.keys[i])) {
					tr->set(blobGranuleMergeBoundaryKeyFor(finalSplit.keys[i]),
					        blobGranuleMergeBoundaryValueFor(finalSplit.boundaries[finalSplit.keys[i]]));
				}
			}

			// Need to destroy the old change feed for the no longer needed feed, otherwise it will leak
			// This has to be a non-ryw transaction for the change feed destroy mutations to propagate properly
			// TODO: fix this better! (privatize change feed key clear)
			wait(updateChangeFeed(&tr->getTransaction(),
			                      granuleIDToCFKey(granuleID),
			                      ChangeFeedStatus::CHANGE_FEED_DESTROY,
			                      granuleRange));

			retried = true;
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
		wait(delay(0)); // should be cancelled
		ASSERT(false);
	}

	// transaction committed, send updated range assignments. Even if there is only one range still, we need to revoke
	// it and re-assign it to cancel the old granule and retry
	CODE_PROBE(true, "BM successfully changed initial split too big");
	RangeAssignment raRevoke;
	raRevoke.isAssign = false;
	raRevoke.keyRange = granuleRange;
	raRevoke.revoke = RangeRevokeData(false); // not a dispose
	handleRangeAssign(bmData, raRevoke);

	for (int i = 0; i < finalSplit.keys.size() - 1; i++) {
		// reassign new range and do handover of previous range
		RangeAssignment raAssignSplit;
		raAssignSplit.isAssign = true;
		raAssignSplit.keyRange = KeyRangeRef(finalSplit.keys[i], finalSplit.keys[i + 1]);
		raAssignSplit.assign = RangeAssignmentData();
		// don't care who this range gets assigned to
		handleRangeAssign(bmData, raAssignSplit);
	}

	if (BM_DEBUG) {
		fmt::print("BM {0} Re-splitting initial range [{1} - {2}) into {3} granules done\n",
		           bmData->epoch,
		           granuleRange.begin.printable(),
		           granuleRange.end.printable(),
		           finalSplit.keys.size() - 1);
	}

	TraceEvent("BMCheckInitialSplitTooBigDone", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("OldGranule", granuleRange)
	    .detail("NewGranules", finalSplit.keys.size() - 1);

	return Void();
}

ACTOR Future<Void> maybeSplitRange(Reference<BlobManagerData> bmData,
                                   UID currentWorkerId,
                                   KeyRange granuleRange,
                                   UID granuleID,
                                   Version granuleStartVersion,
                                   bool writeHot,
                                   int64_t originalEpoch,
                                   int64_t originalSeqno) {
	DebugBMTraceEvent(SevDebug, "BMMaybeSplitRange", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("Granule", granuleRange)
	    .detail("GranuleID", granuleID)
	    .detail("OriginalEpoch", originalEpoch)
	    .detail("OriginalSeqno", originalSeqno);

	if (bmData->isForcePurging(granuleRange)) {
		// ignore
		return Void();
	}
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);

	state ActiveCounter<int>::Releaser holdingSplitCheckCounter = bmData->stats.activeGranuleSplitChecks.take(1);

	// first get ranges to split
	// +1 because this is boundaries, so N granules would have N+1 boundaries
	state BlobGranuleSplitPoints splitPoints =
	    wait(splitRange(bmData, granuleRange, writeHot, false, SERVER_KNOBS->BG_MAX_SPLIT_FANOUT + 1));

	ASSERT(splitPoints.keys.size() >= 2);
	if (splitPoints.keys.size() == 2) {
		// not large enough to split, just reassign back to worker
		if (BM_DEBUG) {
			fmt::print("Not splitting existing range [{0} - {1}). Continuing assignment to {2}\n",
			           granuleRange.begin.printable(),
			           granuleRange.end.printable(),
			           currentWorkerId.toString());
		}

		DebugBMTraceEvent(SevDebug, "BMMaybeSplitRangeNoSplit", bmData->id)
		    .detail("Epoch", bmData->epoch)
		    .detail("GranuleID", granuleID)
		    .detail("Granule", granuleRange)
		    .detail("BWID", currentWorkerId);

		// -1 because we are going to send the continue at seqNo, so the guard we are setting here needs to be greater
		// than whatever came before, but less than the continue seqno
		int64_t seqnoForEval = bmData->seqNo - 1;

		RangeAssignment raContinue;
		raContinue.isAssign = true;
		raContinue.worker = currentWorkerId;
		raContinue.keyRange = granuleRange;
		raContinue.assign = RangeAssignmentData(AssignRequestType::Continue); // continue assignment and re-snapshot
		bool reassignSuccess;
		handleRangeAssign(bmData, raContinue, &reassignSuccess);

		// set updated boundary evaluation to avoid racing calls getting unblocked after here
		// We need to only revoke based on non-continue seqno, but ignore duplicate calls based on continue seqno
		if (reassignSuccess) {
			bmData->boundaryEvaluations.insert(
			    granuleRange,
			    BoundaryEvaluation(bmData->epoch, seqnoForEval, BoundaryEvalType::SPLIT, originalEpoch, originalSeqno));
		}

		return Void();
	}

	ASSERT(granuleRange.begin == splitPoints.keys.front());
	ASSERT(granuleRange.end == splitPoints.keys.back());

	// Have to make set of granule ids deterministic across retries to not end up with extra UIDs in the split
	// state, which could cause recovery to fail and resources to not be cleaned up.
	// This entire transaction must be idempotent across retries for all splitting state
	state std::vector<UID> newGranuleIDs;
	newGranuleIDs.reserve(splitPoints.keys.size() - 1);
	for (int i = 0; i < splitPoints.keys.size() - 1; i++) {
		newGranuleIDs.push_back(deterministicRandom()->randomUniqueID());
	}

	if (BM_DEBUG) {
		fmt::print("Splitting range [{0} - {1}) into {2} granules:\n",
		           granuleRange.begin.printable(),
		           granuleRange.end.printable(),
		           splitPoints.keys.size() - 1);
		for (int i = 0; i < splitPoints.keys.size(); i++) {
			fmt::print("    {0}:{1}{2}\n",
			           (i < newGranuleIDs.size() ? newGranuleIDs[i] : UID()).toString().substr(0, 6).c_str(),
			           splitPoints.keys[i].printable(),
			           splitPoints.boundaries.count(splitPoints.keys[i]) ? " *" : "");
		}
	}

	state Version splitVersion;

	// Need to split range. Persist intent to split and split metadata to DB BEFORE sending split assignments to blob
	// workers, so that nothing is lost on blob manager recovery
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			ASSERT(splitPoints.keys.size() > 2);

			state Key lockKey = blobGranuleLockKeyFor(granuleRange);

			// make sure we're still manager when this transaction gets committed
			state Future<Void> managerLockCheckF = checkManagerLock(tr, bmData);
			state Future<ForcedPurgeState> forcePurgeStateF = getForcePurgedState(&tr->getTransaction(), granuleRange);
			state Future<RangeReadResult> existingStateF =
			    tr->getRange(blobGranuleSplitKeyRangeFor(granuleID), SERVER_KNOBS->BG_MAX_SPLIT_FANOUT + 2);
			state Future<ValueReadResult> granuleLockCheckF = tr->get(lockKey);

			wait(managerLockCheckF && success(forcePurgeStateF) && success(existingStateF) &&
			     success(granuleLockCheckF));

			ForcedPurgeState purgeState = wait(forcePurgeStateF);
			if (purgeState != ForcedPurgeState::NonePurged) {
				CODE_PROBE(true, "Split stopped because of force purge", probe::decoration::rare);
				TraceEvent("GranuleSplitCancelledForcePurge", bmData->id)
				    .detail("Epoch", bmData->epoch)
				    .detail("GranuleRange", granuleRange);
				return Void();
			}

			// TODO can do this + lock in parallel
			// Read splitState to see if anything was committed instead of reading granule mapping because we don't want
			// to conflict with mapping changes/reassignments
			state RangeReadResult existingState = wait(existingStateF);
			ASSERT_WE_THINK(!existingState.more && existingState.size() <= SERVER_KNOBS->BG_MAX_SPLIT_FANOUT + 1);
			// maybe someone decreased the knob, we should gracefully handle it not in simulation
			if (existingState.more || existingState.size() > SERVER_KNOBS->BG_MAX_SPLIT_FANOUT) {
				RangeReadResult tryAgain = wait(tr->getRange(blobGranuleSplitKeyRangeFor(granuleID), 10000));
				ASSERT(!tryAgain.more);
				existingState = tryAgain;
			}
			if (!existingState.empty()) {
				// Something was previously committed, we must go with that decision.
				// Read its boundaries and override our planned split boundaries
				CODE_PROBE(true, "Overriding split ranges with existing ones from DB");
				RangeReadResult existingBoundaries =
				    wait(tr->getRange(KeyRangeRef(granuleRange.begin.withPrefix(blobGranuleMappingKeys.begin),
				                                  keyAfter(granuleRange.end).withPrefix(blobGranuleMappingKeys.begin)),
				                      existingState.size() + 2));
				// +2 because this is boundaries and existingState was granules, and to ensure it doesn't set more
				ASSERT(!existingBoundaries.more);
				ASSERT(existingBoundaries.size() == existingState.size() + 1);
				splitPoints.keys.clear();
				splitPoints.keys.arena().dependsOn(existingBoundaries.arena());
				for (auto& it : existingBoundaries) {
					splitPoints.keys.push_back(splitPoints.keys.arena(),
					                           it.key.removePrefix(blobGranuleMappingKeys.begin));
				}
				// We don't care about splitPoints.boundaries as they are already persisted.
				splitPoints.boundaries.clear();
				ASSERT(splitPoints.keys.front() == granuleRange.begin);
				ASSERT(splitPoints.keys.back() == granuleRange.end);
				if (BM_DEBUG) {
					fmt::print("Replaced old range splits for [{0} - {1}) with {2}:\n",
					           granuleRange.begin.printable(),
					           granuleRange.end.printable(),
					           splitPoints.keys.size() - 1);
					for (int i = 0; i < splitPoints.keys.size(); i++) {
						fmt::print("    {}\n", splitPoints.keys[i].printable());
					}
				}
				break;
			}

			// acquire lock for old granule to make sure nobody else modifies it

			ValueReadResult lockValue = wait(granuleLockCheckF);
			ASSERT(lockValue.present());
			std::tuple<int64_t, int64_t, UID> prevGranuleLock = decodeBlobGranuleLockValue(lockValue.get());
			int64_t ownerEpoch = std::get<0>(prevGranuleLock);

			if (ownerEpoch > bmData->epoch) {
				if (BM_DEBUG) {
					fmt::print("BM {0} found a higher epoch {1} than {2} for granule lock of [{3} - {4})\n",
					           bmData->epoch,
					           ownerEpoch,
					           bmData->epoch,
					           granuleRange.begin.printable(),
					           granuleRange.end.printable());
				}

				if (bmData->iAmReplaced.canBeSet()) {
					bmData->iAmReplaced.send(Void());
				}
				return Void();
			}

			// Set lock to max value for this manager, so other reassignments can't race with this transaction
			// and existing owner can't modify it further.
			// Merging makes lock go backwards if we later merge other granules back to this same range, but it is fine
			tr->set(lockKey,
			        blobGranuleLockValueFor(
			            bmData->epoch, std::numeric_limits<int64_t>::max(), std::get<2>(prevGranuleLock)));

			// get last delta file version written, to make that the split version
			RangeReadResult lastDeltaFile =
			    wait(tr->getRange(blobGranuleFileKeyRangeFor(granuleID), 1, Snapshot::False, Reverse::True));
			ASSERT(lastDeltaFile.size() == 1);
			std::tuple<UID, Version, uint8_t> k = decodeBlobGranuleFileKey(lastDeltaFile[0].key);
			ASSERT(std::get<0>(k) == granuleID);
			ASSERT(std::get<2>(k) == 'D');
			splitVersion = std::get<1>(k);

			if (BM_DEBUG) {
				fmt::print("BM {0} found version {1} for splitting [{2} - {3})\n",
				           bmData->epoch,
				           splitVersion,
				           granuleRange.begin.printable(),
				           granuleRange.end.printable());
			}

			// set up splits in granule mapping, but point each part to the old owner (until they get reassigned)
			state int i;
			wait(krmSetRange(tr,
			                 blobGranuleMappingKeys.begin,
			                 KeyRangeRef(splitPoints.keys.front(), splitPoints.keys.back()),
			                 blobGranuleMappingValueFor(currentWorkerId)));
			for (i = 0; i < splitPoints.keys.size() - 1; i++) {
				KeyRangeRef splitRange(splitPoints.keys[i], splitPoints.keys[i + 1]);

				// Record split state.
				Key splitKey = blobGranuleSplitKeyFor(granuleID, newGranuleIDs[i]);

				tr->atomicOp(splitKey,
				             blobGranuleSplitValueFor(BlobGranuleSplitState::Initialized),
				             MutationRef::SetVersionstampedValue);

				// Update BlobGranuleMergeBoundary.
				if (splitPoints.boundaries.count(splitRange.begin)) {
					tr->set(blobGranuleMergeBoundaryKeyFor(splitRange.begin),
					        blobGranuleMergeBoundaryValueFor(splitPoints.boundaries[splitRange.begin]));
				}

				// History.
				Key historyKey = blobGranuleHistoryKeyFor(splitRange, splitVersion);

				Standalone<BlobGranuleHistoryValue> historyValue;
				historyValue.granuleID = newGranuleIDs[i];
				historyValue.parentBoundaries.push_back(historyValue.arena(), granuleRange.begin);
				historyValue.parentBoundaries.push_back(historyValue.arena(), granuleRange.end);
				historyValue.parentVersions.push_back(historyValue.arena(), granuleStartVersion);

				tr->set(historyKey, blobGranuleHistoryValueFor(historyValue));

				// split the assignment but still pointing to the same worker
				// FIXME: could pick new random workers here, they'll get overridden shortly unless the BM immediately
				// restarts
				if (i != 0) {
					tr->set(splitPoints.keys[i].withPrefix(blobGranuleMappingKeys.begin),
					        blobGranuleMappingValueFor(currentWorkerId));
				}
			}

			wait(tr->commit());

			if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
				wait(delay(0)); // should be cancelled
				ASSERT(false);
			}

			// Update BlobGranuleMergeBoundary in-memory state.
			for (auto it = splitPoints.boundaries.begin(); it != splitPoints.boundaries.end(); it++) {
				bmData->mergeBoundaries[it->first] = it->second;
			}

			break;
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw;
			}
			if (BM_DEBUG) {
				fmt::print("BM {0} Persisting granule split got error {1}\n", bmData->epoch, e.name());
			}
			if (e.code() == error_code_granule_assignment_conflict) {
				if (bmData->iAmReplaced.canBeSet()) {
					bmData->iAmReplaced.send(Void());
				}
				return Void();
			}
			wait(tr->onError(e));
		}
	}

	if (BM_DEBUG) {
		fmt::print("Splitting range [{0} - {1}) into {2} granules @ {3} done, sending assignments:\n",
		           granuleRange.begin.printable(),
		           granuleRange.end.printable(),
		           splitPoints.keys.size() - 1,
		           splitVersion);
	}

	++bmData->stats.granuleSplits;
	if (writeHot) {
		++bmData->stats.granuleWriteHotSplits;
	}

	int64_t seqnoForEval = bmData->seqNo;

	// transaction committed, send range assignments
	// range could have been moved since split eval started, so just revoke from whoever has it
	RangeAssignment raRevoke;
	raRevoke.isAssign = false;
	raRevoke.keyRange = granuleRange;
	raRevoke.revoke = RangeRevokeData(false); // not a dispose
	handleRangeAssign(bmData, raRevoke);

	for (int i = 0; i < splitPoints.keys.size() - 1; i++) {
		// reassign new range and do handover of previous range
		RangeAssignment raAssignSplit;
		raAssignSplit.isAssign = true;
		raAssignSplit.keyRange = KeyRangeRef(splitPoints.keys[i], splitPoints.keys[i + 1]);
		raAssignSplit.assign = RangeAssignmentData();
		// don't care who this range gets assigned to
		handleRangeAssign(bmData, raAssignSplit);
	}

	if (BM_DEBUG) {
		fmt::print("Splitting range [{0} - {1}) into {2} granules @ {3} got assignments processed\n",
		           granuleRange.begin.printable(),
		           granuleRange.end.printable(),
		           splitPoints.keys.size() - 1,
		           splitVersion);
	}

	DebugBMTraceEvent(SevDebug, "BMMaybeSplitRangeComplete", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("Granule", granuleRange)
	    .detail("GranuleID", granuleID)
	    .detail("OriginalEpoch", originalEpoch)
	    .detail("OriginalSeqno", originalSeqno)
	    .detail("NewGranules", splitPoints.keys.size() - 1);

	// set updated boundary evaluation to avoid racing calls getting unblocked after here
	bmData->boundaryEvaluations.insert(
	    granuleRange,
	    BoundaryEvaluation(bmData->epoch, seqnoForEval, BoundaryEvalType::SPLIT, originalEpoch, originalSeqno));

	return Void();
}

// read mapping from db to handle any in flight granules or other issues
// Forces all granules in the specified key range to flush data to blob up to the specified version. This is required
// for executing a merge.
ACTOR Future<bool> forceGranuleFlush(Reference<BlobManagerData> bmData,
                                     UID mergeGranuleID,
                                     KeyRange keyRange,
                                     Version version) {
	state Transaction tr(bmData->db);
	state Key beginKey = keyRange.begin;
	state Key endKey = keyRange.end;
	int64_t epoch = bmData->epoch;
	Version v = version;
	state FlushGranuleRequest req(epoch, keyRange, v, false);

	if (BM_DEBUG) {
		fmt::print("Flushing Granules [{0} - {1}) @ {2}\n", beginKey.printable(), endKey.printable(), version);
	}

	DebugBMTraceEvent(SevDebug, "BMForceGranuleFlush", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("Granule", keyRange)
	    .detail("Version", version);

	loop {
		if (beginKey >= endKey) {
			break;
		}
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			ForcedPurgeState purgeState = wait(getForcePurgedState(&tr, keyRange));
			if (purgeState != ForcedPurgeState::NonePurged) {
				CODE_PROBE(true, "Granule flush stopped because of force purge", probe::decoration::rare);
				TraceEvent("GranuleFlushCancelledForcePurge", bmData->id)
				    .detail("Epoch", bmData->epoch)
				    .detail("KeyRange", keyRange);

				// destroy already created change feed from earlier so it doesn't leak
				wait(updateChangeFeed(
				    &tr, granuleIDToCFKey(mergeGranuleID), ChangeFeedStatus::CHANGE_FEED_DESTROY, keyRange));

				wait(tr.commit());
				return false;
			}

			wait(success(
			    txnDoBlobGranuleRequests(&tr, &beginKey, endKey, req, &BlobWorkerInterface::flushGranuleRequest)));

		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	if (BM_DEBUG) {
		fmt::print("Flushing Granules [{0} - {1}) @ {2} Complete!\n",
		           keyRange.begin.printable(),
		           keyRange.end.printable(),
		           version);
	}

	DebugBMTraceEvent(SevDebug, "BMForceGranuleFlushComplete", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("Granule", keyRange)
	    .detail("Version", version);

	return true;
}

// Persist the merge intent for this merge in the database. Once this transaction commits, the merge is in progress. It
// cannot be aborted, and must be completed.
ACTOR Future<std::pair<UID, Version>> persistMergeGranulesStart(Reference<BlobManagerData> bmData,
                                                                KeyRange mergeRange,
                                                                std::vector<UID> parentGranuleIDs,
                                                                std::vector<Key> parentGranuleRanges,
                                                                std::vector<Version> parentGranuleStartVersions) {
	state UID mergeGranuleID = deterministicRandom()->randomUniqueID();
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			wait(checkManagerLock(tr, bmData));

			ForcedPurgeState purgeState = wait(getForcePurgedState(&tr->getTransaction(), mergeRange));
			if (purgeState != ForcedPurgeState::NonePurged) {
				CODE_PROBE(true, "Merge start stopped because of force purge", probe::decoration::rare);
				TraceEvent("GranuleMergeStartCancelledForcePurge", bmData->id)
				    .detail("Epoch", bmData->epoch)
				    .detail("GranuleRange", mergeRange);

				// destroy already created change feed from earlier so it doesn't leak
				wait(updateChangeFeed(&tr->getTransaction(),
				                      granuleIDToCFKey(mergeGranuleID),
				                      ChangeFeedStatus::CHANGE_FEED_DESTROY,
				                      mergeRange));

				wait(tr->commit());

				bmData->activeGranuleMerges.insert(mergeRange, invalidVersion);
				bmData->activeGranuleMerges.coalesce(mergeRange.begin);

				// TODO better error?
				return std::pair(UID(), invalidVersion);
			}
			// FIXME: extra safeguard: check that granuleID of active lock == parentGranuleID for each parent, abort
			// merge if so

			tr->atomicOp(
			    blobGranuleMergeKeyFor(mergeGranuleID),
			    blobGranuleMergeValueFor(mergeRange, parentGranuleIDs, parentGranuleRanges, parentGranuleStartVersions),
			    MutationRef::SetVersionstampedValue);

			wait(updateChangeFeed(
			    tr, granuleIDToCFKey(mergeGranuleID), ChangeFeedStatus::CHANGE_FEED_CREATE, mergeRange));

			wait(tr->commit());

			if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
				wait(delay(0)); // should be cancelled
				ASSERT(false);
			}

			Version mergeVersion = tr->getCommittedVersion();
			if (BM_DEBUG) {
				fmt::print("Granule merge intent persisted [{0} - {1}): {2} @ {3}!\n",
				           mergeRange.begin.printable(),
				           mergeRange.end.printable(),
				           mergeGranuleID.shortString().substr(0, 6),
				           mergeVersion);
			}

			// update merge version in boundary evals so racing splits can continue if necessary
			auto mergeInProgress = bmData->activeGranuleMerges.rangeContaining(mergeRange.begin);
			if (BM_DEBUG) {
				fmt::print("Updating merge in progress [{0} - {1}) to merge version {2}!\n",
				           mergeInProgress.begin().printable(),
				           mergeInProgress.end().printable(),
				           mergeVersion);
			}
			ASSERT(mergeInProgress.begin() == mergeRange.begin);
			ASSERT(mergeInProgress.end() == mergeRange.end);
			ASSERT(mergeInProgress.cvalue() == 0);
			mergeInProgress.value() = mergeVersion;

			return std::pair(mergeGranuleID, mergeVersion);
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// Persists the merge being complete in the database by clearing the merge intent. Once this transaction commits, the
// merge is considered completed.
ACTOR Future<bool> persistMergeGranulesDone(Reference<BlobManagerData> bmData,
                                            UID mergeGranuleID,
                                            KeyRange mergeRange,
                                            Version mergeVersion,
                                            std::vector<UID> parentGranuleIDs,
                                            std::vector<Key> parentGranuleRanges,
                                            std::vector<Version> parentGranuleStartVersions) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	// pick worker that has part of old range, it will soon get overridden anyway
	state UID tmpWorkerId;
	auto ranges = bmData->workerAssignments.intersectingRanges(mergeRange);
	for (auto& it : ranges) {
		if (it.cvalue() != UID()) {
			tmpWorkerId = it.cvalue();
			break;
		}
	}
	if (tmpWorkerId == UID()) {
		CODE_PROBE(true, "All workers dead right now", probe::decoration::rare);
		while (bmData->workersById.empty()) {
			wait(bmData->recruitingStream.onChange() || bmData->foundBlobWorkers.getFuture());
		}
		tmpWorkerId = bmData->workersById.begin()->first;
	}
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			wait(checkManagerLock(tr, bmData));

			ForcedPurgeState purgeState = wait(getForcePurgedState(&tr->getTransaction(), mergeRange));
			if (purgeState != ForcedPurgeState::NonePurged) {
				CODE_PROBE(true, "Merge finish stopped because of force purge", probe::decoration::rare);
				TraceEvent("GranuleMergeCancelledForcePurge", bmData->id)
				    .detail("Epoch", bmData->epoch)
				    .detail("GranuleRange", mergeRange);

				// destroy already created change feed from earlier so it doesn't leak
				wait(updateChangeFeed(&tr->getTransaction(),
				                      granuleIDToCFKey(mergeGranuleID),
				                      ChangeFeedStatus::CHANGE_FEED_DESTROY,
				                      mergeRange));

				// TODO could also delete history entry here

				wait(tr->commit());

				bmData->activeGranuleMerges.insert(mergeRange, invalidVersion);
				bmData->activeGranuleMerges.coalesce(mergeRange.begin);

				return false;
			}

			tr->clear(blobGranuleMergeKeyFor(mergeGranuleID));

			state int parentIdx;
			// TODO: could parallelize these
			for (parentIdx = 0; parentIdx < parentGranuleIDs.size(); parentIdx++) {
				KeyRange parentRange(KeyRangeRef(parentGranuleRanges[parentIdx], parentGranuleRanges[parentIdx + 1]));
				state Key lockKey = blobGranuleLockKeyFor(parentRange);
				state Future<ValueReadResult> oldLockFuture = tr->get(lockKey);

				// Clear existing merge boundaries.
				tr->clear(blobGranuleMergeBoundaryKeyFor(parentRange.begin));

				// This has to be a non-ryw transaction for the change feed destroy mutations to propagate properly
				// TODO: fix this better! (privatize change feed key clear)
				wait(updateChangeFeed(&tr->getTransaction(),
				                      granuleIDToCFKey(parentGranuleIDs[parentIdx]),
				                      ChangeFeedStatus::CHANGE_FEED_DESTROY,
				                      parentRange));
				if (BM_DEBUG) {
					fmt::print("Granule merge destroying CF {0} ({1})!\n",
					           parentGranuleIDs[parentIdx].shortString().substr(0, 6),
					           granuleIDToCFKey(parentGranuleIDs[parentIdx]).printable());
				}

				ValueReadResult oldLock = wait(oldLockFuture);
				ASSERT(oldLock.present());
				auto prevLock = decodeBlobGranuleLockValue(oldLock.get());
				// Set lock to max value for this manager, so other reassignments can't race with this transaction
				// and existing owner can't modify it further.
				// Merging makes lock go backwards if we later split another granule back to this same range, but it is
				// fine and handled in the blob worker
				tr->set(
				    lockKey,
				    blobGranuleLockValueFor(bmData->epoch, std::numeric_limits<int64_t>::max(), std::get<2>(prevLock)));
			}

			tr->clear(KeyRangeRef(keyAfter(blobGranuleMergeBoundaryKeyFor(mergeRange.begin)),
			                      blobGranuleMergeBoundaryKeyFor(mergeRange.end)));

			// either set initial lock value, or re-set it if it was set to (epoch, <max>) in a previous split
			int64_t seqNo = bmData->seqNo++;
			tr->set(blobGranuleLockKeyFor(mergeRange), blobGranuleLockValueFor(bmData->epoch, seqNo, mergeGranuleID));

			// persist history entry
			Key historyKey = blobGranuleHistoryKeyFor(mergeRange, mergeVersion);

			Standalone<BlobGranuleHistoryValue> historyValue;
			historyValue.granuleID = mergeGranuleID;
			for (parentIdx = 0; parentIdx < parentGranuleIDs.size(); parentIdx++) {
				historyValue.parentBoundaries.push_back(historyValue.arena(), parentGranuleRanges[parentIdx]);
				historyValue.parentVersions.push_back(historyValue.arena(), parentGranuleStartVersions[parentIdx]);
			}
			historyValue.parentBoundaries.push_back(historyValue.arena(), parentGranuleRanges.back());

			tr->set(historyKey, blobGranuleHistoryValueFor(historyValue));

			wait(krmSetRange(tr, blobGranuleMappingKeys.begin, mergeRange, blobGranuleMappingValueFor(tmpWorkerId)));
			wait(tr->commit());

			// Update in-memory mergeBoundaries map.
			for (parentIdx = 1; parentIdx < parentGranuleIDs.size(); parentIdx++) {
				bmData->mergeBoundaries.erase(parentGranuleRanges[parentIdx]);
			}

			if (BM_DEBUG) {
				fmt::print("Granule merge intent cleared [{0} - {1}): {2} @ {3} (cv={4})\n",
				           mergeRange.begin.printable(),
				           mergeRange.end.printable(),
				           mergeGranuleID.shortString().substr(0, 6),
				           mergeVersion,
				           tr->getCommittedVersion());
			}
			CODE_PROBE(true, "Granule merge complete");

			if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
				wait(delay(0)); // should be cancelled
				ASSERT(false);
			}

			return true;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// This is the idempotent function that executes a granule merge once the initial merge intent has been persisted.
ACTOR Future<Void> finishMergeGranules(Reference<BlobManagerData> bmData,
                                       UID mergeGranuleID,
                                       KeyRange mergeRange,
                                       Version mergeVersion,
                                       std::vector<UID> parentGranuleIDs,
                                       std::vector<Key> parentGranuleRanges,
                                       std::vector<Version> parentGranuleStartVersions) {
	++bmData->stats.activeMerges;

	// wait for BM to be fully recovered and have loaded hard boundaries before starting actual merges
	wait(bmData->doneRecovering.getFuture());
	wait(bmData->loadedClientRanges.getFuture());
	wait(delay(0));

	// Assert that none of the subsequent granules are hard boundaries.
	if (g_network->isSimulated()) {
		for (int i = 1; i < parentGranuleRanges.size() - 1; i++) {
			ASSERT(!bmData->mergeHardBoundaries.count(parentGranuleRanges[i]));
		}
	}

	// force granules to persist state up to mergeVersion
	bool successFlush = wait(forceGranuleFlush(bmData, mergeGranuleID, mergeRange, mergeVersion));
	if (!successFlush) {
		bmData->activeGranuleMerges.insert(mergeRange, invalidVersion);
		bmData->activeGranuleMerges.coalesce(mergeRange.begin);
		--bmData->stats.activeMerges;
		return Void();
	}

	// update state and clear merge intent
	bool successFinish = wait(persistMergeGranulesDone(bmData,
	                                                   mergeGranuleID,
	                                                   mergeRange,
	                                                   mergeVersion,
	                                                   parentGranuleIDs,
	                                                   parentGranuleRanges,
	                                                   parentGranuleStartVersions));
	if (!successFinish) {
		bmData->activeGranuleMerges.insert(mergeRange, invalidVersion);
		bmData->activeGranuleMerges.coalesce(mergeRange.begin);
		--bmData->stats.activeMerges;
		return Void();
	}

	int64_t seqnoForEval = bmData->seqNo;

	// revoke old ranges and assign new range
	RangeAssignment revokeOld;
	revokeOld.isAssign = false;
	revokeOld.keyRange = mergeRange;
	revokeOld.revoke = RangeRevokeData(false);
	handleRangeAssign(bmData, revokeOld);

	RangeAssignment assignNew;
	assignNew.isAssign = true;
	assignNew.keyRange = mergeRange;
	assignNew.assign = RangeAssignmentData(); // not a continue
	handleRangeAssign(bmData, assignNew);

	bmData->activeGranuleMerges.insert(mergeRange, invalidVersion);
	bmData->activeGranuleMerges.coalesce(mergeRange.begin);

	bmData->boundaryEvaluations.insert(mergeRange,
	                                   BoundaryEvaluation(bmData->epoch, seqnoForEval, BoundaryEvalType::MERGE, 0, 0));
	bmData->setMergeCandidate(mergeRange, MergeCandidateMerging);

	--bmData->stats.activeMerges;

	return Void();
}

ACTOR Future<Void> doMerge(Reference<BlobManagerData> bmData,
                           KeyRange mergeRange,
                           std::vector<std::tuple<UID, KeyRange, Version>> toMerge) {
	// switch to format persist merge wants
	state std::vector<UID> ids;
	state std::vector<Key> ranges;
	state std::vector<Version> startVersions;
	for (auto& it : toMerge) {
		ids.push_back(std::get<0>(it));
		ranges.push_back(std::get<1>(it).begin);
		startVersions.push_back(std::get<2>(it));
	}
	ranges.push_back(std::get<1>(toMerge.back()).end);

	++bmData->stats.granuleMerges;

	try {
		std::pair<UID, Version> persistMerge =
		    wait(persistMergeGranulesStart(bmData, mergeRange, ids, ranges, startVersions));
		if (persistMerge.second == invalidVersion) {
			// cancelled because of force purge

			return Void();
		}
		wait(finishMergeGranules(
		    bmData, persistMerge.first, mergeRange, persistMerge.second, ids, ranges, startVersions));
		return Void();
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled || e.code() == error_code_blob_manager_replaced) {
			throw;
		}
		TraceEvent(SevError, "UnexpectedErrorGranuleMerge").error(e).detail("Range", mergeRange);
		throw e;
	}
}

// Needs to not be an actor to run synchronously for the race checking.
// Technically this could just be the first part of doMerge, but this guarantees no waits happen for the checks before
// the logic starts
static void attemptStartMerge(Reference<BlobManagerData> bmData,
                              const std::vector<std::tuple<UID, KeyRange, Version>>& toMerge) {
	if (toMerge.size() < 2) {
		return;
	}
	// TODO REMOVE validation eventually
	for (int i = 0; i < toMerge.size() - 1; i++) {
		ASSERT(std::get<1>(toMerge[i]).end == std::get<1>(toMerge[i + 1]).begin);
	}
	KeyRange mergeRange(KeyRangeRef(std::get<1>(toMerge.front()).begin, std::get<1>(toMerge.back()).end));
	// merge/merge races should not be possible because granuleMergeChecker should only start attemptMerges() for
	// disjoint ranges, and merge candidate is not updated if it is already in the state MergeCandidateMerging
	ASSERT(!bmData->isMergeActive(mergeRange));
	// Check to avoid races where a split eval came in while merge was evaluating. This also effectively checks
	// boundaryEvals because they're both updated before maybeSplitRange is called. This handles split/merge races.
	auto reCheckMergeCandidates = bmData->mergeCandidates.intersectingRanges(mergeRange);
	for (auto it : reCheckMergeCandidates) {
		if (!it->cvalue().mergeEligible()) {
			CODE_PROBE(true, "granule no longer merge candidate after checking metrics, aborting merge");
			return;
		}
	}

	if (bmData->isForcePurging(mergeRange)) {
		// ignore
		return;
	}

	if (BM_DEBUG) {
		fmt::print("BM {0} Starting merge of [{1} - {2}) ({3})\n",
		           bmData->epoch,
		           mergeRange.begin.printable(),
		           mergeRange.end.printable(),
		           toMerge.size());
	}
	DebugBMTraceEvent(SevDebug, "BMGranuleMergeStart", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("NewGranule", mergeRange)
	    .detail("OldGranules", toMerge.size());

	CODE_PROBE(true, "Doing granule merge");
	bmData->activeGranuleMerges.insert(mergeRange, 0);
	bmData->setMergeCandidate(mergeRange, MergeCandidateMerging);
	// Now, after setting activeGranuleMerges, we have committed to doing the merge, so any subsequent split eval for
	// any of the ranges will be ignored. This handles merge/split races.
	bmData->addActor.send(doMerge(bmData, mergeRange, toMerge));
}

// Greedily merges any consecutive 2+ granules in a row that are mergeable
ACTOR Future<Void> attemptMerges(Reference<BlobManagerData> bmData,
                                 std::vector<std::tuple<UID, KeyRange, Version>> candidates) {
	ASSERT(candidates.size() >= 2);

	// TODO REMOVE validation eventually
	for (int i = 0; i < candidates.size() - 1; i++) {
		ASSERT(std::get<1>(candidates[i]).end == std::get<1>(candidates[i + 1]).begin);
	}
	CODE_PROBE(true, "Candidate ranges to merge");
	wait(bmData->concurrentMergeChecks.take());
	state FlowLock::Releaser holdingLock(bmData->concurrentMergeChecks);

	// start merging any set of 2+ consecutive granules that can be merged
	state int64_t currentBytes = 0;
	// large keys can cause a large number of granules in the merge to exceed the maximum value size
	state int currentKeySumBytes = 0;
	state std::vector<std::tuple<UID, KeyRange, Version>> currentCandidates;
	state int i;
	for (i = 0; i < candidates.size(); i++) {
		StorageMetrics metrics =
		    wait(bmData->db->getStorageMetrics(std::get<1>(candidates[i]), CLIENT_KNOBS->TOO_MANY));

		if (metrics.bytes >= SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES ||
		    metrics.bytesWrittenPerKSecond >= SERVER_KNOBS->SHARD_MIN_BYTES_PER_KSEC) {
			// This granule cannot be merged with any neighbors.
			// If current candidates up to here can be merged, merge them and skip over this one
			attemptStartMerge(bmData, currentCandidates);
			currentCandidates.clear();
			currentBytes = 0;
			currentKeySumBytes = 0;
			continue;
		}

		// if the current window is already at the maximum merge size, or adding this granule would push the window over
		// the edge, merge the existing candidates if possible
		ASSERT(currentCandidates.size() <= SERVER_KNOBS->BG_MAX_MERGE_FANIN);
		if (currentCandidates.size() == SERVER_KNOBS->BG_MAX_MERGE_FANIN ||
		    currentBytes + metrics.bytes > SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES ||
		    currentKeySumBytes >= CLIENT_KNOBS->VALUE_SIZE_LIMIT / 2) {
			ASSERT(currentBytes <= SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES);
			CODE_PROBE(currentKeySumBytes >= CLIENT_KNOBS->VALUE_SIZE_LIMIT / 2,
			           "merge early because of key size",
			           probe::decoration::rare);
			attemptStartMerge(bmData, currentCandidates);
			currentCandidates.clear();
			currentBytes = 0;
			currentKeySumBytes = 0;
		}

		// add this granule to the window
		if (currentCandidates.empty()) {
			currentKeySumBytes += std::get<1>(candidates[i]).begin.size();
		}
		currentKeySumBytes += std::get<1>(candidates[i]).end.size();
		currentCandidates.push_back(candidates[i]);
	}

	attemptStartMerge(bmData, currentCandidates);

	return Void();
}

// Uses single-pass algorithm to identify mergeable sections of granules.
// To ensure each granule waits to see whether all of its neighbors are merge-eligible before merging it, a newly
// merge-eligible granule will be ignored on the first pass
ACTOR Future<Void> granuleMergeChecker(Reference<BlobManagerData> bmData) {
	// wait for BM data to have loaded hard boundaries before starting
	wait(bmData->loadedClientRanges.getFuture());
	// initial sleep
	wait(delayJittered(SERVER_KNOBS->BG_MERGE_CANDIDATE_DELAY_SECONDS));
	// TODO could optimize to not check if there are no new merge-eligible granules and none in merge pending state
	loop {

		double sleepTime = SERVER_KNOBS->BG_MERGE_CANDIDATE_DELAY_SECONDS;
		// Check more frequently if speedUpSimulation is set. This may
		if (g_network->isSimulated() && g_simulator->speedUpSimulation) {
			sleepTime = std::min(5.0, sleepTime);
		}
		// start delay at the start of the loop, to account for time spend in calculation
		state Future<Void> intervalDelay = delayJittered(sleepTime);

		// go over granule states, and start a findMergeableGranules for each sub-range of mergeable granules
		// FIXME: avoid SlowTask by breaking this up periodically

		// Break it up into parallel chunks. This makes it possible to process large ranges, but does mean the merges
		// can be slightly suboptimal at boundaries. Use relatively large chunks to minimize the impact of this.
		int maxRangeSize = SERVER_KNOBS->BG_MAX_MERGE_FANIN * 10;

		state std::vector<Future<Void>> mergeChecks;
		auto allRanges = bmData->mergeCandidates.ranges();
		std::vector<std::tuple<UID, KeyRange, Version>> currentCandidates;
		auto& mergeBoundaries = bmData->mergeBoundaries;
		for (auto& it : allRanges) {
			// Conditions:
			//   1. Next range is not eligible.
			//   2. Hit the maximum in a merge evaluation window.
			//   3. Hit a hard merge boundary meaning we should not merge across them.
			if (!it->cvalue().mergeEligible() || currentCandidates.size() == maxRangeSize ||
			    bmData->mergeHardBoundaries.count(it->range().begin)) {
				if (currentCandidates.size() >= 2) {
					mergeChecks.push_back(attemptMerges(bmData, currentCandidates));
				}
				currentCandidates.clear();
			}

			// Soft boundaries.
			// We scan all ranges including non-eligible ranges which trigger non-consecutive merge flushes.
			if (!currentCandidates.empty()) {
				KeyRangeRef lastRange = std::get<1>(currentCandidates.back());
				KeyRangeRef curRange = it->range();
				ASSERT(lastRange.end == curRange.begin);
				// Conditions:
				//   1. Start a new soft merge range.
				//   2. End a soft merge range.
				if ((!mergeBoundaries.count(curRange.begin) && mergeBoundaries.count(curRange.end)) ||
				    (mergeBoundaries.count(lastRange.begin) && !mergeBoundaries.count(lastRange.end))) {
					if (currentCandidates.size() >= 2) {
						mergeChecks.push_back(attemptMerges(bmData, currentCandidates));
					}
					currentCandidates.clear();
				}
			}

			if (it->cvalue().mergeEligible()) {
				currentCandidates.push_back(std::tuple(it->cvalue().granuleID, it->range(), it->cvalue().startVersion));
			} else if (it->cvalue().canMerge()) {
				// set flag so this can get merged on the next pass
				it->value().seen = true;
			}
		}
		if (currentCandidates.size() >= 2) {
			mergeChecks.push_back(attemptMerges(bmData, currentCandidates));
		}

		CODE_PROBE(mergeChecks.size() > 1, "parallel merge checks");
		wait(waitForAll(mergeChecks));
		// if the calculation took longer than the desired interval, still wait a bit
		wait(intervalDelay && delay(5.0));
	}
}

ACTOR Future<Void> deregisterBlobWorker(Reference<BlobManagerData> bmData, BlobWorkerInterface interf) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	TraceEvent("BMDeregisteringWorker", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("WorkerId", interf.id())
	    .detail("WorkerAddr", interf.address());
	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			wait(checkManagerLock(tr, bmData));
			Key blobWorkerListKey = blobWorkerListKeyFor(interf.id());
			// FIXME: should be able to remove this conflict range
			tr->addReadConflictRange(singleKeyRange(blobWorkerListKey));
			tr->clear(blobWorkerListKey);

			BulkLoadMetadata::loaderListMap().erase(tr, interf.id());

			wait(tr->commit());

			TraceEvent("BMDeregisteredWorker", bmData->id)
			    .detail("Epoch", bmData->epoch)
			    .detail("WorkerId", interf.id())
			    .detail("WorkerAddr", interf.address());

			if (BM_DEBUG) {
				fmt::print("Deregistered blob worker {0}\n", interf.id().toString());
			}
			return Void();
		} catch (Error& e) {
			if (BM_DEBUG) {
				fmt::print("Deregistering blob worker {0} got error {1}\n", interf.id().toString(), e.name());
			}
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> haltBlobWorker(Reference<BlobManagerData> bmData, BlobWorkerInterface bwInterf) {
	loop {
		try {
			wait(bwInterf.haltBlobWorker.getReply(HaltBlobWorkerRequest(bmData->epoch, bmData->id)));
			break;
		} catch (Error& e) {
			// throw other errors instead of returning?
			if (e.code() == error_code_operation_cancelled) {
				throw;
			}
			if (e.code() != error_code_blob_manager_replaced) {
				break;
			}
			if (bmData->iAmReplaced.canBeSet()) {
				bmData->iAmReplaced.send(Void());
			}
			throw;
		}
	}

	return Void();
}

ACTOR Future<Void> killBlobWorker(Reference<BlobManagerData> bmData, BlobWorkerInterface bwInterf, bool registered) {
	state UID bwId = bwInterf.id();

	// Remove blob worker from stats map so that when we try to find a worker to takeover the range,
	// the one we just killed isn't considered.
	// Remove it from workersById also since otherwise that worker addr will remain excluded
	// when we try to recruit new blob workers.

	TraceEvent("KillBlobWorker", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("WorkerId", bwId)
	    .detail("Registered", registered);

	state Promise<Void> signalDead;

	if (registered) {
		auto it = bmData->workerStats.find(bwId);
		ASSERT(it != bmData->workerStats.end());
		signalDead = it->second.signalDead;
		bmData->deadWorkers.insert(bwId);
		bmData->workerStats.erase(bwId);
		bmData->workersById.erase(bwId);
		bmData->workerAddresses.erase(bwInterf.stableAddress());
	}

	// Remove blob worker from persisted list of blob workers
	Future<Void> deregister = deregisterBlobWorker(bmData, bwInterf);

	// for every range owned by this blob worker, we want to
	// - send a revoke request for that range
	// - add the range back to the stream of ranges to be assigned
	if (BM_DEBUG) {
		fmt::print("Taking back ranges from BW {0}\n", bwId.toString());
	}
	// copy ranges into vector before sending, because send then modifies workerAssignments
	state std::vector<KeyRange> rangesToMove;
	for (auto& it : bmData->workerAssignments.ranges()) {
		if (it.cvalue() == bwId) {
			rangesToMove.push_back(it.range());
		}
	}

	Optional<UID> successor = bwId;
	while (bmData->workerAffinities.count(successor.get())) {
		successor = bmData->workerAffinities[successor.get()];
	}
	if (successor.get() == bwId || !bmData->workersById.count(successor.get())) {
		successor = Optional<UID>();
	}

	CODE_PROBE(successor.present(), "Blob worker has affinity after failure");

	for (auto& it : rangesToMove) {
		// Send revoke request
		RangeAssignment raRevoke;
		raRevoke.isAssign = false;
		raRevoke.keyRange = it;
		raRevoke.revoke = RangeRevokeData(false);
		handleRangeAssign(bmData, raRevoke);

		// Add range back into the stream of ranges to be assigned
		RangeAssignment raAssign;
		raAssign.isAssign = true;
		raAssign.worker = successor;
		raAssign.keyRange = it;
		raAssign.assign = RangeAssignmentData(); // not a continue
		handleRangeAssign(bmData, raAssign);
	}

	// Send halt to blob worker, with no expectation of hearing back
	if (BM_DEBUG) {
		fmt::print("Sending halt to BW {}\n", bwId.toString());
	}
	bmData->addActor.send(haltBlobWorker(bmData, bwInterf));

	// wait for blob worker to be removed from DB and in-memory mapping to have reassigned all shards from this worker
	// before removing it from deadWorkers, to avoid a race with checkBlobWorkerList
	wait(deregister);

	// restart recruiting to replace the dead blob worker
	bmData->restartRecruiting.trigger();

	if (registered) {
		bmData->deadWorkers.erase(bwInterf.id());
	}

	signalDead.send(Void());

	TraceEvent("KillBlobWorkerComplete", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("WorkerId", bwId)
	    .detail("Registered", registered);

	return Void();
}

ACTOR Future<Void> monitorBlobWorkerStatus(Reference<BlobManagerData> bmData, BlobWorkerInterface bwInterf) {
	// outer loop handles reconstructing stream if it got a retryable error
	// do backoff, we can get a lot of retries in a row

	// wait for blob manager to be done recovering, so it has initial granule mapping and worker data
	wait(bmData->doneRecovering.getFuture());
	wait(delay(0));

	state double backoff = SERVER_KNOBS->BLOB_MANAGER_STATUS_EXP_BACKOFF_MIN;
	loop {
		try {
			state ReplyPromiseStream<GranuleStatusReply> statusStream =
			    bwInterf.granuleStatusStreamRequest.getReplyStream(GranuleStatusStreamRequest(bmData->epoch));
			// read from stream until worker fails (should never get explicit end_of_stream)
			loop {
				GranuleStatusReply rep = waitNext(statusStream.getFuture());

				++bmData->stats.granuleStreamReplies;

				if (BM_DEBUG) {
					fmt::print("BM {0} got status of [{1} - {2}) @ ({3}, {4}) from BW {5}: {6} {7}\n",
					           bmData->epoch,
					           rep.granuleRange.begin.printable(),
					           rep.granuleRange.end.printable(),
					           rep.continueEpoch,
					           rep.continueSeqno,
					           bwInterf.id().toString(),
					           rep.doSplit ? "split" : (rep.mergeCandidate ? "merge" : ""),
					           rep.mergeCandidate
					               ? ""
					               : (rep.writeHotSplit ? "hot" : (rep.initialSplitTooBig ? "toobig" : "normal")));
				}

				ASSERT(rep.doSplit || rep.mergeCandidate);

				// if we get a reply from the stream, reset backoff
				backoff = SERVER_KNOBS->BLOB_MANAGER_STATUS_EXP_BACKOFF_MIN;
				if (rep.continueEpoch > bmData->epoch) {
					if (BM_DEBUG) {
						fmt::print("BM {0} heard from BW {1} that there is a new manager with higher epoch\n",
						           bmData->epoch,
						           bwInterf.id().toString());
					}
					if (bmData->iAmReplaced.canBeSet()) {
						bmData->iAmReplaced.send(Void());
					}
					throw blob_manager_replaced();
				}

				BoundaryEvaluation newEval(rep.continueEpoch,
				                           rep.continueSeqno,
				                           rep.doSplit ? BoundaryEvalType::SPLIT : BoundaryEvalType::MERGE,
				                           rep.originalEpoch,
				                           rep.originalSeqno);

				bool ignore = false;
				Optional<std::pair<KeyRange, BoundaryEvaluation>> existingInProgress;
				auto lastBoundaryEvals = bmData->boundaryEvaluations.intersectingRanges(rep.granuleRange);
				for (auto& lastBoundaryEval : lastBoundaryEvals) {
					if (ignore) {
						break;
					}
					if (rep.granuleRange.begin == lastBoundaryEval.begin() &&
					    rep.granuleRange.end == lastBoundaryEval.end() && newEval == lastBoundaryEval.cvalue()) {
						if (BM_DEBUG) {
							fmt::print("BM {0} received repeat status for the same granule [{1} - {2}) {3}, "
							           "ignoring.\n",
							           bmData->epoch,
							           rep.granuleRange.begin.printable(),
							           rep.granuleRange.end.printable(),
							           newEval.toString());
						}
						DebugBMTraceEvent(SevDebug, "BlobManagerGotGranuleStreamRepeat");
						ignore = true;
					} else if (newEval < lastBoundaryEval.cvalue()) {
						CODE_PROBE(true, "BM got out-of-date split request");
						if (BM_DEBUG) {
							fmt::print("BM {0} ignoring status from BW {1} for granule [{2} - {3}) {4} since it "
							           "already processed [{5} - {6}) {7}.\n",
							           bmData->epoch,
							           bwInterf.id().toString().substr(0, 5),
							           rep.granuleRange.begin.printable(),
							           rep.granuleRange.end.printable(),
							           newEval.toString(),
							           lastBoundaryEval.begin().printable(),
							           lastBoundaryEval.end().printable(),
							           lastBoundaryEval.cvalue().toString());
						}

						DebugBMTraceEvent(SevDebug, "BlobManagerGotGranuleStreamIgnore")
						    .detail("Granule", rep.granuleRange)
						    .detail("NewEval", newEval.toString())
						    .detail("LastGranule", KeyRangeRef(lastBoundaryEval.begin(), lastBoundaryEval.end()))
						    .detail("LastEval", lastBoundaryEval.cvalue().toString());

						// only revoke if original epoch + seqno is older, different assignment
						if (newEval.isOlderThanOriginal(lastBoundaryEval.cvalue())) {
							// revoke range from out-of-date worker, but bypass rangeAssigner and hack (epoch, seqno) to
							// be (requesting epoch, requesting seqno + 1) to ensure no race with then reassigning the
							// range to the worker at a later version

							if (BM_DEBUG) {
								fmt::print("BM {0} revoking from BW {1} granule [{2} - {3}) {4} with original ({5}, "
								           "{6}) since it already processed original ({7}, {8}).\n",
								           bmData->epoch,
								           bwInterf.id().toString().substr(0, 5),
								           rep.granuleRange.begin.printable(),
								           rep.granuleRange.end.printable(),
								           newEval.toString(),
								           newEval.originalEpoch,
								           newEval.originalSeqno,
								           lastBoundaryEval.cvalue().originalEpoch,
								           lastBoundaryEval.cvalue().originalSeqno);
							}
							RangeAssignment revokeOld;
							revokeOld.isAssign = false;
							revokeOld.worker = bwInterf.id();
							revokeOld.keyRange = rep.granuleRange;
							revokeOld.revoke = RangeRevokeData(false);

							bmData->addActor.send(doRangeAssignment(
							    bmData, revokeOld, bwInterf.id(), rep.continueEpoch, rep.continueSeqno + 1));
						}

						ignore = true;
					} else if (lastBoundaryEval.cvalue().inProgress.isValid() &&
					           !lastBoundaryEval.cvalue().inProgress.isReady()) {
						existingInProgress = std::pair(lastBoundaryEval.range(), lastBoundaryEval.value());
					}
				}
				if (rep.doSplit && !ignore) {
					ASSERT(!rep.mergeCandidate);

					bool clearMergeCandidate = !existingInProgress.present() ||
					                           existingInProgress.get().second.type != BoundaryEvalType::MERGE;

					// Check for split/merge race
					Version inProgressMergeVersion = bmData->activeMergeVersion(rep.granuleRange);

					if (BM_DEBUG) {
						fmt::print("BM {0} splt eval [{1} - {2}). existing={3}, inProgressMergeVersion={4}, "
						           "blockedVersion={5}\n",
						           bmData->epoch,
						           rep.granuleRange.begin.printable().c_str(),
						           rep.granuleRange.end.printable().c_str(),
						           existingInProgress.present() ? "T" : "F",
						           inProgressMergeVersion,
						           rep.blockedVersion);
					}

					// If the in progress one is a merge, and the blockedVersion < the mergeVersion, this granule
					// needs to continue to flush up to the merge version. If the merge intent is still not
					// persisted, the version will be invalidVersion, so this should only happen after the merge
					// intent is persisted and the merge version is fixed. This can happen if a merge candidate
					// suddenly gets a burst of writes after a decision to merge is made
					if (inProgressMergeVersion != invalidVersion) {
						if (rep.blockedVersion < inProgressMergeVersion) {
							CODE_PROBE(true, "merge blocking re-snapshot");
							if (BM_DEBUG) {
								fmt::print("BM {0} MERGE @ {1} blocking re-snapshot [{2} - {3}) @ {4}, "
								           "continuing snapshot\n",
								           bmData->epoch,
								           inProgressMergeVersion,
								           rep.granuleRange.begin.printable(),
								           rep.granuleRange.end.printable(),
								           rep.blockedVersion);
							}
							RangeAssignment raContinue;
							raContinue.isAssign = true;
							raContinue.worker = bwInterf.id();
							raContinue.keyRange = rep.granuleRange;
							raContinue.assign =
							    RangeAssignmentData(AssignRequestType::Continue); // continue assignment and re-snapshot
							handleRangeAssign(bmData, raContinue);
						}
						DebugBMTraceEvent(SevDebug, "BlobManagerGotGranuleSplitBlockedByMerge", bmData->id)
						    .detail("Epoch", bmData->epoch)
						    .detail("Granule", rep.granuleRange)
						    .detail("BlockedVersion", rep.blockedVersion)
						    .detail("InProgressMergeVersion", inProgressMergeVersion);
						clearMergeCandidate = false;
						ignore = true;
					} else if (existingInProgress.present()) {
						// For example, one worker asked BM to split, then died, granule was moved, new worker asks
						// to split on recovery. We need to ensure that they are semantically the same split. We
						// will just rely on the in-progress split to finish
						if (BM_DEBUG) {
							fmt::print("BM {0} got request for [{1} - {2}) {3}, but already in "
							           "progress from [{4} - {5}) {6}\n",
							           bmData->epoch,
							           rep.granuleRange.begin.printable(),
							           rep.granuleRange.end.printable(),
							           newEval.toString(),
							           existingInProgress.get().first.begin.printable(),
							           existingInProgress.get().first.end.printable(),
							           existingInProgress.get().second.toString());
						}

						DebugBMTraceEvent(SevDebug, "BlobManagerGotGranuleStreamAlreadyProcessing")
						    .detail("Granule", rep.granuleRange)
						    .detail("NewEval", newEval.toString())
						    .detail("LastGranule", existingInProgress.get().first.begin)
						    .detail("LastEval", existingInProgress.get().second.toString());

						// ignore the request, they will retry
						ignore = true;
					}
					if (!ignore) {
						if (BM_DEBUG) {
							fmt::print("BM {0} evaluating [{1} - {2}) {3}\n",
							           bmData->epoch,
							           rep.granuleRange.begin.printable().c_str(),
							           rep.granuleRange.end.printable().c_str(),
							           newEval.toString());
						}
						if (rep.initialSplitTooBig) {
							ASSERT(rep.proposedSplitKey.present());
							newEval.inProgress = reevaluateInitialSplit(bmData,
							                                            bwInterf.id(),
							                                            rep.granuleRange,
							                                            rep.granuleID,
							                                            rep.originalEpoch,
							                                            rep.originalSeqno,
							                                            rep.proposedSplitKey.get());
						} else {
							newEval.inProgress = maybeSplitRange(bmData,
							                                     bwInterf.id(),
							                                     rep.granuleRange,
							                                     rep.granuleID,
							                                     rep.startVersion,
							                                     rep.writeHotSplit,
							                                     rep.originalEpoch,
							                                     rep.originalSeqno);
						}
						// also add to actor collection for error propagation
						bmData->addActor.send(newEval.inProgress);
						bmData->boundaryEvaluations.insert(rep.granuleRange, newEval);
					} else {
						++bmData->stats.granuleStreamRepliesIgnored;
					}

					// clear merge candidates for range, if not already merging
					if (clearMergeCandidate) {
						bmData->clearMergeCandidate(rep.granuleRange);
					}
				}
				if (rep.mergeCandidate && !ignore) {
					// mark granule as merge candidate
					ASSERT(!rep.doSplit);
					CODE_PROBE(true, "Granule merge candidate");
					if (BM_DEBUG) {
						fmt::print("Manager {0} merge candidate granule [{1} - {2}) {3}\n",
						           bmData->epoch,
						           rep.granuleRange.begin.printable().c_str(),
						           rep.granuleRange.end.printable().c_str(),
						           newEval.toString());
					}
					bmData->boundaryEvaluations.insert(rep.granuleRange, newEval);
					bmData->setMergeCandidate(rep.granuleRange, rep.granuleID, rep.startVersion);
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}

			DebugBMTraceEvent(SevDebug, "BlobManagerGotGranuleStreamError").errorUnsuppressed(e).suppressFor(10.0);

			// on known network errors or stream close errors, throw
			if (e.code() == error_code_broken_promise) {
				throw e;
			}

			// if manager is replaced, die
			if (e.code() == error_code_blob_manager_replaced) {
				if (bmData->iAmReplaced.canBeSet()) {
					bmData->iAmReplaced.send(Void());
				}
				return Void();
			}

			// if we got an error constructing or reading from stream that is retryable, wait and retry.
			// Sometimes we get connection_failed without the failure monitor tripping. One example is simulation's
			// rollRandomClose. In this case, just reconstruct the stream. If it was a transient failure, it works, and
			// if it is permanent, the failure monitor will eventually trip.
			ASSERT(e.code() != error_code_end_of_stream);
			if (e.code() == error_code_request_maybe_delivered || e.code() == error_code_connection_failed) {
				CODE_PROBE(true, "BM retrying BW monitoring");
				wait(delay(backoff));
				backoff = std::min(backoff * SERVER_KNOBS->BLOB_MANAGER_STATUS_EXP_BACKOFF_EXPONENT,
				                   SERVER_KNOBS->BLOB_MANAGER_STATUS_EXP_BACKOFF_MAX);
				continue;
			} else {
				TraceEvent(SevError, "BlobManagerUnexpectedErrorStatusMonitoring", bmData->id)
				    .error(e)
				    .detail("Epoch", bmData->epoch);
				ASSERT_WE_THINK(false);
				// if not simulation, kill the BM
				if (bmData->iAmReplaced.canBeSet()) {
					bmData->iAmReplaced.sendError(e);
				}
				throw e;
			}
		}
	}
}

ACTOR Future<Void> monitorBlobWorker(Reference<BlobManagerData> bmData, BlobWorkerInterface bwInterf) {
	try {
		state Future<Void> waitFailure = waitFailureClient(bwInterf.waitFailure, SERVER_KNOBS->BLOB_WORKER_TIMEOUT);
		state Future<Void> monitorStatus = monitorBlobWorkerStatus(bmData, bwInterf);
		// set to already run future so we check this first loop
		state Future<Void> exclusionsChanged = Future<Void>(Void());

		loop {
			choose {
				when(wait(waitFailure)) {
					if (SERVER_KNOBS->BLOB_WORKER_DISK_ENABLED) {
						wait(delay(SERVER_KNOBS->BLOB_WORKER_REJOIN_TIME));
					}
					if (BM_DEBUG) {
						fmt::print("BM {0} detected BW {1} is dead\n", bmData->epoch, bwInterf.id().toString());
					}
					TraceEvent("BlobWorkerFailed", bmData->id).detail("BlobWorkerID", bwInterf.id());
					break;
				}
				when(wait(monitorStatus)) {
					// should only return when manager got replaced
					ASSERT(!bmData->iAmReplaced.canBeSet());
					break;
				}
				when(wait(exclusionsChanged)) {
					// check to see if we were just excluded
					if (bmData->exclusionTracker.isFailedOrExcluded(bwInterf.stableAddress())) {
						TraceEvent("BlobWorkerExcluded", bmData->id)
						    .detail("BlobWorkerID", bwInterf.id())
						    .detail("Addr", bwInterf.stableAddress());
						break;
					}
					exclusionsChanged = bmData->exclusionTracker.changed.onTrigger();
				}
			}
		}
	} catch (Error& e) {
		// will blob worker get cleaned up in this case?
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}

		if (BM_DEBUG) {
			fmt::print(
			    "BM {0} got monitoring error {1} from BW {2}\n", bmData->epoch, e.name(), bwInterf.id().toString());
		}

		// Expected errors here are: [broken_promise]
		if (e.code() != error_code_broken_promise) {
			if (BM_DEBUG) {
				fmt::print("BM got unexpected error {0} monitoring BW {1}\n", e.name(), bwInterf.id().toString());
			}
			TraceEvent(SevError, "BlobManagerUnexpectedErrorMonitorBW", bmData->id)
			    .error(e)
			    .detail("Epoch", bmData->epoch);
			ASSERT_WE_THINK(false);
			// if not simulation, kill the BM
			if (bmData->iAmReplaced.canBeSet()) {
				bmData->iAmReplaced.sendError(e);
			}
			throw e;
		}
	}

	// kill the blob worker
	wait(killBlobWorker(bmData, bwInterf, true));

	if (BM_DEBUG) {
		fmt::print("No longer monitoring BW {0}\n", bwInterf.id().toString());
	}
	return Void();
}

ACTOR Future<Void> checkBlobWorkerList(Reference<BlobManagerData> bmData, Promise<Void> workerListReady) {

	try {
		loop {
			// Get list of last known blob workers
			// note: the list will include every blob worker that the old manager knew about,
			// but it might also contain blob workers that died while the new manager was being recruited
			state std::vector<BlobWorkerInterface> blobWorkers = wait(getBlobWorkers(bmData->db, true));

			// We could get the affinity list transactionally with the blob workers, however it is simpilier from an API
			// perspective to get the affinities after the blob worker list, which ensures we will have the affinity for
			// every worker returned.
			std::vector<std::pair<UID, UID>> blobWorkerAffinities = wait(getBlobWorkerAffinity(bmData->db, true));
			bmData->workerAffinities.clear();
			for (auto& it : blobWorkerAffinities) {
				bmData->workerAffinities[it.second] = it.first;
			}
			// add all blob workers to this new blob manager's records and start monitoring it
			bool foundAnyNew = false;
			int failedOrExcludedCount = 0;
			for (auto& worker : blobWorkers) {
				if (!bmData->deadWorkers.count(worker.id())) {
					bool isFailedOrExcluded = bmData->exclusionTracker.isFailedOrExcluded(worker.stableAddress());
					if (isFailedOrExcluded) {
						failedOrExcludedCount++;
					}
					if (!bmData->workerAddresses.count(worker.stableAddress()) &&
					    worker.locality.dcId() == bmData->dcId && !isFailedOrExcluded) {
						bmData->workerAddresses.insert(worker.stableAddress());
						bmData->workersById[worker.id()] = worker;
						bmData->workerStats[worker.id()] = BlobWorkerInfo();
						bmData->addActor.send(monitorBlobWorker(bmData, worker));
						foundAnyNew = true;
						TraceEvent("BMFoundNewWorker", bmData->id)
						    .detail("Epoch", bmData->epoch)
						    .detail("WorkerId", worker.id())
						    .detail("WorkerAddr", worker.address());
					} else if (!bmData->workersById.count(worker.id())) {
						TraceEvent("KillingExtraneousBlobWorker", bmData->id)
						    .detail("WorkerId", worker.id())
						    .detail("Addr", worker.stableAddress())
						    .detail("FailedOrExcluded", isFailedOrExcluded);
						bmData->addActor.send(killBlobWorker(bmData, worker, false));
					}
				}
			}
			if (workerListReady.canBeSet()) {
				workerListReady.send(Void());
			}
			// if any assigns are stuck on workers, and we have workers, wake them
			if (foundAnyNew || !bmData->workersById.empty()) {
				Promise<Void> hold = bmData->foundBlobWorkers;
				bmData->foundBlobWorkers = Promise<Void>();
				hold.send(Void());
			} else if (bmData->workersById.empty()) {
				CODE_PROBE(true, "blob manager found no workers");
				TraceEvent(SevWarn, "BlobManagerFoundNoWorkers", bmData->id)
				    .detail("Epoch", bmData->epoch)
				    .detail("InDBCount", blobWorkers.size())
				    .detail("DeadCount", bmData->deadWorkers.size())
				    .detail("FailedOrExcludedCount", failedOrExcludedCount);
			}
			wait(delay(SERVER_KNOBS->BLOB_WORKERLIST_FETCH_INTERVAL));
		}
	} catch (Error& e) {
		if (BM_DEBUG) {
			fmt::print("BM {0} got error {1} reading blob worker list!!\n", bmData->epoch, e.name());
		}
		throw e;
	}
}
// Shared code for handling KeyRangeMap<tuple(UID, epoch, seqno)> that is used several places in blob manager recovery
// when there can be conflicting sources of what assignments exist or which workers owns a granule.
// Resolves these conflicts by comparing the epoch + seqno for the range
// Special epoch/seqnos:
//   (0,0): range is not mapped
static void addAssignment(KeyRangeMap<std::tuple<UID, int64_t, int64_t, UID>>& map,
                          const KeyRangeRef& newRange,
                          UID newId,
                          int64_t newEpoch,
                          int64_t newSeqno,
                          std::vector<std::pair<UID, KeyRange>>& outOfDate) {
	std::vector<std::pair<KeyRange, std::tuple<UID, int64_t, int64_t, UID>>> newer;
	auto intersecting = map.intersectingRanges(newRange);
	bool allExistingNewer = true;
	bool anyConflicts = false;
	for (auto& old : intersecting) {
		UID oldWorker = std::get<0>(old.value());
		int64_t oldEpoch = std::get<1>(old.value());
		int64_t oldSeqno = std::get<2>(old.value());
		UID oldAffinity = std::get<3>(old.value());
		if (oldEpoch > newEpoch || (oldEpoch == newEpoch && oldSeqno > newSeqno)) {
			newer.push_back(std::pair(old.range(), std::tuple(oldWorker, oldEpoch, oldSeqno, oldAffinity)));
			if (old.range() != newRange) {
				CODE_PROBE(true, "BM Recovery: BWs disagree on range boundaries", probe::decoration::rare);
				anyConflicts = true;
			}
		} else {
			allExistingNewer = false;
			if (newId != UID() && newEpoch != std::numeric_limits<int64_t>::max()) {
				// different workers can't have same epoch and seqno for granule assignment
				ASSERT(oldEpoch != newEpoch || oldSeqno != newSeqno);
			}
			if (newEpoch == std::numeric_limits<int64_t>::max() && (oldWorker != newId || old.range() != newRange)) {
				CODE_PROBE(true, "BM Recovery: DB disagrees with workers");
				// new one is from DB (source of truth on boundaries) and existing mapping disagrees on boundary or
				// assignment, do explicit revoke and re-assign to converge
				anyConflicts = true;
				// if ranges don't match, need to explicitly reassign all parts of old range, as it could be from a
				// yet-unassigned split
				if (old.range() != newRange) {
					std::get<0>(old.value()) = UID();
				}
				if (oldWorker != UID() &&
				    (outOfDate.empty() || outOfDate.back() != std::pair(oldWorker, KeyRange(old.range())))) {

					outOfDate.push_back(std::pair(oldWorker, old.range()));
				}
			} else if (oldWorker != UID() && oldWorker != newId &&
			           (oldEpoch < newEpoch || (oldEpoch == newEpoch && oldSeqno < newSeqno))) {
				// 2 blob workers reported conflicting mappings, add old one to out of date (if not already added by a
				// previous intersecting range in the split case)
				// if ranges don't match, need to explicitly reassign all parts of old range, as it could be from a
				// partially-assigned split
				if (old.range() != newRange) {
					std::get<0>(old.value()) = UID();
				}
				if (outOfDate.empty() || outOfDate.back() != std::pair(oldWorker, KeyRange(old.range()))) {
					CODE_PROBE(
					    true, "BM Recovery: Two workers claim ownership of same granule", probe::decoration::rare);
					outOfDate.push_back(std::pair(oldWorker, old.range()));
				}
			}
		}
	}

	if (!allExistingNewer) {
		// if this range supersedes an old range insert it over that
		map.insert(newRange, std::tuple(anyConflicts ? UID() : newId, newEpoch, newSeqno, newId));

		// then, if there were any ranges superseded by this one, insert them over this one
		if (newer.size()) {
			if (newId != UID()) {
				outOfDate.push_back(std::pair(newId, newRange));
			}
			for (auto& it : newer) {
				map.insert(it.first, it.second);
			}
		}
	} else {
		if (newId != UID()) {
			outOfDate.push_back(std::pair(newId, newRange));
		}
	}
}

// essentially just error handling for resumed merge, since doMerge does it for new merge
ACTOR Future<Void> resumeMerge(Future<Void> finishMergeFuture, KeyRange mergeRange) {
	try {
		wait(finishMergeFuture);
		return Void();
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled || e.code() == error_code_blob_manager_replaced) {
			throw;
		}
		TraceEvent(SevError, "UnexpectedErrorResumeGranuleMerge").error(e).detail("Range", mergeRange);
		throw e;
	}
}

ACTOR Future<Void> loadForcePurgedRanges(Reference<BlobManagerData> bmData) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	state Key beginKey = blobGranuleForcePurgedKeys.begin;
	state int rowLimit = BUGGIFY ? deterministicRandom()->randomInt(2, 10) : 10000;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			// using the krm functions can produce incorrect behavior here as it does weird stuff with beginKey
			KeyRange nextRange(KeyRangeRef(beginKey, blobGranuleForcePurgedKeys.end));
			state GetRangeLimits limits(rowLimit, GetRangeLimits::BYTE_LIMIT_UNLIMITED);
			limits.minRows = 2;
			RangeReadResult results = wait(tr->getRange(nextRange, limits));

			// Add the mappings to our in memory key range map
			for (int rangeIdx = 0; rangeIdx < results.size() - 1; rangeIdx++) {
				if (results[rangeIdx].value == "1"_sr) {
					Key rangeStartKey = results[rangeIdx].key.removePrefix(blobGranuleForcePurgedKeys.begin);
					Key rangeEndKey = results[rangeIdx + 1].key.removePrefix(blobGranuleForcePurgedKeys.begin);
					// note: if the old owner is dead, we handle this in rangeAssigner
					bmData->forcePurgingRanges.insert(KeyRangeRef(rangeStartKey, rangeEndKey), true);
				}
			}

			if (!results.more || results.size() <= 1) {
				break;
			}

			// re-read last key to get range that starts there
			beginKey = results.back().key;
		} catch (Error& e) {
			if (BM_DEBUG) {
				fmt::print(
				    "BM {0} got error reading force purge ranges during recovery: {1}\n", bmData->epoch, e.name());
			}
			wait(tr->onError(e));
		}
	}

	return Void();
}

ACTOR Future<Void> resumeActiveMerges(Reference<BlobManagerData> bmData, Future<Void> loadForcePurgedRanges) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	wait(loadForcePurgedRanges); // load these first, to make sure to avoid resuming any merges that are being force
	                             // purged

	// FIXME: use range stream instead
	state int rowLimit = BUGGIFY ? deterministicRandom()->randomInt(1, 10) : 10000;
	state KeyRange currentRange = blobGranuleMergeKeys;
	state int found = 0;
	state Key startedThrough = blobGranuleMergeKeys.begin;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			wait(checkManagerLock(tr, bmData));

			state RangeReadResult result = wait(tr->getRange(currentRange, rowLimit));
			found += result.size();
			state std::vector<Future<Void>> cleanupForcePurged;
			for (auto& it : result) {
				CODE_PROBE(true, "Blob Manager Recovery found merging granule");
				UID mergeGranuleID = decodeBlobGranuleMergeKey(it.key);
				KeyRange mergeRange;
				std::vector<UID> parentGranuleIDs;
				std::vector<Key> parentGranuleRanges;
				std::vector<Version> parentGranuleStartVersions;
				Version mergeVersion;
				std::tie(mergeRange, mergeVersion, parentGranuleIDs, parentGranuleRanges, parentGranuleStartVersions) =
				    decodeBlobGranuleMergeValue(it.value);

				if (BM_DEBUG) {
					fmt::print("BM {0} found merge in progress: [{1} - {2}) @ {3}\n",
					           bmData->epoch,
					           mergeRange.begin.printable(),
					           mergeRange.end.printable(),
					           mergeVersion);
				}

				if (bmData->isForcePurging(mergeRange)) {
					if (BM_DEBUG) {
						fmt::print(
						    "BM {0} cleaning up merge [{1} - {2}): {3} @ {4} in progress b/c it was force purged\n",
						    bmData->epoch,
						    mergeRange.begin.printable(),
						    mergeRange.end.printable(),
						    mergeGranuleID.toString().substr(0, 6),
						    mergeVersion);
					}
					cleanupForcePurged.push_back(updateChangeFeed(&tr->getTransaction(),
					                                              granuleIDToCFKey(mergeGranuleID),
					                                              ChangeFeedStatus::CHANGE_FEED_DESTROY,
					                                              mergeRange));
					continue;
				}

				if (it.key <= startedThrough) {
					continue;
				}
				// want to mark in progress granule ranges as merging, BEFORE recovery is complete and workers can
				// report updated status. Start with early (epoch, seqno) to guarantee lower than later status
				BoundaryEvaluation eval(1, 0, BoundaryEvalType::MERGE, 1, 0);
				ASSERT(!bmData->isMergeActive(mergeRange));
				bmData->addActor.send(resumeMerge(finishMergeGranules(bmData,
				                                                      mergeGranuleID,
				                                                      mergeRange,
				                                                      mergeVersion,
				                                                      parentGranuleIDs,
				                                                      parentGranuleRanges,
				                                                      parentGranuleStartVersions),
				                                  mergeRange));
				bmData->boundaryEvaluations.insert(mergeRange, eval);
				bmData->activeGranuleMerges.insert(mergeRange, mergeVersion);
				bmData->setMergeCandidate(mergeRange, MergeCandidateMerging);
			}
			// if we already processed these merges, but the transaction fails on deleting any change feeds from ranges
			// that are force purging, just retry the feed deletes
			if (result.empty()) {
				startedThrough = blobGranuleMergeKeys.end;
			} else {
				startedThrough = result.back().key;
			}

			if (!cleanupForcePurged.empty()) {
				wait(waitForAll(cleanupForcePurged));
				wait(tr->commit());
				tr->reset();
			}

			if (result.more) {
				currentRange = KeyRangeRef(keyAfter(result.back().key), currentRange.end);
			} else {
				break;
			}
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
	TraceEvent("BMResumeMerges", bmData->id).detail("Epoch", bmData->epoch).detail("Count", found);
	return Void();
}

ACTOR Future<Void> loadBlobGranuleMergeBoundaries(Reference<BlobManagerData> bmData) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	state int rowLimit = BUGGIFY ? deterministicRandom()->randomInt(2, 10) : 10000;
	state Key beginKey = blobGranuleMergeBoundaryKeys.begin;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			KeyRange nextRange(KeyRangeRef(beginKey, blobGranuleMergeBoundaryKeys.end));
			// using the krm functions can produce incorrect behavior here as it does weird stuff with beginKey
			state GetRangeLimits limits(rowLimit, GetRangeLimits::BYTE_LIMIT_UNLIMITED);
			RangeReadResult results = wait(tr->getRange(nextRange, limits));

			// Add the mappings to our in memory key range map
			for (int i = 0; i < results.size(); i++) {
				bmData->mergeBoundaries[results[i].key.removePrefix(blobGranuleMergeBoundaryKeys.begin)] =
				    decodeBlobGranuleMergeBoundaryValue(results[i].value);
			}

			if (!results.more) {
				break;
			}
			beginKey = keyAfter(results.back().key);
		} catch (Error& e) {
			if (BM_DEBUG) {
				fmt::print("BM {0} got error reading granule merge boundaries during recovery: {1}\n",
				           bmData->epoch,
				           e.name());
			}
			wait(tr->onError(e));
		}
	}

	return Void();
}

// Update blob manager epoc for restore. The blob manager should have larger epoch after restore
// so that it could generate new manifest files with larger epoch number. Blob manifest files
// with largest epoch number is treated as the newest.
ACTOR Future<Void> updateEpoch(Reference<BlobManagerData> bmData, int64_t epoch) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		try {
			ValueReadResult oldEpoch = wait(tr->get(blobManagerEpochKey));
			state int64_t oldEpochValue = oldEpoch.present() ? decodeBlobManagerEpochValue(oldEpoch.get()) : 1;
			if (oldEpochValue < epoch) {
				tr->set(blobManagerEpochKey, blobManagerEpochValueFor(epoch));
				wait(tr->commit());
				TraceEvent("BlobManagerEpoc").detail("Epoch", epoch);
				bmData->epoch = epoch;
			}
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> recoverBlobManager(Reference<BlobManagerData> bmData) {
	state double recoveryStartTime = now();
	state Promise<Void> workerListReady;
	state Future<Void> blobWorkerRejoin =
	    delay(SERVER_KNOBS->BLOB_WORKER_DISK_ENABLED ? SERVER_KNOBS->BLOB_WORKER_REJOIN_TIME : 0);
	bmData->addActor.send(checkBlobWorkerList(bmData, workerListReady));
	wait(workerListReady.getFuture());

	state std::vector<BlobWorkerInterface> startingWorkers;
	for (auto& it : bmData->workersById) {
		startingWorkers.push_back(it.second);
	}

	// Once we acknowledge the existing blob workers, we can go ahead and recruit new ones
	bmData->startRecruiting.trigger();

	bmData->initBStore();

	state Reference<BlobRestoreController> restoreController =
	    makeReference<BlobRestoreController>(bmData->db, normalKeys);
	bool isFullRestore = wait(BlobRestoreController::isRestoring(restoreController));
	bmData->isFullRestoreMode = isFullRestore;
	if (bmData->isFullRestoreMode) {
		state BlobRestorePhase phase = wait(BlobRestoreController::currentPhase(restoreController));
		if (phase == STARTING_MIGRATOR || phase == LOADING_MANIFEST) {
			wait(BlobRestoreController::setPhase(restoreController, LOADING_MANIFEST, {}));
			try {
				auto db = SystemDBWriteLockedNow(bmData->db.getReference());
				std::string url = wait(BlobGranuleRestoreConfig().manifestUrl().getD(db));
				state Reference<BlobConnectionProvider> manifestStore =
				    BlobConnectionProvider::newBlobConnectionProvider(url);
				wait(loadManifest(bmData->db, bmData->dbInfo, manifestStore));
				int64_t epoc = wait(lastBlobEpoc(bmData->db, bmData->dbInfo, manifestStore));
				wait(updateEpoch(bmData, epoc + 1));
				wait(BlobRestoreController::setPhase(restoreController, LOADED_MANIFEST, {}));
				TraceEvent("BlobManifestLoaded", bmData->id).log();
			} catch (Error& e) {
				if (e.code() != error_code_restore_missing_data &&
				    e.code() != error_code_blob_restore_missing_manifest) {
					throw e; // retryable errors
				}
				// terminate blob restore for non-retryable errors
				TraceEvent("ManifestLoadError", bmData->id).error(e).detail("Phase", phase);
				std::string errorMessage = fmt::format("Manifest loading error '{}'", e.what());
				wait(BlobRestoreController::setError(restoreController, errorMessage));
			}
		}
	}

	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);

	// set up force purge keys if not done already
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeReadResult existingForcePurgeKeys = wait(tr->getRange(blobGranuleForcePurgedKeys, 1));
			if (!existingForcePurgeKeys.empty()) {
				break;
			}
			wait(checkManagerLock(tr, bmData));
			wait(krmSetRange(tr, blobGranuleForcePurgedKeys.begin, normalKeys, "0"_sr));
			wait(tr->commit());
			tr->reset();
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	// skip the rest of the algorithm for the first blob manager
	if (bmData->epoch == 1) {
		bmData->doneRecovering.send(Void());
		return Void();
	}

	state Future<Void> forcePurgedRanges = loadForcePurgedRanges(bmData);
	state Future<Void> resumeMergesFuture = resumeActiveMerges(bmData, forcePurgedRanges);

	CODE_PROBE(true, "BM doing recovery");

	wait(delay(0));

	// At this point, bmData->workersById is a list of all alive blob workers, but could also include some dead BWs.
	// The algorithm below works as follows:
	//
	// 1. We get the existing granule mappings. We do this by asking all active blob workers for their current granule
	//    assignments. This guarantees a consistent snapshot of the state of that worker's assignments: Any request it
	//    recieved and processed from the old manager before the granule assignment request will be included in the
	//    assignments, and any request it recieves from the old manager afterwards will be rejected with
	//    blob_manager_replaced. We then read from the database as the source of truth for the assignment. We will
	//    reconcile the set of ongoing splits to this mapping, and any ranges that are not already assigned to existing
	//    blob workers will be reassigned.
	//
	// 2. For every range in our granuleAssignments, we send an assign request to the stream of requests,
	//    ultimately giving every range back to some worker (trying to mimic the state of the old BM).
	//    If the worker already had the range, this is a no-op. If the worker didn't have it, it will
	//    begin persisting it. The worker that had the same range before will now be at a lower seqno.

	state KeyRangeMap<std::tuple<UID, int64_t, int64_t, UID>> workerAssignments;
	workerAssignments.insert(normalKeys, std::tuple(UID(), 0, 0, UID()));

	// FIXME: use range stream instead
	state int rowLimit = BUGGIFY ? deterministicRandom()->randomInt(2, 10) : 10000;

	if (BM_DEBUG) {
		fmt::print("BM {0} recovering:\n", bmData->epoch);
	}

	// Step 1. Get the latest known mapping of granules to blob workers (i.e. assignments)
	// This must happen causally AFTER reading the split boundaries, since the blob workers can clear the split
	// boundaries for a granule as part of persisting their assignment.

	// First, ask existing workers for their mapping
	if (BM_DEBUG) {
		fmt::print("BM {0} requesting assignments from {1} workers:\n", bmData->epoch, startingWorkers.size());
	}
	state std::vector<Future<Optional<GetGranuleAssignmentsReply>>> aliveAssignments;
	aliveAssignments.reserve(startingWorkers.size());
	for (auto& it : startingWorkers) {
		GetGranuleAssignmentsRequest req;
		req.managerEpoch = bmData->epoch;
		aliveAssignments.push_back(timeout(brokenPromiseToNever(it.granuleAssignmentsRequest.getReply(req)),
		                                   SERVER_KNOBS->BLOB_WORKER_TIMEOUT));
	}

	state std::vector<std::pair<UID, KeyRange>> outOfDateAssignments;
	state int successful = 0;
	state int assignIdx = 0;

	for (; assignIdx < aliveAssignments.size(); assignIdx++) {
		Optional<GetGranuleAssignmentsReply> reply = wait(aliveAssignments[assignIdx]);
		UID workerId = startingWorkers[assignIdx].id();

		if (reply.present()) {
			TraceEvent("BMRecoveredWorker", bmData->id)
			    .detail("Epoch", bmData->epoch)
			    .detail("BWID", workerId)
			    .detail("Assignments", reply.get().assignments.size());
			if (BM_DEBUG) {
				fmt::print("  Worker {}: ({})\n", workerId.toString().substr(0, 5), reply.get().assignments.size());
			}
			successful++;
			for (auto& assignment : reply.get().assignments) {
				if (BM_DEBUG) {
					fmt::print("    [{0} - {1}): ({2}, {3})\n",
					           assignment.range.begin.printable(),
					           assignment.range.end.printable(),
					           assignment.epochAssigned,
					           assignment.seqnoAssigned);
				}
				bmData->knownBlobRanges.insert(assignment.range, true);
				addAssignment(workerAssignments,
				              assignment.range,
				              workerId,
				              assignment.epochAssigned,
				              assignment.seqnoAssigned,
				              outOfDateAssignments);
			}
			if (bmData->workerStats.count(workerId)) {
				bmData->workerStats[workerId].numGranulesAssigned = reply.get().assignments.size();
			}
		} else {
			TraceEvent("BMRecoveredWorkerFailed", bmData->id).detail("Epoch", bmData->epoch).detail("BWID", workerId);
			CODE_PROBE(true, "BM Recovery: BW didn't respond to assignments request");
			// SOMEDAY: mark as failed and kill it
			if (BM_DEBUG) {
				fmt::print("  Worker {}: failed\n", workerId.toString().substr(0, 5));
			}
		}
	}

	if (BM_DEBUG) {
		fmt::print("BM {0} got assignments from {1}/{2} workers:\n", bmData->epoch, successful, startingWorkers.size());
	}
	TraceEvent("BMRecoveredWorkers", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("AliveWorkers", successful)
	    .detail("DeadWorkers", startingWorkers.size() - successful);

	if (BM_DEBUG) {
		fmt::print("BM {0} found old assignments:\n", bmData->epoch);
	}

	// DB is the source of truth, so read from here, and resolve any conflicts with current worker mapping
	// We don't have a consistent snapshot of the mapping ACROSS blob workers, so we need the DB to reconcile any
	// differences (eg blob manager revoked from worker A, assigned to B, the revoke from A was processed but the assign
	// to B wasn't, meaning in the snapshot nobody owns the granule). This also handles races with a BM persisting a
	// boundary change, then dying before notifying the workers
	state Key beginKey = blobGranuleMappingKeys.begin;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			KeyRange nextRange(KeyRangeRef(beginKey, blobGranuleMappingKeys.end));
			// using the krm functions can produce incorrect behavior here as it does weird stuff with beginKey
			state GetRangeLimits limits(rowLimit, GetRangeLimits::BYTE_LIMIT_UNLIMITED);
			limits.minRows = 2;
			RangeReadResult results = wait(tr->getRange(nextRange, limits));

			// Add the mappings to our in memory key range map
			for (int rangeIdx = 0; rangeIdx < results.size() - 1; rangeIdx++) {
				Key granuleStartKey = results[rangeIdx].key.removePrefix(blobGranuleMappingKeys.begin);
				Key granuleEndKey = results[rangeIdx + 1].key.removePrefix(blobGranuleMappingKeys.begin);
				if (results[rangeIdx].value.size()) {
					// note: if the old owner is dead, we handle this in rangeAssigner
					UID existingOwner = decodeBlobGranuleMappingValue(results[rangeIdx].value);
					// use (max int64_t, 0) to be higher than anything that existing workers have
					addAssignment(workerAssignments,
					              KeyRangeRef(granuleStartKey, granuleEndKey),
					              existingOwner,
					              std::numeric_limits<int64_t>::max(),
					              0,
					              outOfDateAssignments);

					bmData->knownBlobRanges.insert(KeyRangeRef(granuleStartKey, granuleEndKey), true);
					if (BM_DEBUG) {
						fmt::print("  [{0} - {1})={2}\n",
						           granuleStartKey.printable(),
						           granuleEndKey.printable(),
						           existingOwner.toString().substr(0, 5));
					}
				} else {
					if (BM_DEBUG) {
						fmt::print("  [{0} - {1})\n", granuleStartKey.printable(), granuleEndKey.printable());
					}
				}
			}

			if (!results.more || results.size() <= 1) {
				break;
			}

			// re-read last key to get range that starts there
			beginKey = results.back().key;
		} catch (Error& e) {
			if (BM_DEBUG) {
				fmt::print("BM {0} got error reading granule mapping during recovery: {1}\n", bmData->epoch, e.name());
			}
			wait(tr->onError(e));
		}
	}

	wait(forcePurgedRanges);
	wait(resumeMergesFuture);

	// Step 2. Send assign requests for all the granules and transfer assignments
	// from local workerAssignments to bmData
	// before we take ownership of all of the ranges, check the manager lock again
	tr->reset();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkManagerLock(tr, bmData));
			wait(tr->commit());
			break;
		} catch (Error& e) {
			if (BM_DEBUG) {
				fmt::print("BM {0} got error checking lock after recovery: {1}\n", bmData->epoch, e.name());
			}
			wait(tr->onError(e));
		}
	}

	if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
		throw blob_manager_replaced();
	}

	// Allow time for the worker affinity map to be loaded
	wait(blobWorkerRejoin);

	// Get set of workers again. Some could have died after reporting assignments
	std::unordered_set<UID> endingWorkers;
	for (auto& it : bmData->workersById) {
		endingWorkers.insert(it.first);
	}

	// revoke assignments that are old and incorrect
	CODE_PROBE(!outOfDateAssignments.empty(), "BM resolved conflicting assignments on recovery");
	for (auto& it : outOfDateAssignments) {
		if (BM_DEBUG) {
			fmt::print("BM {0} revoking out of date assignment [{1} - {2}): {3}:\n",
			           bmData->epoch,
			           it.second.begin.printable().c_str(),
			           it.second.end.printable().c_str(),
			           it.first.toString().c_str());
		}
		RangeAssignment raRevoke;
		raRevoke.isAssign = false;
		raRevoke.worker = it.first;
		raRevoke.keyRange = it.second;
		raRevoke.revoke = RangeRevokeData(false);
		handleRangeAssign(bmData, raRevoke);
	}

	if (BM_DEBUG) {
		fmt::print("BM {0} final ranges:\n", bmData->epoch);
	}

	state int totalGranules = 0;
	state int explicitAssignments = 0;
	for (auto& range : workerAssignments.intersectingRanges(normalKeys)) {
		int64_t epoch = std::get<1>(range.value());
		int64_t seqno = std::get<2>(range.value());
		if (epoch == 0 && seqno == 0) {
			continue;
		}

		totalGranules++;

		UID workerId = std::get<0>(range.value());
		UID workerAffinity = std::get<3>(range.value());
		bmData->workerAssignments.insert(range.range(), workerId);

		if (BM_DEBUG) {
			fmt::print("  [{0} - {1}): {2}\n",
			           range.begin().printable(),
			           range.end().printable(),
			           workerId == UID() || epoch == 0 ? " (?)" : workerId.toString().substr(0, 5).c_str());
		}

		// if worker id is already set to a known worker that replied with it in the mapping, range is already assigned
		// there. If not, need to explicitly assign it to someone
		if (workerId == UID() || epoch == 0 || !endingWorkers.count(workerId)) {
			if (workerId == UID()) {
				workerId = workerAffinity;
			}
			while (bmData->workerAffinities.count(workerId)) {
				workerId = bmData->workerAffinities[workerId];
				CODE_PROBE(true, "Blob worker has affinity after reboot");
			}
			// prevent racing status updates from old owner from causing issues until this request gets sent out
			// properly
			bmData->boundaryEvaluations.insert(
			    range.range(), BoundaryEvaluation(bmData->epoch, 0, BoundaryEvalType::RECOVER, bmData->epoch, 0));

			RangeAssignment raAssign;
			raAssign.isAssign = true;
			if (bmData->workersById.count(workerId)) {
				raAssign.worker = workerId;
			}
			raAssign.keyRange = range.range();
			raAssign.assign = RangeAssignmentData(AssignRequestType::Normal);
			handleRangeAssign(bmData, raAssign);
			explicitAssignments++;
		}
	}

	// Load tenant data before letting blob granule operations continue.
	tr->reset();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(loadTenantMap(tr, bmData));
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	wait(loadBlobGranuleMergeBoundaries(bmData));

	TraceEvent("BlobManagerRecovered", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("Duration", now() - recoveryStartTime)
	    .detail("Granules", totalGranules)
	    .detail("Assigned", explicitAssignments)
	    .detail("Revoked", outOfDateAssignments.size());

	ASSERT(bmData->doneRecovering.canBeSet());
	bmData->doneRecovering.send(Void());

	if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
		throw blob_manager_replaced();
	}

	return Void();
}

ACTOR Future<Void> chaosRangeMover(Reference<BlobManagerData> bmData) {
	// Only move each granule once during the test, otherwise it can cause availability issues
	// KeyRange isn't hashable and this is only for simulation, so just use toString of range
	state std::unordered_set<std::string> alreadyMoved;
	ASSERT(g_network->isSimulated());
	CODE_PROBE(true, "BM chaos range mover enabled");
	loop {
		wait(delay(30.0));

		if (g_simulator->speedUpSimulation) {
			if (BM_DEBUG) {
				printf("Range mover stopping\n");
			}
			return Void();
		}

		if (bmData->workersById.size() > 1) {
			int tries = 10;
			while (tries > 0) {
				tries--;
				auto randomRange = bmData->workerAssignments.randomRange();
				if (randomRange.value() != UID() && !alreadyMoved.count(randomRange.range().toString())) {
					if (BM_DEBUG) {
						fmt::print("Range mover moving range [{0} - {1}): {2}\n",
						           randomRange.begin().printable().c_str(),
						           randomRange.end().printable().c_str(),
						           randomRange.value().toString().c_str());
					}
					alreadyMoved.insert(randomRange.range().toString());

					// FIXME: with low probability, could immediately revoke it from the new assignment and move
					// it back right after to test that race

					state KeyRange range = randomRange.range();
					RangeAssignment revokeOld;
					revokeOld.isAssign = false;
					revokeOld.keyRange = range;
					revokeOld.revoke = RangeRevokeData(false);
					handleRangeAssign(bmData, revokeOld);

					RangeAssignment assignNew;
					assignNew.isAssign = true;
					assignNew.keyRange = range;
					assignNew.assign = RangeAssignmentData(); // not a continue
					handleRangeAssign(bmData, assignNew);
					break;
				}
			}
			if (tries == 0 && BM_DEBUG) {
				printf("Range mover couldn't find random range to move, skipping\n");
			}
		} else if (BM_DEBUG) {
			fmt::print("Range mover found {0} workers, skipping\n", bmData->workerAssignments.size());
		}
	}
}

// Returns the number of blob workers on addr
int numExistingBWOnAddr(Reference<BlobManagerData> self, const AddressExclusion& addr) {
	int numExistingBW = 0;
	for (auto& server : self->workersById) {
		const NetworkAddress& netAddr = server.second.stableAddress();
		AddressExclusion usedAddr(netAddr.ip, netAddr.port);
		if (usedAddr == addr) {
			++numExistingBW;
		}
	}

	return numExistingBW;
}

// Tries to recruit a blob worker on the candidateWorker process
ACTOR Future<Void> initializeBlobWorker(Reference<BlobManagerData> self,
                                        RecruitBlobWorkerReply candidateWorker,
                                        DatabaseConfiguration config) {
	const NetworkAddress& netAddr = candidateWorker.worker.stableAddress();
	AddressExclusion workerAddr(netAddr.ip, netAddr.port);
	self->recruitingStream.set(self->recruitingStream.get() + 1);

	// Ask the candidateWorker to initialize a BW only if the worker does not have a pending request
	if (numExistingBWOnAddr(self, workerAddr) == 0 &&
	    self->recruitingLocalities.count(candidateWorker.worker.stableAddress()) == 0) {
		state UID interfaceId = deterministicRandom()->randomUniqueID();

		state InitializeBlobWorkerRequest initReq;
		initReq.reqId = deterministicRandom()->randomUniqueID();
		initReq.interfaceId = interfaceId;
		initReq.storeType = (KeyValueStoreType::StoreType)(SERVER_KNOBS->BLOB_WORKER_STORE_TYPE);
		initReq.encryptMode = config.encryptionAtRestMode;

		// acknowledge that this worker is currently being recruited on
		self->recruitingLocalities.insert(candidateWorker.worker.stableAddress());

		TraceEvent("BMRecruiting", self->id)
		    .detail("Epoch", self->epoch)
		    .detail("State", "Sending request to worker")
		    .detail("WorkerID", candidateWorker.worker.id())
		    .detail("WorkerLocality", candidateWorker.worker.locality.toString())
		    .detail("Interf", interfaceId)
		    .detail("Addr", candidateWorker.worker.address());

		// send initialization request to worker (i.e. worker.actor.cpp)
		// here, the worker will construct the blob worker at which point the BW will start!
		Future<ErrorOr<InitializeBlobWorkerReply>> fRecruit =
		    candidateWorker.worker.blobWorker.tryGetReply(initReq, TaskPriority::BlobManager);

		// wait on the reply to the request
		state ErrorOr<InitializeBlobWorkerReply> newBlobWorker = wait(fRecruit);

		// if the initialization failed in an unexpected way, then kill the BM.
		// if it failed in an expected way, add some delay before we try to recruit again
		// on this worker
		if (newBlobWorker.isError()) {
			CODE_PROBE(true, "BM got error recruiting BW");
			TraceEvent(SevWarn, "BMRecruitmentError", self->id)
			    .error(newBlobWorker.getError())
			    .detail("Epoch", self->epoch)
			    .detail("WorkerID", candidateWorker.worker.id())
			    .detail("WorkerLocality", candidateWorker.worker.locality.toString())
			    .detail("Interf", interfaceId)
			    .detail("Addr", candidateWorker.worker.address());
			if (!newBlobWorker.isError(error_code_recruitment_failed) &&
			    !newBlobWorker.isError(error_code_request_maybe_delivered)) {
				throw newBlobWorker.getError();
			}
			wait(delay(SERVER_KNOBS->STORAGE_RECRUITMENT_DELAY, TaskPriority::BlobManager));
		}

		// if the initialization succeeded, add the blob worker's interface to
		// the blob manager's data and start monitoring the blob worker
		if (newBlobWorker.present()) {
			BlobWorkerInterface bwi = newBlobWorker.get().interf;

			if (!self->deadWorkers.count(bwi.id())) {
				if (!self->workerAddresses.count(bwi.stableAddress()) && bwi.locality.dcId() == self->dcId) {
					self->workerAddresses.insert(bwi.stableAddress());
					self->workersById[bwi.id()] = bwi;
					self->workerStats[bwi.id()] = BlobWorkerInfo();
					self->addActor.send(monitorBlobWorker(self, bwi));
				} else if (!self->workersById.count(bwi.id())) {
					self->addActor.send(killBlobWorker(self, bwi, false));
				}
			}

			TraceEvent("BMRecruiting", self->id)
			    .detail("Epoch", self->epoch)
			    .detail("State", "Finished request")
			    .detail("WorkerID", candidateWorker.worker.id())
			    .detail("WorkerLocality", candidateWorker.worker.locality.toString())
			    .detail("Interf", interfaceId)
			    .detail("Addr", candidateWorker.worker.address());
		}

		// acknowledge that this worker is not actively being recruited on anymore.
		// if the initialization did succeed, then this worker will still be excluded
		// since it was added to workersById.
		self->recruitingLocalities.erase(candidateWorker.worker.stableAddress());
	}

	// try to recruit more blob workers
	self->recruitingStream.set(self->recruitingStream.get() - 1);
	self->restartRecruiting.trigger();
	return Void();
}

// Recruits blob workers in a loop
ACTOR Future<Void> blobWorkerRecruiter(
    Reference<BlobManagerData> self,
    Reference<IAsyncListener<RequestStream<RecruitBlobWorkerRequest>>> recruitBlobWorker) {
	state Future<RecruitBlobWorkerReply> fCandidateWorker;
	state RecruitBlobWorkerRequest lastRequest;

	// wait until existing blob workers have been acknowledged so we don't break recruitment invariants
	loop choose {
		when(wait(self->startRecruiting.onTrigger())) {
			break;
		}
	}

	state DatabaseConfiguration config = wait(getDatabaseConfiguration(self->db, true));
	state double lastVerboseLogTs = 0.0;

	loop {
		try {
			state RecruitBlobWorkerRequest recruitReq;

			// SevDebug normally but SevInfo if we have no active workers
			Severity excludeSeverity = SevDebug;
			if (self->workersById.empty() && now() - lastVerboseLogTs >= 10.0) {
				excludeSeverity = SevInfo;
				lastVerboseLogTs = now();
			}

			// workers that are used by existing blob workers should be excluded
			for (auto const& [bwId, bwInterf] : self->workersById) {
				auto addr = bwInterf.stableAddress();
				AddressExclusion addrExcl(addr.ip, addr.port);
				recruitReq.excludeAddresses.emplace_back(addrExcl);
				TraceEvent(excludeSeverity, "BMRecruitExcl1").detail("Excluding", addr);
			}

			// workers that are used by blob workers that are currently being recruited should be excluded
			for (auto addr : self->recruitingLocalities) {
				recruitReq.excludeAddresses.emplace_back(AddressExclusion(addr.ip, addr.port));
				TraceEvent(excludeSeverity, "BMRecruitExcl2").detail("Excluding", addr);
			}

			// don't recruit on excluded or failed addresses
			CODE_PROBE(!self->exclusionTracker.excluded.empty(), "ignoring excluded hosts in BM recruitment");
			CODE_PROBE(!self->exclusionTracker.failed.empty(),
			           "ignoring failed hosts in BM recruitment",
			           probe::decoration::rare);

			for (auto addr : self->exclusionTracker.excluded) {
				recruitReq.excludeAddresses.push_back(addr);
			}

			for (auto addr : self->exclusionTracker.failed) {
				recruitReq.excludeAddresses.push_back(addr);
			}

			TraceEvent("BMRecruiting", self->id)
			    .detail("Epoch", self->epoch)
			    .detail("ExcludedCount", recruitReq.excludeAddresses.size())
			    .detail("ExistingCount", self->workersById.size())
			    .detail("InProgressCount", self->recruitingLocalities.size())
			    .detail("ExternalCount", self->exclusionTracker.excluded.size() + self->exclusionTracker.failed.size())
			    .detail("DeadCount", self->deadWorkers.size())
			    .detail("State", "Sending request to CC");

			if (!fCandidateWorker.isValid() || fCandidateWorker.isReady() ||
			    recruitReq.excludeAddresses != lastRequest.excludeAddresses) {
				lastRequest = recruitReq;
				// send req to cluster controller to get back a candidate worker we can recruit on
				fCandidateWorker =
				    brokenPromiseToNever(recruitBlobWorker->get().getReply(recruitReq, TaskPriority::BlobManager));
			}

			choose {
				// when we get back a worker we can use, we will try to initialize a blob worker onto that
				// process
				when(RecruitBlobWorkerReply candidateWorker = wait(fCandidateWorker)) {
					self->addActor.send(initializeBlobWorker(self, candidateWorker, config));
				}

				// when the CC changes, so does the request stream so we need to restart recruiting here
				when(wait(recruitBlobWorker->onChange())) {
					fCandidateWorker = Future<RecruitBlobWorkerReply>();
				}

				// signal used to restart the loop and try to recruit the next blob worker
				when(wait(self->restartRecruiting.onTrigger())) {}

				// signal used to restart the loop and update request to CC with new exclusions
				when(wait(self->exclusionTracker.changed.onTrigger())) {}
			}
			wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY, TaskPriority::BlobManager));
		} catch (Error& e) {
			if (e.code() != error_code_timed_out) {
				throw;
			}
			CODE_PROBE(true, "Blob worker recruitment timed out");
		}
	}
}

ACTOR Future<Void> haltBlobGranules(Reference<BlobManagerData> bmData) {
	std::vector<BlobWorkerInterface> blobWorkers = wait(getBlobWorkers(bmData->db, true));
	std::vector<Future<Void>> deregisterBlobWorkers;
	for (auto& worker : blobWorkers) {
		bmData->addActor.send(haltBlobWorker(bmData, worker));
		deregisterBlobWorkers.emplace_back(deregisterBlobWorker(bmData, worker));
	}
	waitForAll(deregisterBlobWorkers);

	return Void();
}

ACTOR Future<bool> canDeleteFullGranuleSplit(Reference<BlobManagerData> self, UID granuleId) {
	state Transaction tr(self->db);
	state KeyRange splitRange = blobGranuleSplitKeyRangeFor(granuleId);
	state KeyRange checkRange = splitRange;
	state bool retry = false;

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Fully delete granule split check {1}\n", self->epoch, granuleId.toString());
	}

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			int lim = SERVER_KNOBS->BG_MAX_SPLIT_FANOUT;
			if (BUGGIFY_WITH_PROB(0.1)) {
				lim = deterministicRandom()->randomInt(1, std::max(2, SERVER_KNOBS->BG_MAX_SPLIT_FANOUT));
			}
			state RangeReadResult splitState = wait(tr.getRange(checkRange, lim));
			// if first try and empty, splitting state is fully cleaned up
			if (!retry && checkRange == splitRange && splitState.empty() && !splitState.more) {
				if (BM_PURGE_DEBUG) {
					fmt::print("BM {0} Proceed with full deletion, no split state for {1}\n",
					           self->epoch,
					           granuleId.toString());
				}
				return true;
			}
			if (BM_PURGE_DEBUG) {
				fmt::print("BM {0} Full delete check found {1} split states for {2}\n",
				           self->epoch,
				           splitState.size(),
				           granuleId.toString());
			}
			state int i = 0;

			for (; i < splitState.size(); i++) {
				UID parent, child;
				BlobGranuleSplitState st;
				Version v;
				std::tie(parent, child) = decodeBlobGranuleSplitKey(splitState[i].key);
				std::tie(st, v) = decodeBlobGranuleSplitValue(splitState[i].value);
				// if split state is done, this granule has definitely persisted a snapshot
				if (st >= BlobGranuleSplitState::Done) {
					continue;
				}
				// if split state isn't even assigned, this granule has definitely not persisted a snapshot
				if (st <= BlobGranuleSplitState::Initialized) {
					retry = true;
					break;
				}

				ASSERT(st == BlobGranuleSplitState::Assigned);
				// if assigned, granule may or may not have snapshotted. Check files to confirm. Since a re-snapshot is
				// the first file written for a new granule, any files present mean it has re-snapshotted from this
				// granule
				KeyRange granuleFileRange = blobGranuleFileKeyRangeFor(child);
				RangeReadResult files = wait(tr.getRange(granuleFileRange, 1));
				if (files.empty()) {
					retry = true;
					break;
				}
			}
			if (retry) {
				tr.reset();
				wait(delay(1.0));
				retry = false;
				checkRange = splitRange;
			} else {
				if (splitState.empty() || !splitState.more) {
					break;
				}
				checkRange = KeyRangeRef(keyAfter(splitState.back().key), checkRange.end);
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Full delete check {1} done. Not deleting history key\n", self->epoch, granuleId.toString());
	}
	return false;
}

ACTOR Future<Void> canDeleteFullGranuleMerge(Reference<BlobManagerData> self, Optional<UID> mergeChildId) {
	// if this granule is the parent of a merged granule, it needs to re-snapshot the merged granule before we can
	// delete this one
	if (!mergeChildId.present()) {
		return Void();
	}
	CODE_PROBE(true, "checking canDeleteFullGranuleMerge");

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Fully delete granule merge check {1}\n", self->epoch, mergeChildId.get().toString());
	}

	state Transaction tr(self->db);
	state KeyRange granuleFileRange = blobGranuleFileKeyRangeFor(mergeChildId.get());
	// loop until granule has snapshotted
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeReadResult files = wait(tr.getRange(granuleFileRange, 1));
			if (!files.empty()) {
				if (BM_PURGE_DEBUG) {
					fmt::print("BM {0} Fully delete granule merge check {1} done\n",
					           self->epoch,
					           mergeChildId.get().toString());
				}
				return Void();
			}
			wait(delay(1.0));
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<bool> canDeleteFullGranule(Reference<BlobManagerData> self, UID granuleId, Optional<UID> mergeChildId) {
	state Future<bool> split = canDeleteFullGranuleSplit(self, granuleId);
	state Future<Void> merge = canDeleteFullGranuleMerge(self, mergeChildId);

	wait(success(split) && merge);
	bool canDeleteHistory = wait(split);
	return canDeleteHistory;
}

static Future<Void> deleteFile(Reference<BlobConnectionProvider> bstoreProvider, std::string filePath) {
	Reference<BackupContainerFileSystem> bstore = bstoreProvider->getForAccess(filePath);
	return bstore->deleteFile(filePath);
}

ACTOR Future<Reference<BlobConnectionProvider>> getBStoreForGranule(Reference<BlobManagerData> self,
                                                                    KeyRange granuleRange) {
	if (self->bstore.isValid()) {
		return self->bstore;
	}
	loop {
		state Reference<GranuleTenantData> data;
		state int retryCount = 0;
		wait(store(data, self->tenantData.getDataForGranule(granuleRange)));
		if (data.isValid()) {
			wait(data->bstoreLoaded.getFuture());
			wait(delay(0));
			return data->bstore;
		} else {
			// race on startup between loading tenant ranges and bgcc/purging. just wait
			retryCount++;
			TraceEvent(retryCount <= 10 ? SevDebug : SevWarn, "BlobManagerUnknownTenantForGranule", self->id)
			    .suppressFor(5.0)
			    .detail("Epoch", self->epoch)
			    .detail("KeyRange", granuleRange)
			    .detail("Retries", retryCount);
			wait(delay(0.1));
		}
	}
}

/*
 * Deletes all files pertaining to the granule with id granuleId and
 * also removes the history entry for this granule from the system keyspace
 */
ACTOR Future<Void> fullyDeleteGranule(Reference<BlobManagerData> self,
                                      UID granuleId,
                                      Key historyKey,
                                      Version purgeVersion,
                                      KeyRange granuleRange,
                                      Optional<UID> mergeChildID,
                                      bool force,
                                      Future<Void> parentFuture) {
	// wait for parent to finish first to avoid ordering/orphaning issues
	wait(parentFuture);
	// yield to avoid a long callstack and to allow this to get cancelled
	wait(delay(0));

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Fully deleting granule [{1} - {2}): {3} @ {4}{5}\n",
		           self->epoch,
		           granuleRange.begin.printable(),
		           granuleRange.end.printable(),
		           granuleId.toString(),
		           purgeVersion,
		           force ? " (force)" : "");
	}

	// if granule is still splitting and files are needed for new sub-granules to re-snapshot, we can only partially
	// delete the granule, since we need to keep the last snapshot and deltas for splitting
	// Or, if the granule isn't finalized (still needs the history entry for the old change feed id, because all data
	// from the old change feed hasn't yet been persisted in blob), we can delete the files but need to keep the granule
	// history entry.
	state bool canDeleteHistoryKey;
	if (force) {
		canDeleteHistoryKey = true;
	} else {
		wait(store(canDeleteHistoryKey, canDeleteFullGranule(self, granuleId, mergeChildID)));
	}
	state Reference<BlobConnectionProvider> bstore = wait(getBStoreForGranule(self, granuleRange));

	// get files
	GranuleFiles files = wait(loadHistoryFiles(self->db, granuleId));

	std::vector<Future<Void>> deletions;
	state std::vector<std::string> filesToDelete; // TODO: remove, just for debugging

	for (auto snapshotFile : files.snapshotFiles) {
		std::string fname = snapshotFile.filename;
		deletions.push_back(deleteFile(bstore, fname));
		filesToDelete.emplace_back(fname);
	}

	for (auto deltaFile : files.deltaFiles) {
		std::string fname = deltaFile.filename;
		deletions.push_back(deleteFile(bstore, fname));
		filesToDelete.emplace_back(fname);
	}

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Fully deleting granule {1}: deleting {2} files\n",
		           self->epoch,
		           granuleId.toString(),
		           filesToDelete.size());
		/*for (auto filename : filesToDelete) {
		    fmt::print(" - {}\n", filename.c_str());
		}*/
	}

	// delete the files before the corresponding metadata.
	// this could lead to dangling pointers in fdb, but this granule should
	// never be read again anyways, and we can clean up the keys the next time around.
	// deleting files before corresponding metadata reduces the # of orphaned files.
	wait(waitForAll(deletions));

	if (BUGGIFY && self->maybeInjectTargetedRestart()) {
		wait(delay(0)); // should be cancelled
		ASSERT(false);
	}

	// delete metadata in FDB (history entry and file keys)
	if (BM_PURGE_DEBUG) {
		fmt::print(
		    "BM {0} Fully deleting granule {1}: deleting history and file keys\n", self->epoch, granuleId.toString());
	}

	state Transaction tr(self->db);

	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			KeyRange fileRangeKey = blobGranuleFileKeyRangeFor(granuleId);
			if (canDeleteHistoryKey) {
				tr.clear(historyKey);
			}
			tr.clear(fileRangeKey);
			if (force) {
				// check manager lock to not delete metadata out from under a later recovering manager
				wait(checkManagerLock(&tr, self));
				wait(updateChangeFeed(
				    &tr, granuleIDToCFKey(granuleId), ChangeFeedStatus::CHANGE_FEED_DESTROY, granuleRange));
				tr.clear(blobGranuleLockKeyFor(granuleRange));
				tr.clear(blobGranuleSplitKeyRangeFor(granuleId));
				tr.clear(blobGranuleMergeKeyFor(granuleId));
				// FIXME: also clear merge boundaries!
			}
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	if (BUGGIFY && self->maybeInjectTargetedRestart()) {
		wait(delay(0)); // should be cancelled
		ASSERT(false);
	}

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Fully deleting granule {1}: success {2}\n",
		           self->epoch,
		           granuleId.toString(),
		           canDeleteHistoryKey ? "" : " ignoring history key!");
	}

	TraceEvent(SevDebug, "GranuleFullPurge", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("GranuleID", granuleId)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("FilesPurged", filesToDelete.size());

	++self->stats.granulesFullyPurged;
	self->stats.filesPurged += filesToDelete.size();

	CODE_PROBE(true, "full granule purged");

	return Void();
}

/*
 * For the granule with id granuleId, finds the first snapshot file at a
 * version <= purgeVersion and deletes all files older than it.
 *
 * Assumption: this granule's startVersion might change because the first snapshot
 * file might be deleted. We will need to ensure we don't rely on the granule's startVersion
 * (that's persisted as part of the key), but rather use the granule's first snapshot's version when needed
 */
ACTOR Future<Void> partiallyDeleteGranule(Reference<BlobManagerData> self,
                                          UID granuleId,
                                          Version purgeVersion,
                                          KeyRange granuleRange) {
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Partially deleting granule {1}: init\n", self->epoch, granuleId.toString());
	}

	state Reference<BlobConnectionProvider> bstore = wait(getBStoreForGranule(self, granuleRange));

	// get files
	GranuleFiles files = wait(loadHistoryFiles(self->db, granuleId));

	// represents the version of the latest snapshot file in this granule with G.version < purgeVersion
	Version latestSnapshotVersion = invalidVersion;

	state std::vector<Future<Void>> deletions; // deletion work per file
	state std::vector<Key> deletedFileKeys; // keys for deleted files
	state std::vector<std::string> filesToDelete; // TODO: remove evenutally, just for debugging

	// TODO: binary search these snapshot files for latestSnapshotVersion
	for (int idx = files.snapshotFiles.size() - 1; idx >= 0; --idx) {
		// if we already found the latestSnapshotVersion, this snapshot can be deleted
		if (latestSnapshotVersion != invalidVersion) {
			std::string fname = files.snapshotFiles[idx].filename;
			deletions.push_back(deleteFile(bstore, fname));
			deletedFileKeys.emplace_back(blobGranuleFileKeyFor(granuleId, files.snapshotFiles[idx].version, 'S'));
			filesToDelete.emplace_back(fname);
		} else if (files.snapshotFiles[idx].version <= purgeVersion) {
			// otherwise if this is the FIRST snapshot file with version < purgeVersion,
			// then we found our latestSnapshotVersion (FIRST since we are traversing in reverse)
			latestSnapshotVersion = files.snapshotFiles[idx].version;
		}
	}

	if (latestSnapshotVersion == invalidVersion) {
		return Void();
	}

	// delete all delta files older than latestSnapshotVersion
	for (auto deltaFile : files.deltaFiles) {
		// traversing in fwd direction, so stop once we find the first delta file past the latestSnapshotVersion
		if (deltaFile.version > latestSnapshotVersion) {
			break;
		}

		// otherwise deltaFile.version <= latestSnapshotVersion so delete it
		// == should also be deleted because the last delta file before a snapshot would have the same version
		std::string fname = deltaFile.filename;
		deletions.push_back(deleteFile(bstore, fname));
		deletedFileKeys.emplace_back(blobGranuleFileKeyFor(granuleId, deltaFile.version, 'D'));
		filesToDelete.emplace_back(fname);
	}

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Partially deleting granule {1}: deleting {2} files\n",
		           self->epoch,
		           granuleId.toString(),
		           filesToDelete.size());
		/*for (auto filename : filesToDelete) {
		    fmt::print(" - {0}\n", filename);
		}*/
	}

	// TODO: the following comment relies on the assumption that BWs will not get requests to
	// read data that was already purged. confirm assumption is fine. otherwise, we'd need
	// to communicate with BWs here and have them ack the purgeVersion

	// delete the files before the corresponding metadata.
	// this could lead to dangling pointers in fdb, but we should never read data older than
	// purgeVersion anyways, and we can clean up the keys the next time around.
	// deleting files before corresponding metadata reduces the # of orphaned files.
	wait(waitForAll(deletions));

	// delete metadata in FDB (deleted file keys)
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Partially deleting granule {1}: deleting file keys\n", self->epoch, granuleId.toString());
	}

	state Transaction tr(self->db);

	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			for (auto& key : deletedFileKeys) {
				tr.clear(key);
			}
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Partially deleting granule {1}: success\n", self->epoch, granuleId.toString());
	}
	TraceEvent(SevDebug, "GranulePartialPurge", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("GranuleID", granuleId)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("FilesPurged", filesToDelete.size());

	++self->stats.granulesPartiallyPurged;
	self->stats.filesPurged += filesToDelete.size();

	CODE_PROBE(true, " partial granule purged");

	return Void();
}

ACTOR Future<Void> waitForcePurgeBlobbified(Reference<BlobManagerData> self, KeyRangeRef range) {
	// To avoid races with a range still being blobbified, wait to initiate force purge until blobbification is complete
	// Also needs to be idempotent with previous purges though, so if range is already purging, skip this check
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} waitForcePurgeBlobbified [{1} - {2}): start\n",
		           self->epoch,
		           range.begin.printable(),
		           range.end.printable());
	}
	state Transaction tr(self->db);
	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);

		try {
			state Future<Optional<Version>> verifyFuture =
			    timeout(self->db->verifyBlobRange(range, latestVersion), 10.0);
			ForcedPurgeState purgeState = wait(getForcePurgedState(&tr, range));
			if (purgeState != ForcedPurgeState::NonePurged) {
				// FIXME: likely does not solve the issue if SomePurged but not all, since some ranges might not be
				// blobbified example would be if first [C - D) was purged, then later [A - F) was.
				if (BM_PURGE_DEBUG) {
					fmt::print("BM {0} waitForcePurgeComplete [{1} - {2}): already purged\n",
					           self->epoch,
					           range.begin.printable(),
					           range.end.printable());
				}
				break;
			}

			Optional<Version> verifyVersion = wait(verifyFuture);
			if (verifyVersion.present() && verifyVersion.get() != invalidVersion) {
				if (BM_PURGE_DEBUG) {
					fmt::print("BM {0} waitForcePurgeComplete [{1} - {2}): verified blobbified\n",
					           self->epoch,
					           range.begin.printable(),
					           range.end.printable());
				}
				break;
			}
			tr.reset();
			wait(delay(1.0));
			// TODO: remove key range stuck!
			TraceEvent("WaitForcePurgeBlobbifiedBlocking", self->id)
			    .suppressFor(60.0)
			    .detail("Epoch", self->epoch)
			    .detail("Range", range);
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
}

ACTOR Future<Void> deleteActiveChangeFeeds(Reference<BlobManagerData> bmData, PromiseStream<Key>* feedsToDelete) {
	state Transaction tr(bmData->db);
	state int batchSize = 100;
	if (BUGGIFY_WITH_PROB(0.1)) {
		batchSize = deterministicRandom()->randomInt(1, 10);
	}

	state bool done = false;

	loop {
		if (done) {
			break;
		}
		state std::vector<Key> feedBatch;
		feedBatch.reserve(batchSize);
		loop {
			try {
				Key k = waitNext(feedsToDelete->getFuture());
				feedBatch.push_back(k);
				if (feedBatch.size() == batchSize) {
					break;
				}
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream) {
					done = true;
					break;
				}
				throw e;
			}
		}

		if (done && feedBatch.empty()) {
			break;
		}

		tr.reset();
		loop {
			try {
				state std::vector<Future<Void>> deleteFutures;
				deleteFutures.reserve(feedBatch.size());
				for (int i = 0; i < feedBatch.size(); i++) {
					deleteFutures.push_back(updateChangeFeed(&tr,
					                                         feedBatch[i],
					                                         ChangeFeedStatus::CHANGE_FEED_DESTROY,
					                                         normalKeys)); // range doesn't matter for delete
				}
				wait(waitForAll(deleteFutures));
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
	return Void();
}

ACTOR Future<Void> revokeRangeFromAll(Reference<BlobManagerData> self,
                                      KeyRangeRef range,
                                      std::set<Key>* knownBoundariesPurged,
                                      UID purgeID,
                                      Version purgeVersion) {
	state std::vector<Future<Void>> allPurges;
	// revoke range from all active blob workers, both targeted at the current owner each BM knows for the granule,
	// and just blanket revoke the whole range from all known workers
	// if purge covers multiple blobbified ranges, revoke each separately
	auto knownRanges = self->knownBlobRanges.intersectingRanges(range);
	for (auto& it : knownRanges) {
		if (it.cvalue()) {
			RangeAssignment ra;
			ra.isAssign = false;
			ra.keyRange = range & it.range();
			ra.revoke = RangeRevokeData(true); // dispose=true
			if (ra.keyRange.begin > range.begin) {
				knownBoundariesPurged->insert(ra.keyRange.begin);
			}
			if (ra.keyRange.end < range.end) {
				knownBoundariesPurged->insert(ra.keyRange.end);
			}
			allPurges.push_back(handleRangeAssign(self, ra));
		}
	}
	for (auto& it : self->workersById) {
		RangeAssignment ra;
		ra.isAssign = false;
		ra.keyRange = range;
		ra.worker = it.first;
		ra.revoke = RangeRevokeData(true); // dispose=true
		allPurges.push_back(handleRangeAssign(self, ra));
	}

	wait(waitForAll(allPurges));
	return Void();
}

/*
 * This method is used to purge the range [startKey, endKey) at (and including) purgeVersion.
 * To do this, we do a BFS traversal starting at the active granules. Then we classify granules
 * in the history as nodes that can be fully deleted (i.e. their files and history can be deleted)
 * and nodes that can be partially deleted (i.e. some of their files can be deleted).
 * Once all this is done, we finally clear the purgeIntent key, if possible, to indicate we are done
 * processing this purge intent.
 */
ACTOR Future<Void> purgeRange(Reference<BlobManagerData> self,
                              KeyRangeRef range,
                              Version purgeVersion,
                              bool force,
                              UID purgeID) {
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} purgeRange starting for range [{1} - {2}) @ purgeVersion={3}, force={4}\n",
		           self->epoch,
		           range.begin.printable(),
		           range.end.printable(),
		           purgeVersion,
		           force);
	}

	state ActiveCounter<int>::Releaser holdingPurgesCounter = self->stats.activePurges.take(1);

	TraceEvent("PurgeGranulesBegin", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("Range", range)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("Force", force)
	    .detail("PurgeID", purgeID);

	// queue of <range, startVersion, endVersion, mergeChildID> for BFS traversal of history
	state std::queue<std::tuple<KeyRange, Version, Version, Optional<UID>>> historyEntryQueue;

	// stacks of <granuleId, historyKey> and <granuleId> (and mergeChildID) to track which granules to delete
	state std::vector<std::tuple<UID, Key, KeyRange, Optional<UID>, Version>> toFullyDelete;
	state std::vector<std::pair<UID, KeyRange>> toPartiallyDelete;

	// track which granules we have already added to traversal
	// note: (startKey, startVersion) uniquely identifies a granule
	state std::unordered_set<std::pair<std::string, Version>, boost::hash<std::pair<std::string, Version>>> visited;

	// find all active granules (that comprise the range) and add to the queue

	state Transaction tr(self->db);

	// FIXME: this should be completely separate by known blob range, to avoid any races or issues
	// if range isn't in known blob ranges, do nothing after writing force purge range to database
	state std::vector<KeyRange> knownPurgeRanges;
	auto knownRanges = self->knownBlobRanges.intersectingRanges(range);
	for (auto& it : knownRanges) {
		if (it.cvalue()) {
			knownPurgeRanges.push_back(range & it.range());
		}
	}

	if (force) {
		// before setting force purge in the database, make sure all ranges to purge are actually blobbified to avoid
		// races leaving orphaned files/change feeds
		// FIXME: this can also apply to non-force-purges but for retention purges it's not as bad since it'll likely
		// get another one soon
		std::vector<Future<Void>> waitForBlobbifies;
		waitForBlobbifies.reserve(knownPurgeRanges.size());
		for (auto& it : knownPurgeRanges) {
			waitForBlobbifies.push_back(waitForcePurgeBlobbified(self, it));
		}
		TraceEvent(SevDebug, "PurgeGranulesWaitForBlobbifiesStart", self->id)
		    .detail("Epoch", self->epoch)
		    .detail("PurgeVersion", purgeVersion)
		    .detail("Count", waitForBlobbifies.size())
		    .detail("PurgeID", purgeID);

		choose {
			when(wait(waitForAll(waitForBlobbifies))) {
				TraceEvent(SevDebug, "PurgeGranulesWaitForBlobbifiesDone", self->id)
				    .detail("Epoch", self->epoch)
				    .detail("PurgeVersion", purgeVersion)
				    .detail("PurgeID", purgeID);
			}
			when(wait(delay(SERVER_KNOBS->FORCE_PURGE_WAIT_BLOBBIFY_TIMEOUT))) {
				TraceEvent(SevWarn, "PurgeGranulesWaitForBlobbifiesTimedOut", self->id)
				    .detail("Epoch", self->epoch)
				    .detail("PurgeVersion", purgeVersion)
				    .detail("PurgeID", purgeID);
			}
		}

		// TODO could clean this up after force purge is done, but it's safer not to
		self->forcePurgingRanges.insert(range, true);
		// set force purged range, to prevent future operations on this range
		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				// set force purged range, but don't clear mapping range yet, so that if a new BM recovers in the middle
				// of purging, it still knows what granules to purge
				wait(checkManagerLock(&tr, self));
				// FIXME: need to handle this better if range is unaligned. Need to not truncate existing granules, and
				// instead cover whole of intersecting granules at begin/end
				wait(krmSetRangeCoalescing(&tr, blobGranuleForcePurgedKeys.begin, range, normalKeys, "1"_sr));
				wait(tr.commit());

				if (BUGGIFY && self->maybeInjectTargetedRestart()) {
					wait(delay(0)); // should be cancelled
					ASSERT(false);
				}

				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		tr.reset();
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

		TraceEvent(SevDebug, "PurgeGranulesSetForcePurged", self->id)
		    .detail("Epoch", self->epoch)
		    .detail("PurgeVersion", purgeVersion)
		    .detail("PurgeID", purgeID);
	}

	if (knownPurgeRanges.empty()) {
		CODE_PROBE(true, "skipping purge because not in known blob ranges");
		TraceEvent("PurgeGranulesSkippingUnknownRange", self->id)
		    .detail("Epoch", self->epoch)
		    .detail("Range", range)
		    .detail("PurgeVersion", purgeVersion)
		    .detail("Force", force)
		    .detail("PurgeID", purgeID);
		return Void();
	}

	// wait for all active splits and merges in the range to come to a stop, so no races with purging
	std::vector<Future<Void>> activeBoundaryEvals;
	auto boundaries = self->boundaryEvaluations.intersectingRanges(range);
	for (auto& it : boundaries) {
		auto& f = it.cvalue().inProgress;
		if (f.isValid() && !f.isReady() && !f.isError()) {
			activeBoundaryEvals.push_back(f);
		}
	}

	TraceEvent(SevDebug, "PurgeGranulesWaitActiveBoundaryEvals", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("Count", activeBoundaryEvals.size())
	    .detail("PurgeID", purgeID);

	if (!activeBoundaryEvals.empty()) {
		wait(waitForAll(activeBoundaryEvals));
	}

	TraceEvent(SevDebug, "PurgeGranulesWaitMergeActive", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("PurgeID", purgeID);

	// some merges aren't counted in boundary evals, for merge/split race reasons
	while (self->isMergeActive(range)) {
		wait(delayJittered(1.0));
	}

	auto ranges = self->workerAssignments.intersectingRanges(range);
	state std::vector<KeyRange> activeRanges;

	// copy into state variable before waits
	for (auto& it : ranges) {
		activeRanges.push_back(it.range());
	}

	state std::set<Key> knownBoundariesPurged;

	if (force) {
		//  Do AFTER we copy set of active ranges to purge
		wait(revokeRangeFromAll(self, range, &knownBoundariesPurged, purgeID, purgeVersion));
	}

	TraceEvent(SevDebug, "PurgeGranulesTraverseBegin", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("PurgeID", purgeID);

	// nuke all active change feeds early on in the process to remove the largest source of impact on the rest of the
	// system as quickly as we can
	state Future<Void> activeFeedDeleter = Future<Void>(Void());
	state PromiseStream<Key> forcePurgeFeedKeysToDelete;

	if (force) {
		activeFeedDeleter = deleteActiveChangeFeeds(self, &forcePurgeFeedKeysToDelete);
	}

	state int rangeIdx;
	for (rangeIdx = 0; rangeIdx < activeRanges.size(); rangeIdx++) {
		state KeyRange activeRange = activeRanges[rangeIdx];
		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0} Checking if active range [{1} - {2}) should be purged\n",
			           self->epoch,
			           activeRange.begin.printable(),
			           activeRange.end.printable());
		}

		// assumption: purge boundaries must respect granule boundaries
		if (activeRange.begin < range.begin || activeRange.end > range.end) {
			TraceEvent(SevWarn, "GranulePurgeRangesUnaligned", self->id)
			    .detail("Epoch", self->epoch)
			    .detail("PurgeRange", range)
			    .detail("GranuleRange", activeRange)
			    .detail("PurgeID", purgeID);
			continue;
		}

		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				if (BM_PURGE_DEBUG) {
					fmt::print("BM {0} Fetching latest history entry for range [{1} - {2})\n",
					           self->epoch,
					           activeRange.begin.printable(),
					           activeRange.end.printable());
				}
				// FIXME: doing this serially will likely be too slow for large purges
				Optional<GranuleHistory> history = wait(getLatestGranuleHistory(&tr, activeRange));
				++self->stats.purgeLatestReads;
				// TODO: can we tell from the krm that this range is not valid, so that we don't need to do a
				// get
				if (history.present()) {
					if (BM_PURGE_DEBUG) {
						fmt::print("BM {0}   Adding range to history queue: [{1} - {2}) @ {3}\n",
						           self->epoch,
						           activeRange.begin.printable(),
						           activeRange.end.printable(),
						           history.get().version);
					}
					visited.insert({ activeRange.begin.toString(), history.get().version });
					historyEntryQueue.push({ activeRange, history.get().version, MAX_VERSION, {} });
					if (force) {
						forcePurgeFeedKeysToDelete.send(granuleIDToCFKey(history->value.granuleID));
					}
				} else if (BM_PURGE_DEBUG) {
					fmt::print("BM {0}   No history for range, ignoring\n", self->epoch);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// no longer needed, can save memory
	activeRanges.clear();

	if (force) {
		forcePurgeFeedKeysToDelete.sendError(end_of_stream());
		wait(activeFeedDeleter);
	}

	TraceEvent("PurgeGranulesTraversalGotLatest", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("Range", range)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("Force", force)
	    .detail("Count", historyEntryQueue.size())
	    .detail("PurgeID", purgeID);

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Beginning BFS traversal of {1} history items for range [{2} - {3}) \n",
		           self->epoch,
		           historyEntryQueue.size(),
		           range.begin.printable(),
		           range.end.printable());
	}
	while (!historyEntryQueue.empty()) {
		// process the node at the front of the queue and remove it
		state KeyRange currRange;
		state Version startVersion;
		state Version endVersion;
		state Optional<UID> mergeChildID;
		std::tie(currRange, startVersion, endVersion, mergeChildID) = historyEntryQueue.front();
		historyEntryQueue.pop();

		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0} Processing history node [{1} - {2}) with versions [{3}, {4})\n",
			           self->epoch,
			           currRange.begin.printable(),
			           currRange.end.printable(),
			           startVersion,
			           endVersion);
		}

		// get the persisted history entry for this granule
		state Standalone<BlobGranuleHistoryValue> currHistoryNode;
		state Key historyKey = blobGranuleHistoryKeyFor(currRange, startVersion);
		state bool foundHistory = false;
		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				ValueReadResult persistedHistory = wait(tr.get(historyKey));
				++self->stats.purgeDAGReads;
				if (persistedHistory.present()) {
					currHistoryNode = decodeBlobGranuleHistoryValue(persistedHistory.get());
					foundHistory = true;
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		if (!foundHistory) {
			if (BM_PURGE_DEBUG) {
				fmt::print("BM {0}  No history for this node, skipping\n", self->epoch);
			}
			continue;
		}

		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0}  Found history entry for this node. It's granuleID is {1}\n",
			           self->epoch,
			           currHistoryNode.granuleID.toString());
		}

		// There are three cases this granule can fall into:
		// - if the granule's end version is at or before the purge version or this is a force delete,
		//   this granule should be completely deleted
		// - else if the startVersion <= purgeVersion, then G.startVersion < purgeVersion < G.endVersion
		//   and so this granule should be partially deleted
		// - otherwise, this granule is active, so don't schedule it for deletion
		if (force || endVersion <= purgeVersion) {
			if (BM_PURGE_DEBUG) {
				fmt::print(
				    "BM {0}   Granule {1} will be FULLY deleted\n", self->epoch, currHistoryNode.granuleID.toString());
			}
			toFullyDelete.push_back({ currHistoryNode.granuleID, historyKey, currRange, mergeChildID, startVersion });
		} else if (startVersion < purgeVersion) {
			if (BM_PURGE_DEBUG) {
				fmt::print("BM {0}   Granule {1} will be partially deleted\n",
				           self->epoch,
				           currHistoryNode.granuleID.toString());
			}
			toPartiallyDelete.push_back({ currHistoryNode.granuleID, currRange });
		}

		// add all of the node's parents to the queue
		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0}   Checking {1} parents\n", self->epoch, currHistoryNode.parentVersions.size());
		}
		Optional<UID> mergeChildID2 =
		    currHistoryNode.parentVersions.size() > 1 ? currHistoryNode.granuleID : Optional<UID>();
		for (int i = 0; i < currHistoryNode.parentVersions.size(); i++) {
			// for (auto& parent : currHistoryNode.parentVersions.size()) {
			// if we already added this node to queue, skip it; otherwise, mark it as visited
			KeyRangeRef parentRange(currHistoryNode.parentBoundaries[i], currHistoryNode.parentBoundaries[i + 1]);
			Version parentVersion = currHistoryNode.parentVersions[i];
			std::string beginStr = parentRange.begin.toString();
			if (!visited.insert({ beginStr, parentVersion }).second) {
				if (BM_PURGE_DEBUG) {
					fmt::print("BM {0}     Already added [{1} - {2}) @ {3} - {4} to queue, so skipping it\n",
					           self->epoch,
					           parentRange.begin.printable(),
					           parentRange.end.printable(),
					           parentVersion,
					           startVersion);
				}
				continue;
			}

			if (BM_PURGE_DEBUG) {
				fmt::print("BM {0}     Adding parent [{1} - {2}) @ {3} - {4} to queue\n",
				           self->epoch,
				           parentRange.begin.printable(),
				           parentRange.end.printable(),
				           parentVersion,
				           startVersion);
			}

			// the parent's end version is this node's startVersion,
			// since this node must have started where it's parent finished
			historyEntryQueue.push({ parentRange, parentVersion, startVersion, mergeChildID2 });
		}
	}

	// The top of the stacks have the oldest ranges. This implies that for a granule located at
	// index i, it's parent must be located at some index j, where j > i. For this reason,
	// we delete granules in reverse order; this way, we will never end up with unreachable
	// nodes in the persisted history. Moreover, for any node that must be fully deleted,
	// any node that must be partially deleted must occur later on in the history. Thus,
	// we delete the 'toFullyDelete' granules first.
	//
	// Unfortunately we can't do parallelize _full_ deletions because they might
	// race and we'll end up with unreachable nodes in the case of a crash.
	// Since partial deletions only occur for "leafs", they can be done in parallel
	//
	// Note about file deletions: although we might be retrying a deletion of a granule,
	// we won't run into any issues with trying to "re-delete" a blob file since deleting
	// a file that doesn't exist is considered successful

	TraceEvent("PurgeGranulesTraversalComplete", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("Range", range)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("Force", force)
	    .detail("VisitedCount", visited.size())
	    .detail("DeletingFullyCount", toFullyDelete.size())
	    .detail("DeletingPartiallyCount", toPartiallyDelete.size())
	    .detail("PurgeID", purgeID);

	state int i;
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0}: {1} granules to fully delete\n", self->epoch, toFullyDelete.size());
	}
	// Go backwards through set of granules to guarantee deleting oldest first. This avoids orphaning granules in the
	// deletion process
	if (!toFullyDelete.empty()) {
		state std::deque<Future<Void>> fullDeletions;
		state KeyRangeMap<std::pair<Version, Future<Void>>> parentDelete;
		parentDelete.insert(normalKeys, { 0, Future<Void>(Void()) });

		// ordered set to order by version + index
		// do this way instead of std::sort on a vector to be able to yield during it
		state std::set<std::pair<Version, int>> deleteOrderSet;
		for (i = 0; i < toFullyDelete.size(); i++) {
			deleteOrderSet.insert({ std::get<4>(toFullyDelete[i]), i });
			if (i % 1000 == 999) {
				wait(yield());
			}
		}
		state std::set<std::pair<Version, int>>::iterator it = deleteOrderSet.begin();

		state Version previousVersion = 0;
		for (i = 0; i < deleteOrderSet.size(); i++) {
			if (fullDeletions.size() == SERVER_KNOBS->BLOB_MANAGER_PURGE_GRANULE_PARALLELISM) {
				wait(fullDeletions.front());
				fullDeletions.pop_front();
			}
			UID granuleId;
			Key historyKey;
			KeyRange keyRange;
			Optional<UID> mergeChildId;
			Version startVersion;
			std::tie(granuleId, historyKey, keyRange, mergeChildId, startVersion) = toFullyDelete[it->second];
			ASSERT(previousVersion <= startVersion);
			previousVersion = startVersion;
			// FIXME: consider batching into a single txn (need to take care of txn size limit)
			if (BM_PURGE_DEBUG) {
				fmt::print("BM {0}: About to fully delete granule {1}\n", self->epoch, granuleId.toString());
			}
			std::vector<Future<Void>> parents;
			auto parentRanges = parentDelete.intersectingRanges(keyRange);
			for (auto& it : parentRanges) {
				if (startVersion <= it.cvalue().first) {
					fmt::print("ERROR: [{0} - {1}) @ {2} <= [{3} - {4}) @ {5}\n",
					           keyRange.begin.printable(),
					           keyRange.end.printable(),
					           startVersion,
					           it.begin().printable(),
					           it.end().printable(),
					           it.cvalue().first);
				}
				ASSERT(startVersion > it.cvalue().first);
				parents.push_back(it.cvalue().second);
			}
			Future<Void> deleteFuture = fullyDeleteGranule(
			    self, granuleId, historyKey, purgeVersion, keyRange, mergeChildId, force, waitForAll(parents));
			fullDeletions.push_back(deleteFuture);
			parentDelete.insert(keyRange, { startVersion, deleteFuture });
			++it;
		}

		// no waitForAll(deque)
		for (i = 0; i < fullDeletions.size(); i++) {
			wait(fullDeletions[i]);
		}
		fullDeletions.clear();
		parentDelete.insert(normalKeys, { 0, Future<Void>(Void()) });
	}

	if (BUGGIFY && self->maybeInjectTargetedRestart()) {
		wait(delay(0)); // should be cancelled
		ASSERT(false);
	}

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0}: {1} granules to partially delete\n", self->epoch, toPartiallyDelete.size());
	}

	state std::deque<Future<Void>> partialDeletions;
	for (i = toPartiallyDelete.size() - 1; i >= 0; --i) {
		if (partialDeletions.size() == SERVER_KNOBS->BLOB_MANAGER_PURGE_GRANULE_PARALLELISM) {
			wait(partialDeletions.front());
			partialDeletions.pop_front();
		}
		UID granuleId;
		KeyRange keyRange;
		std::tie(granuleId, keyRange) = toPartiallyDelete[i];
		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0}: About to partially delete granule {1}\n", self->epoch, granuleId.toString());
		}
		partialDeletions.emplace_back(partiallyDeleteGranule(self, granuleId, purgeVersion, keyRange));
	}

	// no waitForAll(deque)
	for (i = 0; i < partialDeletions.size(); i++) {
		wait(partialDeletions[i]);
	}
	partialDeletions.clear();

	if (BUGGIFY && self->maybeInjectTargetedRestart()) {
		wait(delay(0)); // should be cancelled
		ASSERT(false);
	}

	if (force) {
		tr.reset();
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		loop {
			try {
				// clear mapping range, so that a new BM doesn't try to recover force purged granules, and clients can't
				// read them
				wait(checkManagerLock(&tr, self));
				wait(krmSetRange(&tr, blobGranuleMappingKeys.begin, range, blobGranuleMappingValueFor(UID())));
				// FIXME: there is probably a cleaner fix than setting extra keys in the database if someone does a
				// purge that's not aligned to boundaries
				for (auto& it : knownBoundariesPurged) {
					// keep original bounds in granule mapping as to not confuse future managers on recovery
					tr.set(it.withPrefix(blobGranuleMappingKeys.begin), blobGranuleMappingValueFor(UID()));
				}
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	if (BUGGIFY && self->maybeInjectTargetedRestart()) {
		wait(delay(0)); // should be cancelled
		ASSERT(false);
	}

	// Now that all the necessary granules and their files have been deleted, we can
	// clear the purgeIntent key to signify that the work is done. However, there could have been
	// another purgeIntent that got written for this table while we were processing this one.
	// If that is the case, we should not clear the key. Otherwise, we can just clear the key.

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0}: Successfully purged range [{1} - {2}) at purgeVersion={3}\n",
		           self->epoch,
		           range.begin.printable(),
		           range.end.printable(),
		           purgeVersion);
	}

	TraceEvent("PurgeGranulesComplete", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("Range", range)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("Force", force)
	    .detail("PurgeID", purgeID);

	CODE_PROBE(true, "range purge complete");

	++self->stats.purgesProcessed;
	return Void();
}

ACTOR Future<Void> cleanupPurgeKey(Reference<BlobManagerData> bmData, Key purgeKey, Future<Void> olderSubRangeFutures) {
	state ActiveCounter<int>::Releaser holdingAwaitingCleanupPurgesCounter =
	    bmData->stats.awaitingCleanupPurges.take(1);
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} waiting cleanup up purge range {1}\n", bmData->epoch, purgeKey.printable());
	}
	wait(olderSubRangeFutures);
	wait(delay(0));
	// FIXME: add some simulated validation of purge processing ordering and marking completeness here?
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} cleaning up purge range {1}\n", bmData->epoch, purgeKey.printable());
	}
	bmData->completedPurgeKeys.push_back(purgeKey);
	CODE_PROBE(true, "BM finished processing a purge intent");
	return Void();
}

ACTOR Future<Void> handlePurgeRangeDAGNode(Reference<BlobManagerData> bmData,
                                           PurgeRangeDAGNode* node,
                                           Future<Void> olderSubRangeFutures) {

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} handle purge range {1} [{2} - {3}) @ {4}{5}: {6}\n",
		           bmData->epoch,
		           node->nodeID.shortString(),
		           node->range.begin.printable(),
		           node->range.end.printable(),
		           node->purgeVersion,
		           node->force ? " (force)" : "",
		           node->purgeKey.printable());
	}
	state ActiveCounter<int>::Releaser holdingBlockedPurgesCounter = bmData->stats.blockedPurges.take(1);
	wait(olderSubRangeFutures);
	wait(delay(0));
	holdingBlockedPurgesCounter.release();

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} processing purge range {1} [{2} - {3}) @ {4}{5}\n",
		           bmData->epoch,
		           node->nodeID.shortString(),
		           node->range.begin.printable(),
		           node->range.end.printable(),
		           node->purgeVersion,
		           node->force ? " (force)" : "");
	}

	// if we were partially/entirely superceded by any ranges (as of the point of time of starting/skipping our purge),
	// wait on their futures (in addition to ours if not skipping) before deleting purge key
	state std::vector<Future<Void>> waitBeforeDeletingPurgeKey;
	state bool stillDo = false;
	auto ranges = bmData->latestActivePurges.intersectingRanges(node->range);
	for (auto& activeNode : ranges) {
		bool same = activeNode.cvalue()->nodeID == node->nodeID;
		stillDo = stillDo || same;
		if (!same) {
			waitBeforeDeletingPurgeKey.push_back(activeNode.cvalue()->purgeFuture);
		}
	}

	if (stillDo) {
		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0} purging range {1} [{2} - {3}) @ {4}{5}\n",
			           bmData->epoch,
			           node->nodeID.shortString(),
			           node->range.begin.printable(),
			           node->range.end.printable(),
			           node->purgeVersion,
			           node->force ? " (force)" : "");
		}
		try {
			wait(purgeRange(bmData, node->range, node->purgeVersion, node->force, node->nodeID));
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled || e.code() == error_code_http_request_failed ||
			    e.code() == error_code_blob_manager_replaced || e.code() == error_code_platform_error) {
				throw e;
			}
			// FIXME: refactor this into a function on BlobManagerData
			TraceEvent(SevError, "BlobManagerUnexpectedErrorPurgeRange", bmData->id)
			    .error(e)
			    .detail("Epoch", bmData->epoch)
			    .detail("Range", node->range)
			    .detail("Version", node->purgeVersion)
			    .detail("Force", node->force);
			ASSERT_WE_THINK(false);
		}
	} else {
		++bmData->stats.noopPurges;
		CODE_PROBE(true, "skipping purge as it is completely superceded by newer purges");
		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0} skipping obsolete purge range [{1} - {2}) @ {3}{4}\n",
			           bmData->epoch,
			           node->range.begin.printable(),
			           node->range.end.printable(),
			           node->purgeVersion,
			           node->force ? " (force)" : "");
		}
	}

	// we're done processing so we can unblock future purges, but only clean up the purge key when any purges that
	// superceded us in parallel are also done
	bmData->addActor.send(cleanupPurgeKey(bmData, node->purgeKey, waitForAll(waitBeforeDeletingPurgeKey)));
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} completed purge range {1} [{2} - {3}) @ {4}{5}\n",
		           bmData->epoch,
		           node->nodeID.shortString(),
		           node->range.begin.printable(),
		           node->range.end.printable(),
		           node->purgeVersion,
		           node->force ? " (force)" : "");
	}
	return Void();
}

ACTOR Future<Void> cleanupCompletedPurgeKeys(Reference<BlobManagerData> self) {
	loop {
		// do in delayed batches for efficiency
		state Future<Void> nextDelay = delayJittered(1.0);
		state int batchSize = 1000;
		if (BUGGIFY_WITH_PROB(0.01)) {
			batchSize = deterministicRandom()->randomInt(1, 10);
		}

		if (self->completedPurgeKeys.empty()) {
			wait(nextDelay);
			continue;
		}

		state std::vector<Key> purgeKeysToDelete;
		purgeKeysToDelete.reserve(std::min<size_t>(batchSize, self->completedPurgeKeys.size()));
		while (purgeKeysToDelete.size() < batchSize && !self->completedPurgeKeys.empty()) {
			purgeKeysToDelete.push_back(self->completedPurgeKeys.front());
			if (BM_PURGE_DEBUG) {
				fmt::print(
				    "BM {0} deleting purge key {1}\n", self->epoch, self->completedPurgeKeys.front().printable());
			}
			self->completedPurgeKeys.pop_front();
		}
		CODE_PROBE(!self->completedPurgeKeys.empty(), "too many completed purge keys for 1 batch");

		state Transaction tr(self->db);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				for (auto& it : purgeKeysToDelete) {
					tr.clear(it);
				}
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		if (self->completedPurgeKeys.empty()) {
			wait(nextDelay);
		}
	}
}

/*
 * This monitor watches for new granule purge intents to appear in the purge queue.
 * Upon finding a purge intent, we will see if there is an already running purge for overlapping ranges.
 * Because the purges cannot run in parallel, we will order them by their "newness", where a purge that is a force is
 * always newer than a retention purge, and retention purges are ordered by their purge version. If a purge is at a
 * higher version, it will wait for the previous one to complete. This has several additional optimizations. If a new
 * "older" purge comes in after we are already processing a "newer" purge, it will simply be marked as complete when the
 * already-running purge finishes If we get multiple "newer" purges while an old purge is running, we will coalesce them
 * and only run the newest purge.
 */
ACTOR Future<Void> monitorPurgeKeys(Reference<BlobManagerData> self) {
	self->initBStore();

	// wait for BM to be fully recovered and have loaded hard boundaries before starting purges
	wait(self->doneRecovering.getFuture());
	wait(self->loadedClientRanges.getFuture());

	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);

		// Wait for the watch to change, or some time to expire (whichever comes first)
		// before checking through the purge intents. We write a UID into the change key value
		// so that we can still recognize when the watch key has been changed while we weren't
		// monitoring it

		state Key lastPurgeKey = blobGranulePurgeKeys.begin;

		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			try {
				wait(checkManagerLock(tr, self));
				// TODO: replace 10000 with a knob
				int batchSize = 10000;
				if (BUGGIFY_WITH_PROB(0.01)) {
					batchSize = deterministicRandom()->randomInt(1, 10);
				}
				state RangeReadResult purgeIntents =
				    wait(tr->getRange(KeyRangeRef(lastPurgeKey, blobGranulePurgeKeys.end), batchSize));
				if (purgeIntents.size()) {
					CODE_PROBE(true, "BM found purges to process");
					int rangeIdx = 0;
					for (; rangeIdx < purgeIntents.size(); ++rangeIdx) {
						Version purgeVersion;
						KeyRange range;
						bool force;
						std::tie(purgeVersion, range, force) =
						    decodeBlobGranulePurgeValue(purgeIntents[rangeIdx].value);

						if (BM_PURGE_DEBUG) {
							fmt::print("BM {0} found intent to purge range [{1} - {2}) @ {3}{4}\n",
							           self->epoch,
							           range.begin.printable(),
							           range.end.printable(),
							           purgeVersion,
							           force ? " (force)" : "");
						}

						auto ranges = self->latestActivePurges.intersectingRanges(range);
						Reference<PurgeRangeDAGNode> node =
						    makeReference<PurgeRangeDAGNode>(range, purgeIntents[rangeIdx].key, purgeVersion, force);
						std::vector<KeyRange> newerSubRanges;
						std::vector<Future<Void>> olderSubRangeFutures;
						std::vector<Future<Void>> newerSubRangeFutures;
						Future<Void> waitOnOlder = Future<Void>(Void());

						for (auto prevNode : ranges) {
							KeyRange intersection = prevNode.range() & range;
							bool iAmNewer;
							if (prevNode.cvalue()->force || force) {
								iAmNewer = force && !prevNode.cvalue()->force;
							} else {
								iAmNewer = purgeVersion > prevNode.cvalue()->purgeVersion;
							}

							if (iAmNewer) {
								newerSubRanges.push_back(intersection);
								olderSubRangeFutures.push_back(prevNode.cvalue()->purgeFuture);
								// reference from this node since now the reference is being removed from the purge map
								node->replacedParents.push_back(prevNode.cvalue());
							} else {
								newerSubRangeFutures.push_back(prevNode.cvalue()->purgeFuture);
							}
						}

						if (newerSubRanges.empty()) {
							++self->stats.noopPurges;
							// if no reference in the map because this is completely superceded by existing ranges, just
							// wait for them to complete
							self->addActor.send(
							    cleanupPurgeKey(self, node->purgeKey, waitForAll(newerSubRangeFutures)));
						} else {
							for (auto& it : newerSubRanges) {
								self->latestActivePurges.insert(it, node);
							}
							// even if this purge is already completely obsolete by other purges, we can't delete its
							// purge key until the ones that obsolete this purge finish
							node->purgeFuture =
							    handlePurgeRangeDAGNode(self, node.getPtr(), waitForAll(olderSubRangeFutures));
							self->addActor.send(node->purgeFuture);
						}
					}
					lastPurgeKey = keyAfter(purgeIntents[rangeIdx - 1].key);
				} else {
					state Future<Void> watchPurgeIntentsChange = tr->watch(blobGranulePurgeChangeKey);
					wait(tr->commit());
					wait(watchPurgeIntentsChange);
					tr->reset();
				}
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}
}

ACTOR Future<Void> doLockChecks(Reference<BlobManagerData> bmData) {
	loop {
		Promise<Void> check = bmData->doLockCheck;
		wait(check.getFuture());
		wait(delay(0.5)); // don't do this too often if a lot of conflict

		CODE_PROBE(true, "BM doing lock checks after getting conflicts");

		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(checkManagerLock(tr, bmData));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				if (e.code() == error_code_granule_assignment_conflict) {
					if (BM_DEBUG) {
						fmt::print("BM {0} got lock out of date in lock check on conflict! Dying\n", bmData->epoch);
					}
					if (bmData->iAmReplaced.canBeSet()) {
						bmData->iAmReplaced.send(Void());
					}
					return Void();
				}
				wait(tr->onError(e));
				if (BM_DEBUG) {
					fmt::print("BM {0} still ok after checking lock on conflict\n", bmData->epoch);
				}
			}
		}
		bmData->doLockCheck = Promise<Void>();
	}
}

static void blobManagerExclusionSafetyCheck(Reference<BlobManagerData> self,
                                            BlobManagerExclusionSafetyCheckRequest req) {
	TraceEvent("BMExclusionSafetyCheckBegin", self->id).log();
	BlobManagerExclusionSafetyCheckReply reply(true);
	// make sure at least one blob worker remains after exclusions
	if (self->workersById.empty()) {
		TraceEvent("BMExclusionSafetyCheckNoWorkers", self->id).log();
		reply.safe = false;
	} else {
		std::set<UID> remainingWorkers;
		for (auto& worker : self->workersById) {
			remainingWorkers.insert(worker.first);
		}
		for (const AddressExclusion& excl : req.exclusions) {
			for (auto& worker : self->workersById) {
				if (excl.excludes(worker.second.address())) {
					remainingWorkers.erase(worker.first);
				}
			}
		}

		TraceEvent("BMExclusionSafetyChecked", self->id).detail("RemainingWorkers", remainingWorkers.size()).log();
		reply.safe = !remainingWorkers.empty();
	}

	TraceEvent("BMExclusionSafetyCheckEnd", self->id).log();
	req.reply.send(reply);
}

ACTOR Future<int64_t> bgccCheckGranule(Reference<BlobManagerData> bmData, KeyRange range) {
	state std::pair<RangeResult, Version> fdbResult = wait(readFromFDB(bmData->db, range));
	state Reference<BlobConnectionProvider> bstore = wait(getBStoreForGranule(bmData, range));

	std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blobResult =
	    wait(readFromBlob(bmData->db, bstore, range, 0, fdbResult.second, {}, LockAware::True));

	if (!compareFDBAndBlob(fdbResult.first, blobResult, range, fdbResult.second, BM_DEBUG)) {
		++bmData->stats.ccMismatches;
	}

	int64_t bytesRead = fdbResult.first.expectedSize();

	++bmData->stats.ccGranulesChecked;
	bmData->stats.ccRowsChecked += fdbResult.first.size();
	bmData->stats.ccBytesChecked += bytesRead;

	return bytesRead;
}

// FIXME: could eventually make this more thorough by storing some state in the DB or something
// FIXME: simpler solution could be to shuffle ranges
ACTOR Future<Void> bgConsistencyCheck(Reference<BlobManagerData> bmData) {
	state Reference<IRateControl> rateLimiter =
	    Reference<IRateControl>(new SpeedLimit(SERVER_KNOBS->BG_CONSISTENCY_CHECK_TARGET_SPEED_KB * 1024, 1));

	if (BM_DEBUG) {
		fmt::print("BGCC starting\n");
	}

	loop {
		if (g_network->isSimulated() && g_simulator->speedUpSimulation) {
			if (BM_DEBUG) {
				printf("BGCC stopping\n");
			}
			return Void();
		}

		if (bmData->workersById.size() >= 1) {
			int tries = 10;
			state KeyRange range;
			while (tries > 0) {
				auto randomRange = bmData->workerAssignments.randomRange();
				if (randomRange.value() != UID()) {
					range = randomRange.range();
					break;
				}
				tries--;
			}

			state int64_t allowanceBytes = SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES;
			if (tries == 0) {
				if (BM_DEBUG) {
					printf("BGCC couldn't find random range to check, skipping\n");
				}
			} else {
				try {
					Optional<int64_t> bytesRead =
					    wait(timeout(bgccCheckGranule(bmData, range), SERVER_KNOBS->BGCC_TIMEOUT));
					if (bytesRead.present()) {
						allowanceBytes = bytesRead.get();
					} else {
						++bmData->stats.ccTimeouts;
					}
				} catch (Error& e) {
					if (e.code() == error_code_operation_cancelled) {
						throw e;
					}
					TraceEvent(SevWarn, "BGCCError", bmData->id).error(e).detail("Epoch", bmData->epoch);
					++bmData->stats.ccErrors;
				}
			}
			// wait at least some interval if snapshot is small and to not overwhelm the system with reads (for example,
			// empty database with one empty granule)
			wait(rateLimiter->getAllowance(allowanceBytes) && delay(SERVER_KNOBS->BGCC_MIN_INTERVAL));
		} else {
			if (BM_DEBUG) {
				fmt::print("BGCC found no workers, skipping\n", bmData->workerAssignments.size());
			}
			wait(delay(60.0));
		}
	}
}

ACTOR Future<Void> updateLastFlushVersion(Database db, Version flushVersion) {
	wait(runRYWTransaction(db, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

		BlobGranuleBackupConfig config;
		int64_t epochs = (int64_t)now();
		config.lastFlushTs().set(tr, epochs);
		config.lastFlushVersion().set(tr, flushVersion);
		return Void();
	}));
	return Void();
}

// Flush a blob range and retry for non-fatal errors
ACTOR Future<Void> tryFlushRange(Reference<BlobManagerData> bmData, KeyRange range, Version logEndVersion) {
	state int retryCount = 0;
	loop {
		try {
			FlushGranuleRequest req(bmData->epoch, range, logEndVersion, false);
			wait(success(doBlobGranuleRequests(bmData->db, range, req, &BlobWorkerInterface::flushGranuleRequest)));
			return Void();
		} catch (Error& e) {
			if (e.code() != error_code_blob_granule_transaction_too_old) {
				throw; // terminate for unretryable error
			}

			// check if the range is blobified and then decide retry or skip.
			// it may take long time to flush the whole key range and some ranges may have been unblobified or purged.
			// so we try to check that first when seeing non-fatal errors
			bool knownRange = false;
			for (auto& r : bmData->knownBlobRanges.intersectingRanges(range)) {
				if (r.value()) {
					knownRange = true;
					break;
				}
			}
			if (knownRange) {
				TraceEvent(retryCount > 100 ? SevWarnAlways : SevInfo, "BlobGranulesFlushRetry")
				    .detail("Range", range)
				    .detail("Version", logEndVersion);
				CODE_PROBE(true, "Retry blob granule flush", probe::decoration::rare);
				wait(delayJittered(1));
				++retryCount;
			} else {
				TraceEvent("BlobGranulesFlushSkip").detail("Range", range).detail("Version", logEndVersion);
				CODE_PROBE(true, "Skip blob granule flush", probe::decoration::rare);
				return Void();
			}
		}
	}
}

// Try to flush blob granules. Return the flushed version if it's successful.
ACTOR Future<Version> maybeFlushGranules(Reference<BlobManagerData> bmData) {
	state BlobGranuleBackupConfig config;
	state int64_t lastFlushTs = wait(config.lastFlushTs().getD(SystemDBWriteLockedNow(bmData->db.getReference())));
	bool shouldFlush = lastFlushTs == 0 || (now() - lastFlushTs) > SERVER_KNOBS->BLOB_RESTORE_MLOGS_RETENTION_SECS;
	if (!shouldFlush) {
		int64_t lastFlushVersion =
		    wait(config.lastFlushVersion().getD(SystemDBWriteLockedNow(bmData->db.getReference())));
		TraceEvent("SkipBlobGranulesFlush").detail("LastFlushTs", lastFlushTs).detail("LastFlushVer", lastFlushVersion);
		bmData->stats.lastFlushVersion = lastFlushVersion;
		return invalidVersion;
	}

	state std::string mlogsUrl = wait(config.mutationLogsUrl().getD(SystemDBWriteLockedNow(bmData->db.getReference())));
	state Reference<IBackupContainer> bc = IBackupContainer::openContainer(mlogsUrl, {}, {});
	state BackupDescription desc = wait(bc->describeBackup());
	if (!desc.contiguousLogEnd.present()) {
		TraceEvent("SkipBlobGranulesFlush").detail("LogUrl", mlogsUrl);
		return invalidVersion; // skip truncation if no valid backup for mutation logs
	}
	state Version logEndVersion = desc.contiguousLogEnd.get();
	TraceEvent("DescribedMutationLogs").detail("LogEndVersion", logEndVersion);

	// Flush until end of the mutation log
	state std::vector<KeyRange> flushRanges;
	for (auto& it : bmData->knownBlobRanges.intersectingRanges(normalKeys)) {
		if (it.cvalue()) {
			flushRanges.push_back(it.range());
		}
	}
	TraceEvent("FlushingBlobGranules").detail("Ranges", flushRanges.size());
	state std::vector<Future<Void>> futures;
	for (auto& range : flushRanges) {
		Future<Void> future = tryFlushRange(bmData, range, logEndVersion);
		futures.push_back(future);
		if (futures.size() > SERVER_KNOBS->BLOB_GRANULES_FLUSH_BATCH_SIZE) {
			wait(waitForAll(futures));
			futures.clear();
			TraceEvent("FlushedBlobGranules").detail("Completed", SERVER_KNOBS->BLOB_GRANULES_FLUSH_BATCH_SIZE);
		}
	}
	wait(waitForAll(futures));
	wait(updateLastFlushVersion(bmData->db, logEndVersion));
	bmData->stats.lastFlushVersion = logEndVersion;
	TraceEvent("FlushedBlobGranules").detail("FlushVersion", logEndVersion);
	return logEndVersion;
}

// Truncate mutation logs up to (last flush version - max retention period)
ACTOR Future<Void> truncateMutations(Reference<BlobManagerData> bmData, Version flushVersion) {
	Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(bmData->db));
	Optional<int64_t> logEndEpochs = wait(timeKeeperEpochsFromVersion(flushVersion, tr));
	if (!logEndEpochs.present()) {
		TraceEvent("SkipMutationLogTruncation").detail("LogEndVersion", flushVersion);
		return Void(); // skip truncation if no timestamp about log end
	}

	// Find timestamp and version to truncate
	int64_t epochs = logEndEpochs.get() - SERVER_KNOBS->BLOB_RESTORE_MLOGS_RETENTION_SECS;
	if (epochs <= 0) {
		TraceEvent("SkipMutationLogTruncation").detail("Epochs", epochs);
		return Void();
	}
	state std::string timestamp = BackupAgentBase::formatTime(epochs);
	state Version truncVersion = wait(timeKeeperVersionFromDatetime(timestamp, bmData->db));

	if (truncVersion > 0 && truncVersion < flushVersion) {
		state std::string mlogsUrl =
		    wait(BlobGranuleBackupConfig().mutationLogsUrl().getD(SystemDBWriteLockedNow(bmData->db.getReference())));
		state Reference<IBackupContainer> bc = IBackupContainer::openContainer(mlogsUrl, {}, {});
		wait(bc->expireData(truncVersion, true));
		bmData->stats.lastMLogTruncationVersion = truncVersion;
		TraceEvent("TruncateMutationLogs").detail("Version", truncVersion).detail("Timestamp", timestamp);
		CODE_PROBE(true, "Flush blob granules and truncate mutation logs");
	} else {
		TraceEvent("SkipTruncateMutations").detail("Version", truncVersion).detail("FlushVer", flushVersion);
	}
	return Void();
}

// Dump manifest to external blob storage
ACTOR Future<Void> backupManifest(Reference<BlobManagerData> bmData) {
	TraceEvent("BackupManifest").log();
	auto db = SystemDBWriteLockedNow(bmData->db.getReference());
	std::string url = wait(BlobGranuleBackupConfig().manifestUrl().getD(db));
	Reference<BlobConnectionProvider> manifestStore = BlobConnectionProvider::newBlobConnectionProvider(url);
	int64_t bytes = wait(dumpManifest(bmData->db,
	                                  bmData->dbInfo,
	                                  manifestStore,
	                                  bmData->epoch,
	                                  bmData->manifestDumperSeqNo++,
	                                  bmData->enableManifestEncryption));
	bmData->stats.lastManifestSeqNo = bmData->manifestDumperSeqNo;
	bmData->stats.manifestSizeInBytes += bytes;
	bmData->stats.lastManifestDumpTs = now();
	bmData->manifestCompletitionTrigger.trigger();
	return Void();
}

// Periodically backup manifest
ACTOR Future<Void> backupManifestLoop(Reference<BlobManagerData> bmData) {
	bmData->initBStore();

	DatabaseConfiguration config = wait(getDatabaseConfiguration(bmData->db, true));
	bmData->enableManifestEncryption = config.encryptionAtRestMode.isEncryptionEnabled();

	loop {
		try {
			bool enabled =
			    wait(BlobGranuleBackupConfig().enabled().getD(SystemDBWriteLockedNow(bmData->db.getReference())));
			if (!enabled) {
				TraceEvent("BackupManifestLoopExit").log();
				bmData->stats.lastFlushVersion = 0;
				bmData->stats.lastManifestDumpTs = 0;
				bmData->stats.manifestSizeInBytes = 0;
				return Void();
			}
			wait(backupManifest(bmData));
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw;
			}
			TraceEvent("BackupManifestError").error(e);
		}
		wait(delay(SERVER_KNOBS->BLOB_MANIFEST_BACKUP_INTERVAL) || bmData->backupTrigger.onTrigger());
	}
}

// Periodically flush granules and truncate mutation logs
ACTOR Future<Void> truncateMutationsLoop(Reference<BlobManagerData> bmData) {
	loop {
		try {
			bool enabled =
			    wait(BlobGranuleBackupConfig().enabled().getD(SystemDBWriteLockedNow(bmData->db.getReference())));
			if (!enabled) {
				TraceEvent("TruncateMutationLogsLoopExit").log();
				return Void();
			}

			// Try flush blob granules
			state Version lastFlushVersion = wait(maybeFlushGranules(bmData));
			if (lastFlushVersion != invalidVersion) {
				// Wait for next manifest dumped
				state int64_t seqNo = bmData->manifestDumperSeqNo;
				bmData->backupTrigger.trigger();
				loop {
					TraceEvent("WaitingBlobManifest").detail("Seq", seqNo);
					if (bmData->manifestDumperSeqNo > seqNo) {
						TraceEvent("BlobManifestDumped").detail("Seq", bmData->manifestDumperSeqNo);
						break;
					}
					wait(bmData->manifestCompletitionTrigger.onTrigger());
				}

				// check again since flush may take long time
				bool enabled2 =
				    wait(BlobGranuleBackupConfig().enabled().getD(SystemDBWriteLockedNow(bmData->db.getReference())));
				if (!enabled2) {
					TraceEvent("TruncateMutationLogsLoopExit").log();
					return Void();
				}
				// Truncate mutations up to lastFlushVersion
				wait(truncateMutations(bmData, lastFlushVersion));
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw;
			}
			TraceEvent("TruncateMutationsError").error(e); // skip and retry next time
		}
		wait(delay(SERVER_KNOBS->BLOB_MANIFEST_BACKUP_INTERVAL) || bmData->backupTrigger.onTrigger());
	}
}

// Watch BlobGranuleBackupConfig to start or stop manifest backup actors.
ACTOR Future<Void> watchBackupEnabled(Reference<BlobManagerData> bmData) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(bmData->db));
	state bool prevState = false; // save the last state so we don't have to initialize twice
	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state bool enabled = wait(BlobGranuleBackupConfig().enabled().getD(tr));
			TraceEvent("WatchBackupEnabled").detail("Enabled", enabled);
			if (enabled && !prevState) { // only initialize once
				int64_t lastFlushTs = wait(BlobGranuleBackupConfig().lastFlushTs().getD(tr));
				if (lastFlushTs == 0) {
					TraceEvent("StartInitialFlush").log();
					loop {
						Version lastFlushVersion = wait(maybeFlushGranules(bmData));
						if (lastFlushVersion != invalidVersion) {
							TraceEvent("CompleteInitialFlush").detail("Version", lastFlushVersion);
							break;
						}
						wait(delay(5));
					}
				}
				bmData->addActor.send(backupManifestLoop(bmData));
				bmData->addActor.send(truncateMutationsLoop(bmData));
			} else {
				bmData->backupTrigger.trigger(); // notify backup loops to exit
			}
			prevState = enabled;

			state Future<Void> watch = BlobGranuleBackupConfig().trigger.watch(tr);
			wait(tr->commit());
			wait(watch);
			tr->reset();
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw;
			}
			wait(tr->onError(e));
		}
	}
}

// TODO: Long term, get this info from TenantCache that already calculates and caches this value.
// TODO: If not, parallelize the calls for different tenants.
ACTOR Future<int64_t> getStorageUsage(Database cx, BulkLoadIDKey idKey, TenantGroupName group) {
	state std::vector<std::pair<TenantName, int64_t>> tenants = wait(TenantAPI::listTenantGroupTenants(
	    cx.getReference(), group, ""_sr, "\xff\xff"_sr, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));

	state int64_t usage = 0;
	state int idx = 0;
	for (; idx < tenants.size(); idx++) {
		state int64_t tenantId = tenants[idx].second;
		state ReadYourWritesTransaction tr(makeReference<Tenant>(cx, tenantId));
		loop {
			try {
				state int64_t size = wait(tr.getEstimatedRangeSizeBytes(normalKeys));
				usage += size;
				break;
			} catch (Error& e) {
				if (e.code() == error_code_tenant_not_found) {
					break;
				} else {
					TraceEvent("BulkLoadReservationGetUsageError")
					    .errorUnsuppressed(e)
					    .suppressFor(10.0)
					    .detail("BulkLoadId", idKey.second)
					    .detail("TenantId", tenantId);
					wait(tr.onError(e));
				}
			}
		}
	}
	return usage;
}

ACTOR Future<bool> makeReservationForBulkLoad(Reference<ReadYourWritesTransaction> tr,
                                              BulkLoadIDKey idKey,
                                              Standalone<VectorRef<BulkLoadWrittenFileRef>> files,
                                              TenantGroupName group,
                                              int64_t usage) {
	// Get total file size for this bulk load
	state int64_t bulkLoadSize = 0;
	for (const auto& file : files) {
		bulkLoadSize += file.logicalSize;
	}

	if (bulkLoadSize == 0) {
		DebugBulkLoadTraceEvent(SevDebug, "BulkLoadReservationEmptyLoad").detail("BulkLoadId", idKey.second);
		return true;
	}

	// Get prior reservations for this tenant group
	state Optional<int64_t> reservation = wait(BulkLoadMetadata::reservationMap().get(tr, group));
	if (!reservation.present()) {
		DebugBulkLoadTraceEvent(SevDebug, "BulkLoadReservationNoPriorReservations").detail("BulkLoadId", idKey.second);
		reservation = 0;
	}

	state int64_t newReservation = reservation.get() + bulkLoadSize;
	state std::string quotaTrace = "not checked";

	// Check the total of all reservations and usage against the quota if BULK_LOAD_STORAGE_RESERVATION_ENABLED
	// is true and the storage quota for this tenant group is set
	if (CLIENT_KNOBS->BULK_LOAD_STORAGE_RESERVATION_ENABLED) {
		// Get storage quota for this tenant group
		state Optional<int64_t> quota = wait(TenantMetadata::storageQuota().get(tr, group));
		if (!quota.present()) {
			quotaTrace = "not set";
			DebugBulkLoadTraceEvent(SevDebug, "BulkLoadReservationNoQuota").detail("BulkLoadId", idKey.second);
		} else if (newReservation + usage > quota.get()) {
			DebugBulkLoadTraceEvent(SevDebug, "BulkLoadReservationExceedsQuota")
			    .detail("BulkLoadId", idKey.second)
			    .detail("TenantGroup", group)
			    .detail("Quota", quota.get())
			    .detail("BulkLoadSize", bulkLoadSize)
			    .detail("TenantGroupCurrentUsage", usage)
			    .detail("PriorReservations", reservation);
			return false;
		} else {
			quotaTrace = std::to_string(quota.get());
		}
	}

	BulkLoadMetadata::reservationMap().set(tr, group, newReservation);
	DebugBulkLoadTraceEvent(SevDebug, "BulkLoadReservationSet")
	    .detail("BulkLoadId", idKey.second)
	    .detail("TenantGroup", group)
	    .detail("Quota", quotaTrace)
	    .detail("BulkLoadSize", bulkLoadSize)
	    .detail("TenantGroupCurrentUsage", usage)
	    .detail("PriorReservations", reservation)
	    .detail("NewReservation", newReservation);
	return true;
}

// FIXME: problem is that BM truncates blob range on restart if it bounces in the middle
ACTOR Future<bool> bulkLoadStageBlob(Reference<BlobManagerData> bmData,
                                     BulkLoadIDKey idKey,
                                     Optional<Reference<Tenant>> tenant) {
	// Get current estimated storage usage for this tenant group.
	// This is expensive and is intentionally done outside the scope of `tr`, so it may be a bit stale.
	state Optional<TenantGroupName> tenantGroup;
	state int64_t usage = 0;
	if (CLIENT_KNOBS->BULK_LOAD_STORAGE_RESERVATION_ENABLED) {
		if (tenant.present() && tenant.get()->tenantGroup().present()) {
			tenantGroup = tenant.get()->tenantGroup().get();
			state int64_t _usage = wait(getStorageUsage(bmData->db, idKey, tenantGroup.get()));
			usage = _usage;
		}
	}

	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	state Optional<BulkLoadInfo> info;
	state bool deletedFiles = false;
	state std::vector<Future<Void>> fileDeleteFutures;
	state Reference<BlobConnectionProvider> bstore;
	state KeyRange physicalRange;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			// clean up file pointers if deleted in staging phase
			// FIXME: do reads in parallel
			if (!info.present()) {
				wait(store(info, BulkLoadMetadata::infoMap().get(tr, idKey)));
				if (!info.present()) {
					throw bulk_load_not_found();
				}
				physicalRange = info->keyRange;
				if (tenant.present()) {
					physicalRange = physicalRange.withPrefix(tenant.get()->prefix());
				}
			}

			if (!bstore.isValid()) {
				wait(store(bstore, getBStoreForGranule(bmData, physicalRange)));
				ASSERT(bstore.isValid());
			}

			wait(checkManagerLock(tr, bmData));

			Optional<BulkLoadStatus> status = wait(BulkLoadMetadata::statusMap().get(tr, idKey));
			// since we cached info, on a re-read it could be non-present if bulk load finished by us or someone else
			if (!status.present()) {
				return true;
			}
			if (status.get().state != BulkLoadStatus::State::STAGING) {
				ASSERT(status.get().state > BulkLoadStatus::State::STAGING);
				if (status.get().state >= BulkLoadStatus::State::STAGING_BLOB_DONE) {
					return true;
				}
				break;
			}

			state std::pair<BulkLoadMetadata::FilePointerKey, BulkLoadMetadata::FilePointerKey> idRange =
			    BulkLoadMetadata::allFilePointersForLoadRange(idKey.second);
			state BulkLoadMetadata::CreatedFilePointersMap::RangeResultType createdPointers =
			    wait(BulkLoadMetadata::createdFilePointersMap().getRange(
			        tr, idRange.first, idRange.second, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!createdPointers.more);

			state BulkLoadMetadata::WrittenFilePointersMap::RangeResultType writtenPointers =
			    wait(BulkLoadMetadata::writtenFilePointersMap().getRange(
			        tr, idRange.first, idRange.second, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!writtenPointers.more);

			if (writtenPointers.results.empty()) {
				CODE_PROBE(true, "bulk load aborted because no files written");
				TraceEvent("BulkLoadAbortingNoFilesWritten", bmData->id).detail("BulkLoadID", idKey.second);
				return false;
			}

			// convert to written pointers for validation
			state Standalone<VectorRef<BulkLoadWrittenFileRef>> filesToValidate;
			filesToValidate.reserve(filesToValidate.arena(), writtenPointers.results.size());
			for (auto& it : writtenPointers.results) {
				ASSERT(it.second.createdPointer.type == BulkLoadFileType::SNAPSHOT);
				// FIXME: ASSERT that encrypt mode matches presence of cipher keys
				BulkLoadWrittenFileRef f;
				ASSERT(idKey.second == it.first.first);
				f.fileId = it.first.second;
				f.keyRange = it.second.keyRange;
				f.logicalSize = it.second.logicalSize;
				f.physicalSize = it.second.physicalSize;
				filesToValidate.push_back(filesToValidate.arena(), f);
			}

			if (tenantGroup.present()) {
				state bool reserved =
				    wait(makeReservationForBulkLoad(tr, idKey, filesToValidate, tenantGroup.get(), usage));
				if (!reserved) {
					TraceEvent(SevWarn, "BulkLoadReservationFailed").detail("BulkLoadID", idKey.second);
					// TODO: This causes the bulk load to be aborted. In the future, we could consider instead
					// throwing an error for reservation failure, which could allow a caller to free up space
					// or increase quota then retry this state change (without having to retry the files),
					// or clean up the bulk load.
					// Alternately, even if the bulk load is aborted here, still propagate this info up to
					// the client for visibility.
					return false;
				}
				// TODO: Improve traces since `reserved` could mean that reservation was set but not checked (if
				// CLIENT_KNOBS->BULK_LOAD_STORAGE_RESERVATION_ENABLED is false)
				DebugBulkLoadTraceEvent(SevDebug, "BulkLoadReservationSucceeded").detail("BulkLoadID", idKey.second);
			}

			try {
				validateBulkLoadWrittenPointers(idKey.second, info.get(), filesToValidate);
			} catch (Error& e) {
				if (e.code() == error_code_bulk_load_invalid_file_metadata) {
					CODE_PROBE(true, "bulk load invalid file metadata when staging");
					return false;
				}
				throw e;
			}

			if (!deletedFiles) {
				CODE_PROBE(!createdPointers.results.empty(), "unused bulk load file pointers");
				// do deletes for each unused bulk file pointer in case a file was written there but it was not staged
				for (auto& it : createdPointers.results) {
					ASSERT(idKey.second == it.first.first);
					fileDeleteFutures.push_back(deleteFile(bstore, it.second.filename.toString()));
				}
			}

			if (!deletedFiles && !SERVER_KNOBS->BULK_LOAD_STAGE_GRANULES) {
				for (auto& it : writtenPointers.results) {
					fileDeleteFutures.push_back(deleteFile(bstore, it.second.createdPointer.filename.toString()));
				}
			}

			if (!deletedFiles) {
				wait(waitForAll(fileDeleteFutures));
				bmData->stats.bulkLoadUnusedFilesCleanedUp += fileDeleteFutures.size();
				fileDeleteFutures.clear();
				deletedFiles = true;
			}

			BulkLoadMetadata::statusMap().set(tr, idKey, BulkLoadStatus(BulkLoadStatus::State::STAGING_BLOB_VALIDATED));
			wait(tr->commit());
			DebugBulkLoadTraceEvent(SevDebug, "BulkLoadReservationCommitDone").detail("BulkLoadID", idKey.second);
			break;

		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	if (SERVER_KNOBS->BULK_LOAD_STAGE_GRANULES) {
		if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
			wait(delay(0)); // should be cancelled
			ASSERT(false);
		}

		DebugBulkLoadTraceEvent(SevDebug, "BulkLoadBlobbifying").detail("BulkLoadID", idKey.second);
		wait(bmData->db->blobbifyRangeForBulkLoad(info->keyRange, idKey.second, tenant));
		DebugBulkLoadTraceEvent(SevDebug, "BulkLoadBlobbified").detail("BulkLoadID", idKey.second);

		if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
			wait(delay(0)); // should be cancelled
			ASSERT(false);
		}

		// wait for BM to pick up blobbified range before aborting or progressing to avoid races with abort
		loop {
			auto r = bmData->knownBlobRanges.rangeContaining(physicalRange.begin);
			if (r.value() == 1 && r.range().contains(physicalRange)) {
				if (BM_BULKLOAD_DEBUG) {
					fmt::print("BulkLoad {0} BM got range {1} - {2}\n",
					           idKey.second.printable(),
					           physicalRange.begin.printable(),
					           physicalRange.end.printable());
				}
				break;
			}

			CODE_PROBE(true, "Bulk Load waiting for BM to pick up range");

			if (BM_BULKLOAD_DEBUG) {
				fmt::print("BulkLoad {0} waiting for BM to pick up range {1} - {2}. Current: {3} - {4}: {5}\n",
				           idKey.second.printable(),
				           physicalRange.begin.printable(),
				           physicalRange.end.printable(),
				           r.begin().printable(),
				           r.end().printable(),
				           r.value());
			}

			wait(bmData->loadedClientRangesRound.onChange());
			wait(delay(0));
		}

		tr->reset();
		loop {
			try {
				// this option lets us read from granules while they don't match storage servers
				// if client aborts load, we need to pick that up in parallel
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				wait(checkManagerLock(tr, bmData));
				Optional<BulkLoadStatus> status = wait(BulkLoadMetadata::statusMap().get(tr, idKey));
				int stateInt = status.present() ? static_cast<int>(status->state) : -1;
				if (!status.present() || status->state != BulkLoadStatus::State::STAGING_BLOB_VALIDATED) {
					if (status.present() && status->state == BulkLoadStatus::State::ABORTED) {
						CODE_PROBE(true, "bulk load aborted while creating granules");
						throw bulk_load_aborted();
					}

					DebugBulkLoadTraceEvent(SevDebug, "BulkLoadWaitBlobbifiedStateError", bmData->id)
					    .detail("BulkLoadID", idKey.second)
					    .detail("State", stateInt)
					    .detail("TenantId", idKey.first);

					throw bulk_load_invalid_state_transition();
				}

				tr->setOption(FDBTransactionOptions::HYBRID_READ_DIRTY);

				// FIXME: this might always time out for big ranges
				wait(success(tr->summarizeBlobGranules(physicalRange, tr->getReadVersion().get(), 1000000)));
				break;
			} catch (Error& e) {
				if (e.code() != error_code_blob_granule_transaction_too_old) {
					wait(tr->onError(e));
				}
			}
			wait(delayJittered(0.1));
			tr->reset();
		}

		DebugBulkLoadTraceEvent(SevDebug, "BulkLoadBlobVerified").detail("BulkLoadID", idKey.second);

		if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
			wait(delay(0)); // should be cancelled
			ASSERT(false);
		}
	}

	return true;
}

ACTOR Future<Void> checkLoadAbortedAndManagerLock(Reference<BlobManagerData> bmData,
                                                  Reference<ReadYourWritesTransaction> tr,
                                                  BulkLoadIDKey idKey) {
	state Future<Void> checkManagerF = checkManagerLock(tr, bmData);
	Optional<BulkLoadStatus> status = wait(BulkLoadMetadata::statusMap().get(tr, idKey));
	if (!status.present() || status->state != BulkLoadStatus::State::ABORTED) {
		if (BM_BULKLOAD_DEBUG) {
			fmt::print("CleanupAbortedBulkLoad {0}: already cleaned up\n", idKey.second.printable());
		}
		CODE_PROBE(true, "cleanup aborted bulk load already done");
		DebugBulkLoadTraceEvent(SevDebug, "BulkLoadCheckLoadAbortedFailed", bmData->id)
		    .detail("BulkLoadID", idKey.second)
		    .detail("TenantId", idKey.first)
		    .detail("State", status.present() ? static_cast<int>(status->state) : -1);
		throw bulk_load_invalid_state_transition();
	}
	wait(checkManagerF);
	return Void();
}

ACTOR Future<Void> bulkLoadAbortCleanupGranules(
    Reference<BlobManagerData> bmData,
    BulkLoadMetadata::WrittenFilePointersMap::RangeResultType* writtenPointers,
    BulkLoadIDKey idKey,
    KeyRange physicalRange) {
	state Database cx(bmData->db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

	if (BM_BULKLOAD_DEBUG) {
		fmt::print("BulkLoadAbortCleanupGranules {0}: start\n", idKey.second.printable());
	}

	DebugBulkLoadTraceEvent(SevDebug, "BulkLoadAbortCleanupGranules", bmData->id)
	    .detail("BulkLoadID", idKey.second)
	    .detail("Range", physicalRange)
	    .detail("Stage", "Init");

	std::set<Key> knownBoundariesPurged;
	wait(revokeRangeFromAll(bmData, physicalRange, &knownBoundariesPurged, bulkLoadIdToUID(idKey.second), 0));

	DebugBulkLoadTraceEvent(SevDebug, "BulkLoadAbortCleanupGranules", bmData->id)
	    .detail("BulkLoadID", idKey.second)
	    .detail("Range", physicalRange)
	    .detail("Stage", "Revoked");

	state std::vector<int> sortedToOriginalIdx;
	state BlobGranuleSplitPoints splitPoints =
	    wait(getSplitPointsFromWrittenPointers(writtenPointers, &sortedToOriginalIdx, idKey.first, physicalRange));

	state int startIdx = writtenPointers->results.size() - 1;
	state int batchSize = 100;
	if (BUGGIFY_WITH_PROB(0.1)) {
		batchSize = deterministicRandom()->randomInt(1, 10);
	}
	while (startIdx >= 0) {
		tr->reset();
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			wait(checkLoadAbortedAndManagerLock(bmData, tr, idKey));

			std::vector<Future<Void>> deletions;
			int limit = std::min<int>(batchSize, startIdx + 1);

			for (int granuleIdx = 0; granuleIdx < limit; granuleIdx++) {
				// go backwards so if we kill this in the middle, we still only have a prefix of the range loaded
				int sortedIdx = startIdx - granuleIdx;
				ASSERT(sortedIdx >= 0);
				int originalIdx = sortedToOriginalIdx[sortedIdx];
				KeyRange expandedKeyRange = KeyRangeRef(splitPoints.keys[sortedIdx], splitPoints.keys[sortedIdx + 1]);
				auto& writtenPtr = writtenPointers->results[originalIdx];
				deletions.push_back(bulkLoadDestroyGranuleMetadata(
				    bmData, &tr->getTransaction(), writtenPtr.first, writtenPtr.second, expandedKeyRange));
			}
			wait(waitForAll(deletions));
			wait(tr->commit());
			Version deletedVersion = tr->getCommittedVersion();
			if (deletedVersion < 0) {
				deletedVersion = tr->getReadVersion().get();
			}
			DebugBulkLoadTraceEvent(SevDebug, "BulkLoadAbortCleanupGranules", bmData->id)
			    .detail("BulkLoadID", idKey.second)
			    .detail("Range", physicalRange)
			    .detail("Stage", "DeletedGranuleBatch")
			    .detail("BatchRange",
			            KeyRangeRef(splitPoints.keys[std::max<int>(0, startIdx - batchSize + 1)],
			                        splitPoints.keys[startIdx + 1]))
			    .detail("Version", deletedVersion);
			startIdx -= batchSize;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	DebugBulkLoadTraceEvent(SevDebug, "BulkLoadAbortCleanupGranules", bmData->id)
	    .detail("BulkLoadID", idKey.second)
	    .detail("Range", physicalRange)
	    .detail("Stage", "DeletedGranules");

	// set the entire granule mapping to empty, unset force purge state, and unblobbify
	tr->reset();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkLoadAbortedAndManagerLock(bmData, tr, idKey));
			wait(krmSetRangeCoalescing(tr, blobGranuleForcePurgedKeys.begin, physicalRange, normalKeys, ""_sr));
			wait(krmSetRange(tr, blobGranuleMappingKeys.begin, physicalRange, ""_sr));
			wait(krmSetRangeCoalescing(tr, blobRangeKeys.begin, physicalRange, normalKeys, ""_sr));
			wait(tr->commit());

			Version resetVersion = tr->getCommittedVersion();
			if (resetVersion < 0) {
				resetVersion = tr->getReadVersion().get();
			}

			DebugBulkLoadTraceEvent(SevDebug, "BulkLoadAbortCleanupGranules", bmData->id)
			    .detail("BulkLoadID", idKey.second)
			    .detail("Range", physicalRange)
			    .detail("Stage", "ResetMetadaata")
			    .detail("ResetVersion", resetVersion);
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	return Void();
}

ACTOR Future<Void> cleanupAbortedBulkLoad(Reference<BlobManagerData> bmData,
                                          BulkLoadIDKey idKey,
                                          Optional<Reference<Tenant>> tenant) {
	state Database cx(bmData->db);
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state Optional<BulkLoadInfo> info;
	state bool deletedFiles = false;
	state std::set<BulkLoadFileID> fileIdsDeleted;
	state std::vector<Future<Void>> fileDeleteFutures;
	state Reference<BlobConnectionProvider> bstore;
	state KeyRange physicalRange;

	state BulkLoadMetadata::WrittenFilePointersMap::RangeResultType writtenPointers;
	state std::pair<BulkLoadMetadata::FilePointerKey, BulkLoadMetadata::FilePointerKey> idRange =
	    BulkLoadMetadata::allFilePointersForLoadRange(idKey.second);
	state bool cleanupGranules = SERVER_KNOBS->BULK_LOAD_STAGE_GRANULES;
	// FIXME: also clean up DD and storage. DD cleanup might just be clear custom replication state, and eventually
	// shards merge?

	if (BM_BULKLOAD_DEBUG) {
		fmt::print("CleanupAbortedBulkLoad {0}: start\n", idKey.second.printable());
	}

	// setup + initial load of stuff to clean up
	if (tenant.present()) {
		wait(tenant.get()->ready());
	}

	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			wait(checkLoadAbortedAndManagerLock(bmData, tr, idKey));

			if (!info.present()) {
				wait(store(info, BulkLoadMetadata::infoMap().get(tr, idKey)));
				ASSERT(info.present());
			}

			physicalRange = info->keyRange;
			if (tenant.present()) {
				wait(tenant.get()->ready());
				physicalRange = physicalRange.withPrefix(tenant.get()->prefix());
			}
			bmData->forcePurgingRanges.insert(physicalRange, true);

			if (cleanupGranules) {
				Standalone<VectorRef<KeyRangeRef>> blobRanges =
				    wait(getBlobRanges(&tr->getTransaction(), physicalRange, 1));
				cleanupGranules = !blobRanges.empty();
				if (BM_BULKLOAD_DEBUG) {
					fmt::print("CleanupAbortedBulkLoad {0}: found {1} blob ranges @ {2}\n",
					           idKey.second.printable(),
					           cleanupGranules ? "1" : "0",
					           tr->getReadVersion().get());
				}
				if (cleanupGranules) {
					ASSERT_EQ(1, blobRanges.size());
					ASSERT_EQ(physicalRange.begin, blobRanges[0].begin);
					ASSERT_EQ(physicalRange.end, blobRanges[0].end);

					// set force purge state
					wait(
					    krmSetRangeCoalescing(tr, blobGranuleForcePurgedKeys.begin, physicalRange, normalKeys, "1"_sr));
				}
			}

			wait(store(writtenPointers,
			           BulkLoadMetadata::writtenFilePointersMap().getRange(
			               tr, idRange.first, idRange.second, CLIENT_KNOBS->TOO_MANY)));
			ASSERT(!writtenPointers.more);

			if (cleanupGranules) {
				wait(tr->commit());
			}

			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	wait(store(bstore, getBStoreForGranule(bmData, physicalRange)));
	ASSERT(bstore.isValid());

	// FIXME: wait for data moves away from migrator to finish for range!

	if (cleanupGranules) {
		wait(bulkLoadAbortCleanupGranules(bmData, &writtenPointers, idKey, physicalRange));
	}

	// now delete actual blob files and clear the file pointers
	tr->reset();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			wait(checkLoadAbortedAndManagerLock(bmData, tr, idKey));

			// clean up file pointers if deleted in staging phase

			if (!deletedFiles) {
				state BulkLoadMetadata::CreatedFilePointersMap::RangeResultType createdPointers =
				    wait(BulkLoadMetadata::createdFilePointersMap().getRange(
				        tr, idRange.first, idRange.second, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!createdPointers.more);

				for (auto& it : createdPointers.results) {
					ASSERT(idKey.second == it.first.first);
					fileDeleteFutures.push_back(deleteFile(bstore, it.second.filename.toString()));
				}

				for (auto& it : writtenPointers.results) {
					ASSERT(idKey.second == it.first.first);
					fileDeleteFutures.push_back(deleteFile(bstore, it.second.createdPointer.filename.toString()));
				}

				wait(waitForAll(fileDeleteFutures));

				deletedFiles = true;
			}

			// probably move this to separate transaction
			BulkLoadMetadata::createdFilePointersMap().erase(tr, idRange.first, idRange.second);

			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	// change bulk load state to ABORTED_COMPLETE
	wait(cx->updateBulkLoadState(idKey.second, BulkLoadStatus::State::ABORTED_COMPLETE, tenant));

	if (BM_BULKLOAD_DEBUG) {
		fmt::print("CleanupAbortedBulkLoad {0}: done\n", idKey.second.printable());
	}

	// FIXME: reenable for re-loading same range
	// bmData->forcePurgingRanges.insert(physicalRange, false);

	++bmData->stats.bulkLoadAbortsCleanedUp;

	return Void();
}

// FIXME: handle state transitions from STAGING_BLOB_VALIDATED to STAGING_BLOB_DONE
ACTOR Future<Void> trackBulkLoad(Reference<BlobManagerData> bmData, BulkLoadIDKey idKey, BulkLoadStatus::State state) {
	state ActiveCounter<int>::Releaser holdingLoadCounter = bmData->stats.activeBulkLoads.take(1);

	wait(bmData->doneRecovering.getFuture());
	wait(bmData->loadedClientRanges.getFuture());
	wait(delay(0));

	if (BM_BULKLOAD_DEBUG) {
		fmt::print("DBGBM {0}: TrackBL {1}: start\n", bmData->epoch, idKey.second.printable());
	}

	state Optional<Reference<Tenant>> tenant;
	if (idKey.first != TenantInfo::INVALID_TENANT) {
		state Optional<TenantMapEntry> entry = wait(TenantAPI::tryGetTenant(bmData->db.getReference(), idKey.first));
		if (entry.present()) {
			tenant = makeReference<Tenant>(bmData->db, entry.get());
		} else {
			tenant = makeReference<Tenant>(bmData->db, idKey.first);
		}
	}
	state BulkLoadID id = idKey.second;
	if (state == BulkLoadStatus::State::ABORTED) {
		if (BM_BULKLOAD_DEBUG) {
			fmt::print("DBGBM {0}: TrackBL {1}: already aborted, cleaning up\n", bmData->epoch, id.printable());
		}
		try {
			wait(cleanupAbortedBulkLoad(bmData, idKey, tenant));
		} catch (Error& e) {
			if (e.code() == error_code_bulk_load_not_found) {
				// somebody else must already have finished cleanup
				return Void();
			}
			throw e;
		}
		return Void();
	}
	if (state >= BulkLoadStatus::State::DONE) {
		if (BM_BULKLOAD_DEBUG) {
			fmt::print("DBGBM {0}: TrackBL {1}: already done\n", bmData->epoch, id.printable());
		}
		return Void();
	}

	try {
		if (state < BulkLoadStatus::State::STAGING) {
			wait(bmData->db->waitBulkLoadStateAtLeast(id, BulkLoadStatus::State::STAGING, tenant));
		}

		if (BM_BULKLOAD_DEBUG) {
			fmt::print("DBGBM {0}: TrackBL {1}: staging blob\n", bmData->epoch, id.printable());
		}
		state bool success = wait(bulkLoadStageBlob(bmData, idKey, tenant));
		if (BM_BULKLOAD_DEBUG) {
			fmt::print(
			    "DBGBM {0}: TrackBL {1}: staged blob: {2}\n", bmData->epoch, id.printable(), success ? "T" : "F");
		}
		// before concluding staging of bulk load, inject failure
		if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
			wait(delay(0)); // should be cancelled
			ASSERT(false);
		}
		if (success) {
			++bmData->stats.bulkLoadsStaged;
			// TODO: do we also need to check manager lock as part of these transactions? should be idempotent by
			// any manager
			// FIXME: only change to STAGING_BLOB_DONE. Still monitor for aborts until we hit DONE though
			wait(bmData->db->updateBulkLoadState(id, BulkLoadStatus::State::DONE, tenant));
		} else {
			++bmData->stats.bulkLoadStagingErrors;
			wait(bmData->db->updateBulkLoadState(id, BulkLoadStatus::State::ABORTED, tenant));
			// don't return, we have to do cleanup
			throw bulk_load_aborted();
		}
		if (BM_BULKLOAD_DEBUG) {
			fmt::print("DBGBM {0}: TrackBL {1}: staged blob: done\n", bmData->epoch, id.printable());
		}
		return Void();
	} catch (Error& e) {
		if (BM_BULKLOAD_DEBUG) {
			fmt::print("DBGBM {0}: TrackBL {1}: error {2}\n", bmData->epoch, id.printable(), e.name());
		}
		if (e.code() == error_code_bulk_load_not_found) {
			// If bulk load aborted/cleaned up, stop tracking it
			// TODO: code probe
			return Void();
		}
		state Error err = e;
		if (err.code() == error_code_bulk_load_aborted) {
			try {
				if (BM_BULKLOAD_DEBUG) {
					fmt::print("DBGBM {0}: TrackBL {1}: cleaning up aborted load\n", bmData->epoch, id.printable());
				}
				wait(cleanupAbortedBulkLoad(bmData, idKey, tenant));
				if (BM_BULKLOAD_DEBUG) {
					fmt::print("DBGBM {0}: TrackBL {1}: cleaned up aborted load\n", bmData->epoch, id.printable());
				}
			} catch (Error& e2) {
				if (BM_BULKLOAD_DEBUG) {
					fmt::print("DBGBM {0}: TrackBL {1}: error cleaning up aborted load {2}\n",
					           bmData->epoch,
					           id.printable(),
					           e2.name());
				}
				if (e2.code() == error_code_bulk_load_not_found) {
					// somebody else must already have finished cleanup
					return Void();
				}
				throw e2;
			}
			return Void();
		} else {
			throw err;
		}
	}
}

// Simulation validation that multiple blob managers aren't started with the same epoch within same cluster
static std::map<std::pair<UID, int64_t>, UID> managerEpochsSeen;

ACTOR Future<Void> checkBlobManagerEpoch(Reference<AsyncVar<ServerDBInfo> const> dbInfo, int64_t epoch, UID dbgid) {
	loop {
		if (dbInfo->get().blobManager.present() && dbInfo->get().blobManager.get().epoch > epoch) {
			TraceEvent("BMRemovedFromDBInfo", dbgid)
			    .detail("Epoch", epoch)
			    .detail("NewEpoch", dbInfo->get().blobManager.get().epoch);
			throw worker_removed();
		}
		wait(dbInfo->onChange());
	}
}

ACTOR Future<UID> fetchClusterId(Database db) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			ValueReadResult clusterIdVal = wait(tr->get(clusterIdKey));
			if (clusterIdVal.present()) {
				UID clusterId = BinaryReader::fromStringRef<UID>(clusterIdVal.get(), IncludeVersion());
				return clusterId;
			}
			wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY, TaskPriority::BlobManager));
			tr->reset();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

} // anonymous namespace

ACTOR Future<Void> blobManager(BlobManagerInterface bmInterf,
                               Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                               int64_t epoch) {
	state Reference<BlobManagerData> self =
	    makeReference<BlobManagerData>(bmInterf.id(),
	                                   dbInfo,
	                                   openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True),
	                                   bmInterf.locality.dcId(),
	                                   epoch);

	if (g_network->isSimulated()) {
		UID clusterId = wait(fetchClusterId(self->db));
		auto clusterEpoc = std::make_pair(clusterId, epoch);
		bool managerEpochAlreadySeen = managerEpochsSeen.count(clusterEpoc);
		if (managerEpochAlreadySeen) {
			TraceEvent(SevError, "DuplicateBlobManagersAtEpoch")
			    .detail("ClusterId", clusterId)
			    .detail("Epoch", epoch)
			    .detail("BMID1", bmInterf.id())
			    .detail("BMID2", managerEpochsSeen.at(clusterEpoc));
		}
		ASSERT(!managerEpochAlreadySeen);
		managerEpochsSeen[clusterEpoc] = bmInterf.id();
	}
	state Future<Void> collection = actorCollection(self->addActor.getFuture());

	if (BM_DEBUG) {
		fmt::print("Blob manager {0} starting...\n", epoch);
	}
	TraceEvent("BlobManagerInit", bmInterf.id()).detail("Epoch", epoch).log();

	self->epoch = epoch;

	try {
		// although we start the recruiter, we wait until existing workers are ack'd
		auto recruitBlobWorker = IAsyncListener<RequestStream<RecruitBlobWorkerRequest>>::create(
		    dbInfo, [](auto const& info) { return info.clusterInterface.recruitBlobWorker; });

		self->addActor.send(blobWorkerRecruiter(self, recruitBlobWorker));
		self->addActor.send(checkBlobManagerEpoch(dbInfo, epoch, bmInterf.id()));

		// we need to recover the old blob manager's state (e.g. granule assignments) before
		// before the new blob manager does anything
		choose {
			when(wait(recoverBlobManager(self) || collection)) {}
			when(wait(self->iAmReplaced.getFuture())) {}
		}

		self->addActor.send(doLockChecks(self));
		self->addActor.send(monitorClientRanges(self));
		self->addActor.send(monitorTenants(self));
		self->addActor.send(monitorPurgeKeys(self));
		self->addActor.send(cleanupCompletedPurgeKeys(self));
		if (SERVER_KNOBS->BG_CONSISTENCY_CHECK_ENABLED && !self->isFullRestoreMode &&
		    (!CLIENT_KNOBS->ALLOW_BULK_LOADS || !SERVER_KNOBS->BULK_LOAD_STAGE_GRANULES ||
		     (SERVER_KNOBS->BULK_LOAD_STAGE_GRANULES && SERVER_KNOBS->BULK_LOAD_SS_MOVE))) {
			// FIXME: reenable once bulk load data in storage servers!!
			self->addActor.send(bgConsistencyCheck(self));
		}
		if (SERVER_KNOBS->BG_ENABLE_MERGING) {
			self->addActor.send(granuleMergeChecker(self));
		}

		if (BUGGIFY && !self->isFullRestoreMode) {
			self->addActor.send(chaosRangeMover(self));
		}

		self->addActor.send(watchBackupEnabled(self));

		if (CLIENT_KNOBS->ALLOW_BULK_LOADS) {
			BulkLoadTracker::Handler bulkLoadHandler = [self = self](const BulkLoadIDKey& id,
			                                                         const BulkLoadStatus::State& state) {
				return trackBulkLoad(self, id, state);
			};
			state BulkLoadTracker bulkLoadTracker(self->db, bulkLoadHandler);
			self->addActor.send(bulkLoadTracker.start());
		}

		CODE_PROBE(true, "blobManager started");

		loop choose {
			when(wait(self->iAmReplaced.getFuture())) {
				if (BM_DEBUG) {
					fmt::print("BM {} exiting because it is replaced\n", self->epoch);
				}
				TraceEvent("BlobManagerReplaced", bmInterf.id()).detail("Epoch", epoch);
				wait(delay(0.0));
				break;
			}
			when(HaltBlobManagerRequest req = waitNext(bmInterf.haltBlobManager.getFuture())) {
				req.reply.send(Void());
				TraceEvent("BlobManagerHalted", bmInterf.id()).detail("Epoch", epoch).detail("ReqID", req.requesterID);
				break;
			}
			when(state HaltBlobGranulesRequest req = waitNext(bmInterf.haltBlobGranules.getFuture())) {
				wait(haltBlobGranules(self) || collection);
				req.reply.send(Void());
				TraceEvent("BlobGranulesHalted", bmInterf.id()).detail("Epoch", epoch).detail("ReqID", req.requesterID);
				break;
			}
			when(BlobManagerExclusionSafetyCheckRequest req = waitNext(bmInterf.blobManagerExclCheckReq.getFuture())) {
				blobManagerExclusionSafetyCheck(self, req);
			}
			when(BlobManagerBlockedRequest req = waitNext(bmInterf.blobManagerBlockedReq.getFuture())) {
				req.reply.send(BlobManagerBlockedReply(self->stats.blockedAssignments));
			}
			when(wait(collection)) {
				TraceEvent(SevError, "BlobManagerActorCollectionError");
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& err) {
		TraceEvent("BlobManagerDied", bmInterf.id()).errorUnsuppressed(err);
	}
	// prevent a reference counting cycle
	self->assignsInProgress = KeyRangeActorMap();
	self->boundaryEvaluations.clear();
	self->latestActivePurges.clear();
	return Void();
}

namespace {
// Test:
// start empty
// DB has [A - B). That should show up in knownBlobRanges and should be in added
// DB has nothing. knownBlobRanges should be empty and [A - B) should be in removed
// DB has [A - B) and [C - D). They should both show up in knownBlobRanges and added.
// DB has [A - D). It should show up coalesced in knownBlobRanges, and [B - C) should be in added.
// DB has [A - C). It should show up coalesced in knownBlobRanges, and [C - D) should be in removed.
// DB has [B - C). It should show up coalesced in knownBlobRanges, and [A - B) should be removed.
// DB has [B - D). It should show up coalesced in knownBlobRanges, and [C - D) should be removed.
// DB has [A - D). It should show up coalesced in knownBlobRanges, and [A - B) should be removed.
// DB has [A - B) and [C - D). They should show up in knownBlobRanges, and [B - C) should be in removed.
// DB has [B - C). It should show up in knownBlobRanges, [B - C) should be in added, and [A - B) and [C - D)
// should be in removed.
TEST_CASE("/blobmanager/updateranges") {
	KeyRangeMap<bool> knownBlobRanges(false, normalKeys.end);
	Arena ar;

	VectorRef<KeyRangeRef> added;
	VectorRef<KeyRangeRef> removed;

	RangeResult dbDataEmpty;
	std::vector<std::pair<KeyRangeRef, bool>> kbrRanges;

	StringRef keyA = StringRef(ar, "A"_sr);
	StringRef keyB = StringRef(ar, "B"_sr);
	StringRef keyC = StringRef(ar, "C"_sr);
	StringRef keyD = StringRef(ar, "D"_sr);

	// db data setup
	RangeResult dbDataAB;
	dbDataAB.emplace_back(ar, keyA, blobRangeActive);
	dbDataAB.emplace_back(ar, keyB, blobRangeInactive);

	RangeResult dbDataAC;
	dbDataAC.emplace_back(ar, keyA, blobRangeActive);
	dbDataAC.emplace_back(ar, keyC, blobRangeInactive);

	RangeResult dbDataAD;
	dbDataAD.emplace_back(ar, keyA, blobRangeActive);
	dbDataAD.emplace_back(ar, keyD, blobRangeInactive);

	RangeResult dbDataBC;
	dbDataBC.emplace_back(ar, keyB, blobRangeActive);
	dbDataBC.emplace_back(ar, keyC, blobRangeInactive);

	RangeResult dbDataBD;
	dbDataBD.emplace_back(ar, keyB, blobRangeActive);
	dbDataBD.emplace_back(ar, keyD, blobRangeInactive);

	RangeResult dbDataCD;
	dbDataCD.emplace_back(ar, keyC, blobRangeActive);
	dbDataCD.emplace_back(ar, keyD, blobRangeInactive);

	RangeResult dbDataAB_CD;
	dbDataAB_CD.emplace_back(ar, keyA, blobRangeActive);
	dbDataAB_CD.emplace_back(ar, keyB, blobRangeInactive);
	dbDataAB_CD.emplace_back(ar, keyC, blobRangeActive);
	dbDataAB_CD.emplace_back(ar, keyD, blobRangeInactive);

	// key ranges setup
	KeyRangeRef rangeAB = KeyRangeRef(keyA, keyB);
	KeyRangeRef rangeAC = KeyRangeRef(keyA, keyC);
	KeyRangeRef rangeAD = KeyRangeRef(keyA, keyD);

	KeyRangeRef rangeBC = KeyRangeRef(keyB, keyC);
	KeyRangeRef rangeBD = KeyRangeRef(keyB, keyD);

	KeyRangeRef rangeCD = KeyRangeRef(keyC, keyD);

	KeyRangeRef rangeStartToA = KeyRangeRef(normalKeys.begin, keyA);
	KeyRangeRef rangeStartToB = KeyRangeRef(normalKeys.begin, keyB);
	KeyRangeRef rangeStartToC = KeyRangeRef(normalKeys.begin, keyC);
	KeyRangeRef rangeBToEnd = KeyRangeRef(keyB, normalKeys.end);
	KeyRangeRef rangeCToEnd = KeyRangeRef(keyC, normalKeys.end);
	KeyRangeRef rangeDToEnd = KeyRangeRef(keyD, normalKeys.end);

	// actual test

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 1);
	ASSERT(kbrRanges[0].first == normalKeys);
	ASSERT(!kbrRanges[0].second);

	// DB has [A - B)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(1, &knownBlobRanges, dbDataAB, ar, &added, &removed);

	ASSERT(added.size() == 1);
	ASSERT(added[0] == rangeAB);

	ASSERT(removed.size() == 0);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToA);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeAB);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeBToEnd);
	ASSERT(!kbrRanges[2].second);

	// DB has nothing
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(1, &knownBlobRanges, dbDataEmpty, ar, &added, &removed);

	ASSERT(added.size() == 0);

	ASSERT(removed.size() == 1);
	ASSERT(removed[0] == rangeAB);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges[0].first == normalKeys);
	ASSERT(!kbrRanges[0].second);

	// DB has [A - B) and [C - D)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(1, &knownBlobRanges, dbDataAB_CD, ar, &added, &removed);

	ASSERT(added.size() == 2);
	ASSERT(added[0] == rangeAB);
	ASSERT(added[1] == rangeCD);

	ASSERT(removed.size() == 0);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 5);
	ASSERT(kbrRanges[0].first == rangeStartToA);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeAB);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeBC);
	ASSERT(!kbrRanges[2].second);
	ASSERT(kbrRanges[3].first == rangeCD);
	ASSERT(kbrRanges[3].second);
	ASSERT(kbrRanges[4].first == rangeDToEnd);
	ASSERT(!kbrRanges[4].second);

	// DB has [A - D)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(1, &knownBlobRanges, dbDataAD, ar, &added, &removed);

	ASSERT(added.size() == 1);
	ASSERT(added[0] == rangeBC);

	ASSERT(removed.size() == 0);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToA);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeAD);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeDToEnd);
	ASSERT(!kbrRanges[2].second);

	// DB has [A - C)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(1, &knownBlobRanges, dbDataAC, ar, &added, &removed);

	ASSERT(added.size() == 0);

	ASSERT(removed.size() == 1);
	ASSERT(removed[0] == rangeCD);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToA);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeAC);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeCToEnd);
	ASSERT(!kbrRanges[2].second);

	// DB has [B - C)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(1, &knownBlobRanges, dbDataBC, ar, &added, &removed);

	ASSERT(added.size() == 0);

	ASSERT(removed.size() == 1);
	ASSERT(removed[0] == rangeAB);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToB);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeBC);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeCToEnd);
	ASSERT(!kbrRanges[2].second);

	// DB has [B - D)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(1, &knownBlobRanges, dbDataBD, ar, &added, &removed);

	ASSERT(added.size() == 1);
	ASSERT(added[0] == rangeCD);

	ASSERT(removed.size() == 0);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToB);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeBD);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeDToEnd);
	ASSERT(!kbrRanges[2].second);

	// DB has [A - D)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(1, &knownBlobRanges, dbDataAD, ar, &added, &removed);

	ASSERT(added.size() == 1);
	ASSERT(added[0] == rangeAB);

	ASSERT(removed.size() == 0);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToA);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeAD);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeDToEnd);
	ASSERT(!kbrRanges[2].second);

	// DB has [A - B) and [C - D)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(1, &knownBlobRanges, dbDataAB_CD, ar, &added, &removed);

	ASSERT(added.size() == 0);

	ASSERT(removed.size() == 1);
	ASSERT(removed[0] == rangeBC);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 5);
	ASSERT(kbrRanges[0].first == rangeStartToA);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeAB);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeBC);
	ASSERT(!kbrRanges[2].second);
	ASSERT(kbrRanges[3].first == rangeCD);
	ASSERT(kbrRanges[3].second);
	ASSERT(kbrRanges[4].first == rangeDToEnd);
	ASSERT(!kbrRanges[4].second);

	// DB has [B - C)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(1, &knownBlobRanges, dbDataBC, ar, &added, &removed);

	ASSERT(added.size() == 1);
	ASSERT(added[0] == rangeBC);

	ASSERT(removed.size() == 2);
	ASSERT(removed[0] == rangeAB);
	ASSERT(removed[1] == rangeCD);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToB);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeBC);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeCToEnd);
	ASSERT(!kbrRanges[2].second);

	return Void();
}

TEST_CASE("/blobmanager/objectwriter") {
	InitializeBlobWorkerRequest initReq1;
	initReq1.reqId = deterministicRandom()->randomUniqueID();
	initReq1.interfaceId = deterministicRandom()->randomUniqueID();
	initReq1.storeType = (KeyValueStoreType::StoreType)(SERVER_KNOBS->BLOB_WORKER_STORE_TYPE);
	initReq1.encryptMode = EncryptionAtRestMode::DOMAIN_AWARE;

	InitializeBlobWorkerRequestOld initReq2;
	initReq2.reqId = deterministicRandom()->randomUniqueID();
	initReq2.interfaceId = deterministicRandom()->randomUniqueID();
	initReq2.storeType = (KeyValueStoreType::StoreType)(SERVER_KNOBS->BLOB_WORKER_STORE_TYPE);

	Value newReq = ObjectWriter::toValue(initReq1, Unversioned());
	Value oldReq = ObjectWriter::toValue(initReq2, Unversioned());

	InitializeBlobWorkerRequest decode1 =
	    ObjectReader::fromStringRef<InitializeBlobWorkerRequest>(oldReq, Unversioned());
	InitializeBlobWorkerRequestOld decode2 =
	    ObjectReader::fromStringRef<InitializeBlobWorkerRequestOld>(newReq, Unversioned());

	ASSERT(decode1.reqId == initReq2.reqId);
	ASSERT(decode1.interfaceId == initReq2.interfaceId);
	ASSERT(decode1.storeType == initReq2.storeType);
	printf("%d %d\n", decode1.encryptMode.mode, EncryptionAtRestMode().mode);
	ASSERT(decode1.encryptMode == EncryptionAtRestMode());

	ASSERT(decode2.reqId == initReq1.reqId);
	ASSERT(decode2.interfaceId == initReq1.interfaceId);
	ASSERT(decode2.storeType == initReq1.storeType);

	return Void();
}

} // anonymous namespace