/*
 * Resolver.actor.cpp
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
#include <memory>

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Notified.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/ApplyMetadataMutation.h"
#include "fdbserver/ConflictSet.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/LogSystemDiskQueueAdapter.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/ResolverInterface.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/StorageMetrics.actor.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/ActorCollection.h"
#include "flow/Error.h"
#include "flow/Histogram.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
struct ProxyRequestsInfo {
	std::map<Version, ResolveTransactionBatchReply> outstandingBatches;
	Version lastVersion;

	ProxyRequestsInfo() : lastVersion(-1) {}
};
} // namespace

namespace {

class RecentStateTransactionsInfo {
public:
	RecentStateTransactionsInfo() = default;

	// Erases state transactions up to the given version (inclusive) and returns
	// the number of bytes for the erased mutations.
	int64_t eraseUpTo(Version oldestVersion) {
		recentStateTransactions.erase(recentStateTransactions.begin(),
		                              recentStateTransactions.upper_bound(oldestVersion));

		int64_t stateBytes = 0;
		while (recentStateTransactionSizes.size() && recentStateTransactionSizes.front().first <= oldestVersion) {
			stateBytes += recentStateTransactionSizes.front().second;
			recentStateTransactionSizes.pop_front();
		}
		return stateBytes;
	}

	// Adds state transactions between two versions to the reply message.
	void applyStateTxnsToBatchReply(ResolveTransactionBatchReply* reply,
	                                Version firstUnseenVersion,
	                                Version commitVersion) {
		auto stateTransactionItr = recentStateTransactions.lower_bound(firstUnseenVersion);
		auto endItr = recentStateTransactions.lower_bound(commitVersion);
		// Resolver only sends back prior state txns back, because the proxy
		// sends this request has them and will apply them via applyMetadataToCommittedTransactions();
		// and other proxies will get this version's state txns as a prior version.
		for (; stateTransactionItr != endItr; ++stateTransactionItr) {
			reply->stateMutations.push_back(reply->arena, stateTransactionItr->value.second);
			reply->arena.dependsOn(stateTransactionItr->value.second.arena());
		}
	}

	bool empty() const { return recentStateTransactionSizes.empty(); }
	// Returns the number of versions with non-empty state transactions.
	uint32_t size() const { return recentStateTransactionSizes.size(); }

	// Returns the first/smallest version of the state transactions.
	// This can only be called when empty() returns false or size() > 0.
	Version firstVersion() const { return recentStateTransactionSizes.front().first; }

	// Records non-zero stateBytes for a version.
	void addVersionBytes(Version commitVersion, int64_t stateBytes) {
		if (stateBytes > 0)
			recentStateTransactionSizes.emplace_back(commitVersion, stateBytes);
	}

	// Returns the reference to the pair of (shardChanged, stateMutations) for the given version
	std::pair<bool, Standalone<VectorRef<StateTransactionRef>>>& getStateTransactionsRef(Version commitVersion) {
		return recentStateTransactions[commitVersion];
	}

private:
	// Commit version to a pair of (shardChanged, stateMutations).
	Map<Version, std::pair<bool, Standalone<VectorRef<StateTransactionRef>>>> recentStateTransactions;

	// Only keep versions with non-zero size state transactions.
	Deque<std::pair<Version, int64_t>> recentStateTransactionSizes;
};

struct Resolver : ReferenceCounted<Resolver> {
	const UID dbgid;
	const int commitProxyCount, resolverCount;
	NotifiedVersion version;
	AsyncVar<Version> neededVersion;

	RecentStateTransactionsInfo recentStateTransactionsInfo;
	// total mutation bytes including normal mutation bytes cached in resolver
	AsyncVar<int64_t> totalStateBytes;
	AsyncTrigger checkNeededVersion;
	std::map<NetworkAddress, ProxyRequestsInfo> proxyInfoMap;
	ConflictSet* conflictSet;
	TransientStorageMetricSample iopsSample;

	EncryptionAtRestMode encryptMode;

	// Use LogSystem as backend for txnStateStore. However, the real commit
	// happens at commit proxies and we never "write" to the LogSystem at
	// Resolvers.
	LogSystemDiskQueueAdapter* logAdapter = nullptr;
	Reference<ILogSystem> logSystem;
	Reference<IKeyValueStore> txnStateStore;
	int localTLogCount = -1;

	std::map<UID, Reference<StorageInfo>> storageCache;
	KeyRangeMap<ServerCacheInfo> keyInfo; // keyrange -> all storage servers in all DCs for the keyrange
	std::unordered_map<UID, StorageServerInterface> tssMapping;
	bool forceRecovery = false;

	Version debugMinRecentStateVersion = 0;

	CounterCollection cc;
	Counter resolveBatchIn;
	Counter resolveBatchStart;
	Counter resolvedTransactions;
	Counter resolvedBytes;
	Counter resolvedReadConflictRanges;
	Counter resolvedWriteConflictRanges;
	Counter transactionsAccepted;
	Counter transactionsTooOld;
	Counter transactionsConflicted;
	Counter resolvedStateTransactions;
	Counter resolvedStateMutations;
	Counter resolvedStateBytes;
	// when a state transaction also contains normal key mutations
	Counter resolvedStateNormalMutations;
	Counter resolvedStateNormalMutationBytes;
	Counter resolveBatchOut;
	Counter metricsRequests;
	Counter splitRequests;
	int numLogs;

	// End-to-end server latency of resolver requests.
	Reference<Histogram> resolverLatencyDist;

	// Queue wait times, per request.
	Reference<Histogram> queueWaitLatencyDist;

	// Actual work, per req request.
	Reference<Histogram> computeTimeDist;

	// Distribution of waiters in queue.
	// 0 or 1 will be most common, but higher values are interesting.
	Reference<Histogram> queueDepthDist;

	Future<Void> logger;

	Resolver(UID dbgid, int commitProxyCount, int resolverCount, EncryptionAtRestMode encryptMode)
	  : dbgid(dbgid),
	    commitProxyCount(commitProxyCount),
	    resolverCount(resolverCount),
	    version(-1),
	    conflictSet(newConflictSet()),
	    iopsSample(SERVER_KNOBS->KEY_BYTES_PER_SAMPLE),
	    encryptMode(encryptMode),
	    cc("Resolver", dbgid.toString()),
	    resolveBatchIn("ResolveBatchIn", cc),
	    resolveBatchStart("ResolveBatchStart", cc),
	    resolvedTransactions("ResolvedTransactions", cc),
	    resolvedBytes("ResolvedBytes", cc),
	    resolvedReadConflictRanges("ResolvedReadConflictRanges", cc),
	    resolvedWriteConflictRanges("ResolvedWriteConflictRanges", cc),
	    transactionsAccepted("TransactionsAccepted", cc),
	    transactionsTooOld("TransactionsTooOld", cc),
	    transactionsConflicted("TransactionsConflicted", cc),
	    resolvedStateTransactions("ResolvedStateTransactions", cc),
	    resolvedStateMutations("ResolvedStateMutations", cc),
	    resolvedStateBytes("ResolvedStateBytes", cc),
	    resolvedStateNormalMutations("ResolvedStateNormalMutations", cc),
	    resolvedStateNormalMutationBytes("ResolvedStateNormalMutationBytes", cc),
	    resolveBatchOut("ResolveBatchOut", cc),
	    metricsRequests("MetricsRequests", cc),
	    splitRequests("SplitRequests", cc),
	    resolverLatencyDist(Histogram::getHistogram("Resolver"_sr, "Latency"_sr, Histogram::Unit::milliseconds)),
	    queueWaitLatencyDist(Histogram::getHistogram("Resolver"_sr, "QueueWait"_sr, Histogram::Unit::milliseconds)),
	    computeTimeDist(Histogram::getHistogram("Resolver"_sr, "ComputeTime"_sr, Histogram::Unit::milliseconds)),
	    // Distribution of queue depths, with knowledge that Histogram has 32 buckets, and each bucket will have size 1.
	    queueDepthDist(Histogram::getHistogram("Resolver"_sr, "QueueDepth"_sr, Histogram::Unit::countLinear, 0, 31)) {
		specialCounter(cc, "Version", [this]() { return this->version.get(); });
		specialCounter(cc, "NeededVersion", [this]() { return this->neededVersion.get(); });
		specialCounter(cc, "TotalStateBytes", [this]() { return this->totalStateBytes.get(); });

		logger = cc.traceCounters("ResolverMetrics", dbgid, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, "ResolverMetrics");
	}
	~Resolver() { destroyConflictSet(conflictSet); }
};
} // namespace

ACTOR Future<Void> versionReady(Resolver* self, ProxyRequestsInfo* proxyInfo, Version prevVersion) {
	loop {
		if (self->recentStateTransactionsInfo.size() &&
		    proxyInfo->lastVersion <= self->recentStateTransactionsInfo.firstVersion()) {
			self->neededVersion.set(std::max(self->neededVersion.get(), prevVersion));
		}

		// Update queue depth metric before waiting. Check if we're going to be one of the waiters or not.
		int waiters = self->version.numWaiting();
		if (self->version.get() < prevVersion) {
			waiters++;
		}
		self->queueDepthDist->sampleRecordCounter(waiters);

		choose {
			when(wait(self->version.whenAtLeast(prevVersion))) {
				// Update queue depth metric after waiting.
				self->queueDepthDist->sampleRecordCounter(self->version.numWaiting());
				return Void();
			}
			when(wait(self->checkNeededVersion.onTrigger())) {}
		}
	}
}

ACTOR Future<Void> resolveBatch(Reference<Resolver> self,
                                ResolveTransactionBatchRequest req,
                                Reference<AsyncVar<ServerDBInfo> const> db) {
	state Optional<UID> debugID;
	state Span span("R:resolveBatch"_loc, req.spanContext);

	// The first request (prevVersion < 0) comes from the master
	state NetworkAddress proxyAddress =
	    req.prevVersion >= 0 ? req.reply.getEndpoint().getPrimaryAddress() : NetworkAddress();
	state ProxyRequestsInfo& proxyInfo = self->proxyInfoMap[proxyAddress];

	state std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cipherKeys;
	if (self->encryptMode.isEncryptionEnabled()) {
		static const std::unordered_set<EncryptCipherDomainId> metadataDomainIds = { SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID,
			                                                                         ENCRYPT_HEADER_DOMAIN_ID };
		std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>> cks =
		    wait(GetEncryptCipherKeys<ServerDBInfo>::getLatestEncryptCipherKeys(
		        db, metadataDomainIds, BlobCipherMetrics::TLOG, nullptr, {}, req.debugID));
		cipherKeys = cks;
	}

	++self->resolveBatchIn;

	if (req.debugID.present()) {
		debugID = nondeterministicRandom()->randomUniqueID();
		g_traceBatch.addAttach("CommitAttachID", req.debugID.get().first(), debugID.get().first());
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "Resolver.resolveBatch.Before");
	}

	/* TraceEvent("ResolveBatchStart", self->dbgid).detail("From", proxyAddress).detail("Version",
	   req.version).detail("PrevVersion", req.prevVersion).detail("StateTransactions", req.txnStateTransactions.size())
	    .detail("RecentStateTransactions", self->recentStateTransactionsInfo.size()).detail("LastVersion",
	   proxyInfo.lastVersion).detail("FirstVersion", self->recentStateTransactionsInfo.empty() ? -1 :
	   self->recentStateTransactionsInfo.firstVersion()) .detail("ResolverVersion", self->version.get()); */

	while (self->recentStateTransactionsInfo.size() &&
	       proxyInfo.lastVersion > self->recentStateTransactionsInfo.firstVersion() &&
	       req.version > self->neededVersion.get()) {
		/* TraceEvent("ResolveBatchDelay").detail("From", proxyAddress).detail("StateBytes",
	 self->totalStateBytes.get()).detail("RecentStateTransactionSize", self->recentStateTransactionsInfo.size())
	 .detail("LastVersion", proxyInfo.lastVersion).detail("RequestVersion", req.version).detail("NeededVersion",
		     self->neededVersion.get()) .detail("RecentStateVer", self->recentStateTransactionsInfo.firstVersion());*/
		wait(self->totalStateBytes.onChange() || self->neededVersion.onChange());
	}

	if (debugID.present()) {
		g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "Resolver.resolveBatch.AfterQueueSizeCheck");
	}

	wait(versionReady(self.getPtr(), &proxyInfo, req.prevVersion));

	if (check_yield(TaskPriority::DefaultEndpoint)) {
		wait(delay(0, TaskPriority::Low) || delay(SERVER_KNOBS->COMMIT_SLEEP_TIME)); // FIXME: Is this still right?
		g_network->setCurrentTask(TaskPriority::DefaultEndpoint);
	}

	// Time until now has been spent waiting in the queue to do actual work.
	const double queueWaitEndTime = timer();
	self->queueWaitLatencyDist->sampleSeconds(queueWaitEndTime - req.requestTime());

	if (self->version.get() ==
	    req.prevVersion) { // Not a duplicate (check relies on no waiting between here and self->version.set() below!)
		// This is the beginning of the compute phase of the
		// resolver. There's no wait before it's done.
		const double beginComputeTime = timer();

		++self->resolveBatchStart;
		self->resolvedTransactions += req.transactions.size();
		self->resolvedBytes += req.transactions.expectedSize();

		if (proxyInfo.lastVersion > 0) {
			proxyInfo.outstandingBatches.erase(proxyInfo.outstandingBatches.begin(),
			                                   proxyInfo.outstandingBatches.upper_bound(req.lastReceivedVersion));
		}

		Version firstUnseenVersion = proxyInfo.lastVersion + 1;
		proxyInfo.lastVersion = req.version;

		if (req.debugID.present())
			g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "Resolver.resolveBatch.AfterOrderer");

		ResolveTransactionBatchReply& reply = proxyInfo.outstandingBatches[req.version];
		reply.writtenTags = req.writtenTags;
		reply.debugID = req.debugID;
		reply.committed.resize(reply.arena, req.transactions.size());

		std::vector<int> commitList;
		std::vector<int> tooOldList;

		// Detect conflicts
		double expire = now() + SERVER_KNOBS->SAMPLE_EXPIRATION_TIME;
		ConflictBatch conflictBatch(self->conflictSet, &reply.conflictingKeyRangeMap, &reply.arena);
		const Version newOldestVersion = req.version - SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS;
		for (int t = 0; t < req.transactions.size(); t++) {
			conflictBatch.addTransaction(req.transactions[t], newOldestVersion);
			self->resolvedReadConflictRanges += req.transactions[t].read_conflict_ranges.size();
			self->resolvedWriteConflictRanges += req.transactions[t].write_conflict_ranges.size();

			if (self->resolverCount > 1) {
				for (auto it : req.transactions[t].write_conflict_ranges)
					self->iopsSample.addAndExpire(
					    it.begin, SERVER_KNOBS->SAMPLE_OFFSET_PER_KEY + it.begin.size(), expire);
				for (auto it : req.transactions[t].read_conflict_ranges)
					self->iopsSample.addAndExpire(
					    it.begin, SERVER_KNOBS->SAMPLE_OFFSET_PER_KEY + it.begin.size(), expire);
			}
		}
		conflictBatch.detectConflicts(req.version, newOldestVersion, commitList, &tooOldList);

		for (int c = 0; c < commitList.size(); c++)
			reply.committed[commitList[c]] = ConflictBatch::TransactionCommitted;

		for (int c = 0; c < tooOldList.size(); c++) {
			ASSERT(reply.committed[tooOldList[c]] == ConflictBatch::TransactionConflict);
			reply.committed[tooOldList[c]] = ConflictBatch::TransactionTooOld;
		}

		ASSERT(req.prevVersion >= 0 ||
		       req.txnStateTransactions.size() == 0); // The master's request should not have any state transactions

		// batch and apply state transactions
		auto committedCount = commitList.size();
		auto& stateTransactionsPair = self->recentStateTransactionsInfo.getStateTransactionsRef(req.version);
		auto& stateTransactions = stateTransactionsPair.second;
		int64_t stateMutations = 0;
		int64_t stateBytes = 0;
		int64_t stateNormalMutationBytes = 0;
		int64_t stateNormalMutations = 0;
		std::unique_ptr<LogPushData> toCommit(nullptr); // For accumulating private mutations
		std::unique_ptr<ResolverData> resolverData(nullptr);
		// We don't want to cache more state transactions, so we will reject state transactions in this batch
		bool rejectStateTransaction = self->totalStateBytes.get() > SERVER_KNOBS->RESOLVER_STATE_MEMORY_LIMIT;

		for (int t : req.txnStateTransactions) {
			bool committed = reply.committed[t] == ConflictBatch::TransactionCommitted;

			// reject only make a difference when normalKeyMutations isn't empty
			if (rejectStateTransaction && committed && !req.transactions[t].normalKeyMutations.empty()) {
				CODE_PROBE(true, "Reject State Mutation Transaction");
				reply.committed[t] = ConflictBatch::TransactionConflict;
				committed = false;
				committedCount--;
			}

			stateMutations += req.transactions[t].mutations.size();
			stateBytes += req.transactions[t].mutations.expectedSize();
			// TODO: can we pass empty mutations & tenantIds back when the committed==false?
			stateTransactions.push_back_deep(
			    stateTransactions.arena(),
			    StateTransactionRef(committed,
			                        req.transactions[t].mutations,
			                        req.transactions[t].tenantIds,
			                        committed ? req.transactions[t].normalKeyMutations : VectorRef<MutationRef>()));

			if (committed) {
				stateNormalMutations += req.transactions[t].normalKeyMutations.size();
				stateNormalMutationBytes += req.transactions[t].normalKeyMutations.expectedSize();
			}
			// for (const auto& m : req.transactions[t].mutations)
			//	DEBUG_MUTATION("Resolver", req.version, m, self->dbgid);
			CODE_PROBE(self->forceRecovery, "Resolver detects forced recovery");
		}

		// update resolver counters
		self->transactionsAccepted += committedCount;
		self->transactionsTooOld += tooOldList.size();
		self->transactionsConflicted += req.transactions.size() - committedCount - tooOldList.size();
		self->resolvedStateTransactions += req.txnStateTransactions.size();
		self->resolvedStateMutations += stateMutations;
		self->resolvedStateBytes += stateBytes;
		self->resolvedStateNormalMutationBytes += stateNormalMutationBytes;
		self->resolvedStateNormalMutations += stateNormalMutations;

		self->recentStateTransactionsInfo.addVersionBytes(req.version, stateBytes + stateNormalMutationBytes);

		if (stateNormalMutationBytes > FLOW_KNOBS->PACKET_WARNING) {
			TraceEvent(SevWarn, "LargeNormalMutationBytes")
			    .suppressFor(10.0)
			    .detail("StateRefVecBytes", stateTransactions.expectedSize())
			    .detail("Count", stateTransactions.size())
			    .detail("NormalKeyBytes", stateNormalMutationBytes)
			    .detail("NormalKeyCount", stateNormalMutations)
			    .detail("Version", req.version);
		}

		ASSERT(req.version >= firstUnseenVersion);
		ASSERT(firstUnseenVersion >= self->debugMinRecentStateVersion);

		CODE_PROBE(firstUnseenVersion == req.version, "Resolver first unseen version is current version");

		self->recentStateTransactionsInfo.applyStateTxnsToBatchReply(&reply, firstUnseenVersion, req.version);

		//TraceEvent("ResolveBatch", self->dbgid).detail("PrevVersion", req.prevVersion).detail("Version", req.version).detail("StateTransactionVersions", self->recentStateTransactionsInfo.size()).detail("StateBytes", stateBytes).detail("FirstVersion", self->recentStateTransactionsInfo.empty() ? -1 : self->recentStateTransactionsInfo.firstVersion()).detail("StateMutationsIn", req.txnStateTransactions.size()).detail("StateMutationsOut", reply.stateMutations.size()).detail("From", proxyAddress);

		ASSERT(!proxyInfo.outstandingBatches.empty());
		ASSERT(self->proxyInfoMap.size() <= self->commitProxyCount + 1);

		// SOMEDAY: This is O(n) in number of proxies. O(log n) solution using appropriate data structure?
		Version oldestProxyVersion = req.version;
		for (auto itr = self->proxyInfoMap.begin(); itr != self->proxyInfoMap.end(); ++itr) {
			//TraceEvent("ResolveBatchProxyVersion", self->dbgid).detail("CommitProxy", itr->first).detail("Version", itr->second.lastVersion);
			if (itr->first.isValid()) { // Don't consider the first master request
				oldestProxyVersion = std::min(itr->second.lastVersion, oldestProxyVersion);
			} else {
				// The master's request version should never prevent us from clearing recentStateTransactions
				ASSERT(self->debugMinRecentStateVersion == 0 ||
				       self->debugMinRecentStateVersion > itr->second.lastVersion);
			}
		}

		CODE_PROBE(oldestProxyVersion == req.version,
		           "The proxy that sent this request has the oldest current version");
		CODE_PROBE(oldestProxyVersion != req.version,
		           "The proxy that sent this request does not have the oldest current version");

		// clear cache of old state transactions
		bool anyPopped = false;
		if (firstUnseenVersion <= oldestProxyVersion && self->proxyInfoMap.size() == self->commitProxyCount + 1) {
			CODE_PROBE(true, "Deleting old state transactions");
			int64_t erasedBytes = self->recentStateTransactionsInfo.eraseUpTo(oldestProxyVersion);
			self->debugMinRecentStateVersion = oldestProxyVersion + 1;
			anyPopped = erasedBytes > 0;
			stateBytes -= erasedBytes;
		}

		self->version.set(req.version);
		bool breachedLimit = self->totalStateBytes.get() <= SERVER_KNOBS->RESOLVER_STATE_MEMORY_LIMIT &&
		                     self->totalStateBytes.get() + stateBytes + stateNormalMutationBytes >
		                         SERVER_KNOBS->RESOLVER_STATE_MEMORY_LIMIT;
		self->totalStateBytes.setUnconditional(self->totalStateBytes.get() + stateBytes + stateNormalMutationBytes);
		if (anyPopped || breachedLimit) {
			self->checkNeededVersion.trigger();
		}

		// Measure the time spent doing actual work in the resolver.
		const double endComputeTime = timer();
		self->computeTimeDist->sampleSeconds(endComputeTime - beginComputeTime);

		if (req.debugID.present())
			g_traceBatch.addEvent("CommitDebug", debugID.get().first(), "Resolver.resolveBatch.After");
	} else {
		CODE_PROBE(true, "Duplicate resolve batch request");
		//TraceEvent("DupResolveBatchReq", self->dbgid).detail("From", proxyAddress);
	}

	auto proxyInfoItr = self->proxyInfoMap.find(proxyAddress);

	if (proxyInfoItr != self->proxyInfoMap.end()) {
		auto batchItr = proxyInfoItr->second.outstandingBatches.find(req.version);
		if (batchItr != proxyInfoItr->second.outstandingBatches.end()) {
			req.reply.send(batchItr->second);
		} else {
			CODE_PROBE(true, "No outstanding batches for version on proxy");
			req.reply.send(Never());
		}
	} else {
		ASSERT_WE_THINK(false); // The first non-duplicate request with this proxyAddress, including this one, should
		                        // have inserted this item in the map!
		// CODE_PROBE(true, "No prior proxy requests");
		req.reply.send(Never());
	}

	// Measure server-side RPC latency from the time a request was
	// received to time the response was sent.
	const double endTime = timer();
	self->resolverLatencyDist->sampleSeconds(endTime - req.requestTime());

	++self->resolveBatchOut;

	return Void();
}

ACTOR Future<Void> resolverCore(ResolverInterface resolver,
                                InitializeResolverRequest initReq,
                                Reference<AsyncVar<ServerDBInfo> const> db) {
	state Reference<Resolver> self(
	    new Resolver(resolver.id(), initReq.commitProxyCount, initReq.resolverCount, initReq.encryptMode));
	state ActorCollection actors(false);
	state Future<Void> doPollMetrics = self->resolverCount > 1 ? Void() : Future<Void>(Never());
	state PromiseStream<Future<Void>> addActor;
	actors.add(waitFailureServer(resolver.waitFailure.getFuture()));
	actors.add(traceRole(Role::RESOLVER, resolver.id()));

	TraceEvent("ResolverInit", resolver.id())
	    .detail("RecoveryCount", initReq.recoveryCount)
	    .detail("EncryptMode", initReq.encryptMode.toString());

	// Wait until we can load the "real" logsystem, since we don't support switching them currently
	while (!(initReq.masterLifetime.isEqual(db->get().masterLifetime) &&
	         db->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION)) {
		// TraceEvent("ResolverInit2", resolver.id()).detail("LSEpoch", db->get().logSystemConfig.epoch);
		wait(db->onChange());
	}

	// Initialize txnStateStore
	self->logSystem = ILogSystem::fromServerDBInfo(resolver.id(), db->get(), false, addActor);
	self->localTLogCount = db->get().logSystemConfig.numLogs();
	state Future<Void> onError =
	    transformError(actorCollection(addActor.getFuture()), broken_promise(), resolver_failed());

	loop choose {
		when(ResolveTransactionBatchRequest batch = waitNext(resolver.resolve.getFuture())) {
			actors.add(resolveBatch(self, batch, db));
		}
		when(ResolutionMetricsRequest req = waitNext(resolver.metrics.getFuture())) {
			++self->metricsRequests;
			req.reply.send(self->iopsSample.getEstimate(allKeys));
		}
		when(ResolutionSplitRequest req = waitNext(resolver.split.getFuture())) {
			++self->splitRequests;
			ResolutionSplitReply rep;
			rep.key = self->iopsSample.splitEstimate(req.range, req.offset, req.front);
			rep.used = self->iopsSample.getEstimate(req.front ? KeyRangeRef(req.range.begin, rep.key)
			                                                  : KeyRangeRef(rep.key, req.range.end));
			req.reply.send(rep);
		}
		when(wait(actors.getResult())) {}
		when(wait(onError)) {}
		when(wait(doPollMetrics)) {
			self->iopsSample.poll();
			doPollMetrics = delay(SERVER_KNOBS->SAMPLE_POLL_TIME);
		}
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo> const> db,
                                uint64_t recoveryCount,
                                ResolverInterface myInterface) {
	loop {
		if (db->get().recoveryCount >= recoveryCount &&
		    !std::count(db->get().resolvers.begin(), db->get().resolvers.end(), myInterface))
			throw worker_removed();
		wait(db->onChange());
	}
}

ACTOR Future<Void> resolver(ResolverInterface resolver,
                            InitializeResolverRequest initReq,
                            Reference<AsyncVar<ServerDBInfo> const> db) {
	try {
		state Future<Void> core = resolverCore(resolver, initReq, db);
		loop choose {
			when(wait(core)) {
				return Void();
			}
			when(wait(checkRemoved(db, initReq.recoveryCount, resolver))) {}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled || e.code() == error_code_worker_removed) {
			TraceEvent("ResolverTerminated", resolver.id()).errorUnsuppressed(e);
			return Void();
		}
		throw;
	}
}
