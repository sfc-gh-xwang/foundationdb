/**
 * ReadVersionBatchers.actor.cpp
 */

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadVersionBatchers.h"
#include "fdbclient/ThroughputTracker.h"
#include "flow/actorcompiler.h" // must be last include

ACTOR static Future<GetReadVersionReply> getConsistentReadVersion(SpanContext parentSpan,
                                                                  DatabaseContext* cx,
                                                                  uint32_t transactionCount,
                                                                  TransactionPriority priority,
                                                                  uint32_t flags,
                                                                  Optional<ThrottlingId> throttlingId,
                                                                  Optional<UID> debugID,
                                                                  uint64_t expectedCosts,
                                                                  uint64_t expectedCostsCount) {
	state Span span("NAPI:getConsistentReadVersion"_loc, parentSpan);

	++cx->transactionReadVersionBatches;
	if (debugID.present())
		g_traceBatch.addEvent("TransactionDebug", debugID.get().first(), "NativeAPI.getConsistentReadVersion.Before");
	loop {
		try {
			state Optional<double> expectedTotalCost = {};
			if (throttlingId.present() && isClientAverageCostTrackingEnabled()) {
				cx->addTransaction(throttlingId.get(), transactionCount);

				auto averageCost = cx->getAverageTransactionCost(throttlingId.get());

				expectedTotalCost = averageCost * (transactionCount - expectedCostsCount) + expectedCosts;
			}

			state GetReadVersionRequest req(span.context,
			                                transactionCount,
			                                priority,
			                                cx->ssVersionVectorCache.getMaxVersion(),
			                                flags,
			                                throttlingId,
			                                debugID,
			                                expectedTotalCost);
			state Future<Void> onProxiesChanged = cx->onProxiesChanged();

			choose {
				when(wait(onProxiesChanged)) {
					onProxiesChanged = cx->onProxiesChanged();
				}
				when(GetReadVersionReply v =
				         wait(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies(
				                                   flags & GetReadVersionRequest::FLAG_USE_PROVISIONAL_PROXIES)),
				                               &GrvProxyInterface::getConsistentReadVersion,
				                               req,
				                               cx->taskID))) {
					CODE_PROBE(v.proxyTagThrottledDuration > 0.0,
					           "getConsistentReadVersion received GetReadVersionReply delayed by proxy tag throttling");

					if (debugID.present())
						g_traceBatch.addEvent(
						    "TransactionDebug", debugID.get().first(), "NativeAPI.getConsistentReadVersion.After");
					ASSERT(v.version > 0);
					cx->minAcceptableReadVersion = std::min(cx->minAcceptableReadVersion, v.version);
					if (cx->versionVectorCacheActive(v.ssVersionVectorDelta)) {
						if (cx->isCurrentGrvProxy(v.proxyId)) {
							cx->ssVersionVectorCache.applyDelta(v.ssVersionVectorDelta);
						} else {
							continue; // stale GRV reply, retry
						}
					}
					return v;
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_broken_promise && e.code() != error_code_batch_transaction_throttled &&
			    e.code() != error_code_grv_proxy_memory_limit_exceeded && e.code() != error_code_proxy_tag_throttled)
				TraceEvent(SevError, "GetConsistentReadVersionError").error(e);
			if (e.code() == error_code_batch_transaction_throttled && !cx->apiVersionAtLeast(630)) {
				wait(delayJittered(5.0));
			} else if (e.code() == error_code_grv_proxy_memory_limit_exceeded) {
				// FIXME(xwang): the better way is to let this error broadcast to transaction.onError(e), otherwise
				// the txn->cx counter doesn't make sense
				wait(delayJittered(CLIENT_KNOBS->GRV_ERROR_RETRY_DELAY));
			} else {
				throw;
			}
		}
	}
}

class Batcher {
	struct VersionRequest {
		SpanContext spanContext;
		Promise<GetReadVersionReply> reply;
		Optional<UID> debugID;
		Optional<uint64_t> expectedCost;

		VersionRequest(SpanContext spanContext, Optional<UID> debugID = {}, Optional<uint64_t> expectedCost = {})
		  : spanContext(spanContext),
		    debugID(debugID),
		    expectedCost(expectedCost) {}
	};

	int outstandingRequests = 0;
	PromiseStream<VersionRequest> stream;
	Future<Void> actor;
	double lastRequestTime = now();

	ACTOR static Future<Void> readVersionBatcher(Batcher* self,
	                                             DatabaseContext* cx,
	                                             TransactionPriority priority,
	                                             uint32_t flags,
	                                             Optional<ThrottlingId> throttlingId) {
		state std::vector<Promise<GetReadVersionReply>> requests;

		// The total transaction cost, provided by the client as an optional API.
		// If the client does not specify an expected cost, the average cost will be utilized instead.
		state uint64_t expectedCost = 0;
		// How many transactions provided the expected cost.
		state uint expectedCostCount = 0;

		state PromiseStream<Future<Void>> addActor;
		state Future<Void> collection = actorCollection(addActor.getFuture(), &self->outstandingRequests);
		state Future<Void> timeout;
		state Optional<UID> debugID;
		state bool send_batch;
		state Reference<Histogram> batchSizeDist = Histogram::getHistogram("GrvBatcher"_sr,
		                                                                   "ClientGrvBatchSize"_sr,
		                                                                   Histogram::Unit::countLinear,
		                                                                   0,
		                                                                   CLIENT_KNOBS->MAX_BATCH_SIZE * 2);
		state Reference<Histogram> batchIntervalDist =
		    Histogram::getHistogram("GrvBatcher"_sr,
		                            "ClientGrvBatchInterval"_sr,
		                            Histogram::Unit::milliseconds,
		                            0,
		                            CLIENT_KNOBS->GRV_BATCH_TIMEOUT * 1000000 * 2);
		state Reference<Histogram> grvReplyLatencyDist =
		    Histogram::getHistogram("GrvBatcher"_sr, "ClientGrvReplyLatency"_sr, Histogram::Unit::milliseconds);
		state double lastRequestTime = now();

		// dynamic batching
		state PromiseStream<double> replyTimes;
		state double batchTime = 0;
		state Span span("NAPI:readVersionBatcher"_loc);
		loop {
			send_batch = false;
			choose {
				when(VersionRequest req = waitNext(self->stream.getFuture())) {
					if (req.debugID.present()) {
						if (!debugID.present()) {
							debugID = nondeterministicRandom()->randomUniqueID();
						}
						g_traceBatch.addAttach("TransactionAttachID", req.debugID.get().first(), debugID.get().first());
					}
					span.addLink(req.spanContext);
					requests.push_back(req.reply);

					if (req.expectedCost.present()) {
						expectedCost += req.expectedCost.get();
						expectedCostCount++;
					}

					if (requests.size() == CLIENT_KNOBS->MAX_BATCH_SIZE) {
						send_batch = true;
						++cx->transactionGrvFullBatches;
					} else if (!timeout.isValid()) {
						timeout = delay(batchTime, TaskPriority::GetConsistentReadVersion);
					}
				}
				when(wait(timeout.isValid() ? timeout : Never())) {
					send_batch = true;
					++cx->transactionGrvTimedOutBatches;
				}
				// dynamic batching monitors reply latencies
				when(double reply_latency = waitNext(replyTimes.getFuture())) {
					double target_latency = reply_latency * 0.5;
					batchTime = std::min(0.1 * target_latency + 0.9 * batchTime, CLIENT_KNOBS->GRV_BATCH_TIMEOUT);
					grvReplyLatencyDist->sampleSeconds(reply_latency);
				}
				when(wait(collection)) {} // for errors
			}
			if (send_batch) {
				int count = requests.size();
				ASSERT(count);

				batchSizeDist->sampleRecordCounter(count);
				auto requestTime = now();
				batchIntervalDist->sampleSeconds(requestTime - lastRequestTime);
				lastRequestTime = requestTime;

				// dynamic batching
				Promise<GetReadVersionReply> GRVReply;
				requests.push_back(GRVReply);
				addActor.send(ready(timeReply(GRVReply.getFuture(), replyTimes)));

				Future<Void> batch = incrementalBroadcastWithError(getConsistentReadVersion(span.context,
				                                                                            cx,
				                                                                            count,
				                                                                            priority,
				                                                                            flags,
				                                                                            throttlingId,
				                                                                            std::move(debugID),
				                                                                            expectedCost,
				                                                                            expectedCostCount),
				                                                   std::move(requests),
				                                                   CLIENT_KNOBS->BROADCAST_BATCH_SIZE);

				span = Span("NAPI:readVersionBatcher"_loc);
				debugID = Optional<UID>();
				requests.clear();
				expectedCost = 0;
				expectedCostCount = 0;
				addActor.send(batch);
				timeout = Future<Void>();
			}
		}
	}

public:
	Future<GetReadVersionReply> getReadVersion(SpanContext spanContext,
	                                           Optional<UID> debugId,
	                                           Optional<uint64_t> const& expectedCost) {
		lastRequestTime = now();
		auto const req = VersionRequest(spanContext, debugId, expectedCost);
		stream.send(req);
		return req.reply.getFuture();
	}

	void startActor(Database cx,
	                TransactionPriority priority,
	                uint32_t flags,
	                Optional<ThrottlingId> const& throttlingId) {
		if (!actor.isValid()) {
			actor = readVersionBatcher(this, cx.getPtr(), priority, flags, throttlingId);
		}
	}

	bool isActive(double expirationTimeout) const {
		return (now() - lastRequestTime < expirationTimeout) || (outstandingRequests > 0);
	}
};

class Index {
	uint32_t flags;
	Optional<ThrottlingId> throttlingId;

public:
	Index(uint32_t flags, Optional<ThrottlingId> const& throttlingId) : flags(flags), throttlingId(throttlingId) {}
	bool operator==(Index const& rhs) const { return (flags == rhs.flags) && (throttlingId == rhs.throttlingId); }
	size_t hash() const {
		size_t result = 0;
		boost::hash_combine(result, flags);
		boost::hash_combine(result, throttlingId);
		return result;
	}
};

struct HashIndex {
	size_t operator()(Index const& index) const { return index.hash(); }
};

class ReadVersionBatchersImpl {
	std::unordered_map<Index, Batcher, HashIndex> batchers;
	int capacity;
	Future<Void> cleaner;

	ACTOR static Future<Void> cleanerActor(ReadVersionBatchersImpl* self,
	                                       double expirationTimeout,
	                                       double expirationCheckInterval) {
		loop {
			wait(delay(expirationCheckInterval));
			auto const _expirationTimeout = expirationTimeout;
			std::erase_if(self->batchers, [_expirationTimeout](auto const& it) {
				auto const& [_, batcher] = it;
				CODE_PROBE(!batcher.isActive(_expirationTimeout), "Expiring GRV batcher");
				return !batcher.isActive(_expirationTimeout);
			});
		}
	}

public:
	ReadVersionBatchersImpl(int capacity, double expirationTimeout, double expirationCheckInterval)
	  : capacity(capacity) {
		cleaner = cleanerActor(this, expirationTimeout, expirationCheckInterval);
	}

	Future<GetReadVersionReply> getReadVersion(Database cx,
	                                           TransactionPriority priority,
	                                           uint32_t flags,
	                                           Optional<TenantGroupName> const& tenantGroup,
	                                           SpanContext spanContext,
	                                           Optional<TransactionTag> tag,
	                                           Optional<UID> debugID,
	                                           Optional<uint64_t> const& expectedCost) {
		Optional<ThrottlingId> throttlingId;
		if (tenantGroup.present()) {
			throttlingId = ThrottlingIdRef::fromTenantGroup(tenantGroup.get());
		} else if (tag.present()) {
			throttlingId = ThrottlingIdRef::fromTag(tag.get());
		}
		Index index(flags, throttlingId);
		auto it = batchers.find(index);
		if (it == batchers.end()) {
			if (batchers.size() == capacity) {
				CODE_PROBE(true, "Too many GRV batchers");
				throw too_many_grv_batchers();
			}
			it = batchers.try_emplace(index).first;
		}
		auto& batcher = it->second;
		batcher.startActor(cx, priority, flags, throttlingId);
		return batcher.getReadVersion(spanContext, debugID, expectedCost);
	}
};

Future<GetReadVersionReply> ReadVersionBatchers::getReadVersion(Database cx,
                                                                TransactionPriority priority,
                                                                uint32_t flags,
                                                                Optional<TenantGroupName> const& tenantGroup,
                                                                SpanContext context,
                                                                Optional<TransactionTag> const& tag,
                                                                Optional<UID> debugID,
                                                                Optional<uint64_t> const& expectedCost) {
	return impl->getReadVersion(cx, priority, flags, tenantGroup, context, tag, debugID, expectedCost);
}

ReadVersionBatchers::ReadVersionBatchers(int capacity, double expirationTimeout, double expirationCheckInterval)
  : impl(PImpl<ReadVersionBatchersImpl>::create(capacity, expirationTimeout, expirationCheckInterval)) {}

ReadVersionBatchers::~ReadVersionBatchers() = default;
