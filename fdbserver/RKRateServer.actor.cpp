/**
 * RKRateServer.h
 */

#include "fdbclient/ThroughputTracker.h"
#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbserver/IRKRateServer.h"
#include "fdbserver/IRKRateUpdater.h"
#include "fdbserver/IRKRecoveryTracker.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/QuotaThrottler.h"

#include "flow/actorcompiler.h" // must be last include

class RKRateServerImpl {
public:
	ACTOR static Future<Void> run(RKRateServer* self,
	                              IRKRateUpdater const* normalRateUpdater,
	                              IRKRateUpdater const* batchRateUpdater,
	                              QuotaThrottler* quotaThrottler,
	                              IRKThroughputQuotaCache const* quotaCache,
	                              IRKMetricsTracker const* metricsTracker,
	                              IRKRecoveryTracker* recoveryTracker) {
		loop {
			GetRateInfoRequest req = waitNext(self->getRateInfo);
			GetRateInfoReply reply;

			auto& p = self->grvProxyInfo[req.requesterID];
			//TraceEvent("RKMPU", req.requesterID).detail("TRT", req.totalReleasedTransactions).detail("Last", p.totalTransactions).detail("Delta", req.totalReleasedTransactions - p.totalTransactions);
			if (p.totalTransactions > 0) {
				self->smoothReleasedTransactions.addDelta(req.totalReleasedTransactions - p.totalTransactions);

				for (auto const& [throttlingId, count] : req.throttlingIdToTransactionCount) {
					quotaThrottler->addRequests(throttlingId, count);
				}
				quotaThrottler->updateThroughput(req.throttlingIdToThroughput);
			}
			if (p.batchTransactions > 0) {
				self->smoothBatchReleasedTransactions.addDelta(req.batchReleasedTransactions - p.batchTransactions);
			}

			p.totalTransactions = req.totalReleasedTransactions;
			p.batchTransactions = req.batchReleasedTransactions;
			p.version = req.version;

			recoveryTracker->updateMaxVersion(req.version);

			p.lastUpdateTime = now();

			reply.transactionRate = normalRateUpdater->getTpsLimit() / self->grvProxyInfo.size();
			reply.batchTransactionRate = batchRateUpdater->getTpsLimit() / self->grvProxyInfo.size();
			reply.leaseDuration = SERVER_KNOBS->METRIC_UPDATE_RATE;

			if (now() > p.lastTagPushTime + SERVER_KNOBS->TAG_THROTTLE_PUSH_INTERVAL) {
				p.lastTagPushTime = now();

				if (isClientAverageCostTrackingEnabled()) {
					auto targetThroughput =
					    quotaThrottler->getTargetThroughput(self->grvProxyInfo.size(), *quotaCache, *metricsTracker);
					if (!SERVER_KNOBS->GLOBAL_TAG_THROTTLING_REPORT_ONLY) {
						CODE_PROBE(targetThroughput.size() > 0, "Returning per-throttlingId TPS limits");
						reply.targetThroughput = std::move(targetThroughput);
					}
				} else {
					auto targetTransactionRate =
					    quotaThrottler->getTargetRates(self->grvProxyInfo.size(), *quotaCache, *metricsTracker);
					if (!SERVER_KNOBS->GLOBAL_TAG_THROTTLING_REPORT_ONLY) {
						CODE_PROBE(targetTransactionRate.size() > 0, "Returning per-throttlingId throughput limits");
						reply.targetTransactionRate = std::move(targetTransactionRate);
					}
				}
			}

			reply.healthMetrics.update(normalRateUpdater->getHealthMetrics(), true, req.detailed);
			reply.healthMetrics.tpsLimit = normalRateUpdater->getTpsLimit();
			reply.healthMetrics.batchLimited = self->lastLimited;

			req.reply.send(reply);
		}
	}
}; // class RKRateServerImpl

RKRateServer::RKRateServer(FutureStream<GetRateInfoRequest> getRateInfo)
  : getRateInfo(getRateInfo),
    smoothReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT),
    smoothBatchReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT) {}

RKRateServer::~RKRateServer() = default;

double RKRateServer::getSmoothReleasedTransactionRate() const {
	return smoothReleasedTransactions.smoothRate();
}

double RKRateServer::getSmoothBatchReleasedTransactionRate() const {
	return smoothBatchReleasedTransactions.smoothRate();
}

std::unordered_map<UID, RKGrvProxyInfo> const& RKRateServer::getGrvProxyInfo() const& {
	return grvProxyInfo;
}

void RKRateServer::cleanupExpiredGrvProxies() {
	double tooOld = now() - 1.0;
	for (auto p = grvProxyInfo.begin(); p != grvProxyInfo.end();) {
		if (p->second.lastUpdateTime < tooOld)
			p = grvProxyInfo.erase(p);
		else
			++p;
	}
}

void RKRateServer::updateLastLimited(double batchTpsLimit) {
	lastLimited = getSmoothReleasedTransactionRate() > SERVER_KNOBS->LAST_LIMITED_RATIO * batchTpsLimit;
}

Future<Void> RKRateServer::run(IRKRateUpdater const& normalRateUpdater,
                               IRKRateUpdater const& batchRateUpdater,
                               QuotaThrottler& quotaThrottler,
                               IRKThroughputQuotaCache const& quotaCache,
                               IRKMetricsTracker const& metricsTracker,
                               IRKRecoveryTracker& recoveryTracker) {
	return RKRateServerImpl::run(
	    this, &normalRateUpdater, &batchRateUpdater, &quotaThrottler, &quotaCache, &metricsTracker, &recoveryTracker);
}

void MockRKRateServer::updateProxy(UID proxyId, Version v, int newTotalTransactions) {
	auto& proxy = grvProxyInfo[proxyId];
	proxy.version = v;
	proxy.totalTransactions += newTotalTransactions;
	proxy.lastUpdateTime = now();
}
