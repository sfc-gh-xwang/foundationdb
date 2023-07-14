/*
 * QuotaThrottler.actor.cpp
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/ThrottlingId.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbserver/IRKThroughputTracker.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/QuotaThrottler.h"

#include <boost/unordered/unordered_flat_map.hpp>

// In the function names below, several terms are used repeatedly. The context-specific are defined here:
//
// Cost: Every read or write operation has an associated cost, determined by the number of bytes accessed.
//       Throttling quotas are specified in terms of the amount of this cost that can be consumed
//       per second. In the quota throttler, cost refers to the per second rate of bytes accessed.
//
// TPS: Transactions per second. Quotas are not specified in terms of TPS, but the limits given to clients must
//      be specified in terms of TPS because throttling is performed at the front end of transactions (before costs are
//      known).
//
// Total: Refers to the total quota specified by clients through the quota throttling API. The sum of the
//        costs of all operations (cluster-wide) with a particular throttling ID cannot exceed the throttling ID's
//        specified total quota, even if the cluster has no saturated processes.
//
// Desired TPS: Assuming that a throttling ID is able to achieve its total quota, this is the TPS it would be able to
// perform.
//
// Reserved: Refers to the reserved quota specified by clients through the quota throttling API. As long as the
//           sum of the costs of all operations (cluster-wide) with a particular throttling ID is not above the
//           throttling ID's specified reserved quota, the throttling ID should not experience any throttling
//           from the quota throttler.
//
// Current [Cost|TPS]: Measuring the current throughput on the cluster, independent of any specified quotas.
//
// ThrottlingRatio: Based on the health of each storage server, a throttling ratio is calculated,
//                  informing the quota throttler what ratio of the current throughput can be maintained.
//
// Limiting [Cost|TPS]: Based on the health of storage servers, a limiting throughput may be enforced.
//
// Target [Cost|TPS]: Based on reserved, limiting, and desired throughputs, this is the target throughput
//                    that the global tag throttler aims to achieve (across all clients).

class QuotaThrottlerImpl {
	static Optional<double> getMin(Optional<double> a, Optional<double> b) {
		if (a.present() && b.present()) {
			return std::min(a.get(), b.get());
		} else if (a.present()) {
			return a;
		} else {
			return b;
		}
	}

	static Optional<double> getMax(Optional<double> a, Optional<double> b) {
		if (a.present() && b.present()) {
			return std::max(a.get(), b.get());
		} else if (a.present()) {
			return a;
		} else {
			return b;
		}
	}

	// Track various statistics per throttling ID, aggregated across all storage servers
	class PerThrottlingIdStatistics {
		HoltLinearSmoother transactionCounter;
		Smoother targetRate;
		double transactionsLastAdded;
		double lastLogged;

	public:
		explicit PerThrottlingIdStatistics()
		  : transactionCounter(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRANSACTION_COUNT_FOLDING_TIME,
		                       SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRANSACTION_RATE_FOLDING_TIME),
		    targetRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TARGET_RATE_FOLDING_TIME),
		    transactionsLastAdded(now()),
		    lastLogged(0) {}

		void addTransactions(int count) {
			transactionsLastAdded = now();
			transactionCounter.addDelta(count);
		}

		double getTransactionRate() const { return transactionCounter.smoothRate(); }

		double updateAndGetTargetLimit(double targetTps) {
			targetRate.setTotal(targetTps);
			return targetRate.smoothTotal();
		}

		bool recentTransactionsAdded() const {
			return now() - transactionsLastAdded < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER;
		}

		bool canLog() const { return now() - lastLogged > SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRACE_INTERVAL; }

		void updateLastLogged() { lastLogged = now(); }
	};

	UID id;
	int maxFallingBehind{ 0 };
	double limitingThreshold{ 0.0 };
	uint32_t lastBusyThrottlingIdCount{ 0 };

	ThrottlingIdMap<PerThrottlingIdStatistics> throttlingStatistics;
	ServerThroughputTracker serverThroughputTracker;
	Optional<ClientThroughputTracker> clientThroughputTracker;

	IRKThroughputTracker const& throughputTracker() const& {
		if (clientThroughputTracker.present()) {
			return clientThroughputTracker.get();
		} else {
			return serverThroughputTracker;
		}
	}

	Optional<double> getTransactionRate(ThrottlingId const& throttlingId) const {
		auto it = throttlingStatistics.find(throttlingId);
		if (it == throttlingStatistics.end()) {
			return {};
		}
		return it->second.getTransactionRate();
	}

	// For transactions with the provided throttling ID, returns the average cost of all transactions
	// accross the cluster. The minimum cost is one page. If the transaction rate is too low,
	// return an empty Optional, because no accurate estimation can be made.
	Optional<double> getAverageTransactionCost(ThrottlingId throttlingId, TraceEvent& te) const {
		auto const cost = throughputTracker().getThroughput(throttlingId);
		auto const transactionRate = getTransactionRate(throttlingId);
		if (!transactionRate.present()) {
			return CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
		}
		te.detail("TransactionRate", transactionRate.get());
		te.detail("Cost", cost);
		if (transactionRate < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MIN_RATE) {
			return {};
		} else {
			return std::max(static_cast<double>(CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE), cost / transactionRate.get());
		}
	}

	// Returns the limiting transaction rate for the provided throttlingId.
	// The throttling ratios from the worst maxFallingBehind zones are
	// ignored, because we do not want non-workload related issues (e.g. slow disks)
	// to affect tag throttling. If more than maxFallingBehind zones are at
	// or near saturation, this indicates that throttling should take place.
	Optional<double> getLimitingTps(ThrottlingId const& throttlingId, IRKMetricsTracker const& metricsTracker) const {
		auto const transactionRate = getTransactionRate(throttlingId);
		if (!transactionRate.present()) {
			return {};
		}

		std::unordered_map<Optional<Standalone<StringRef>>, double> zoneThrottlingRatios;
		auto const& storageQueueInfo = metricsTracker.getStorageQueueInfo();
		for (auto const& [id, ssInfo] : storageQueueInfo) {
			// Ignore storage servers where throttlingId is not significantly contributing
			auto const throughput = serverThroughputTracker.getThroughput(id, throttlingId);
			if (!throughput.present() || throughput.get() < limitingThreshold) {
				continue;
			}

			auto const throttlingRatio =
			    ssInfo.getTagThrottlingRatio(SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES,
			                                 SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_BYTES_STORAGE_SERVER);
			if (throttlingRatio.present()) {
				auto const [it, inserted] =
				    zoneThrottlingRatios.try_emplace(ssInfo.locality.zoneId(), throttlingRatio.get());
				if (!inserted) {
					auto& zoneThrottlingRatio = it->second;
					zoneThrottlingRatio = std::min<double>(zoneThrottlingRatio, throttlingRatio.get());
				}
			}
		}
		if (zoneThrottlingRatios.size() <= maxFallingBehind) {
			return {};
		} else {
			// max heap:
			std::priority_queue<double, std::vector<double>> pq;
			ASSERT_GE(maxFallingBehind, 0);
			for (auto const& [_, throttlingRatio] : zoneThrottlingRatios) {
				if (pq.size() < maxFallingBehind + 1) {
					pq.push(throttlingRatio);
				} else if (pq.top() > throttlingRatio) {
					pq.pop();
					pq.push(throttlingRatio);
				}
			}
			return transactionRate.get() * pq.top();
		}
	}

	struct TargetTpsResult {
		Optional<double> targetTps;
		bool isBusy{ false };
	};

	TargetTpsResult getTargetTps(ThrottlingId throttlingId,
	                             IRKThroughputQuotaCache const& quotaCache,
	                             IRKMetricsTracker const& metricsTracker,
	                             TraceEvent& te) {
		TargetTpsResult result;
		auto const limitingTps = getLimitingTps(throttlingId, metricsTracker);
		auto const averageTransactionCost = getAverageTransactionCost(throttlingId, te);
		auto const totalQuota = quotaCache.getTotalQuota(throttlingId);
		auto const reservedQuota = quotaCache.getReservedQuota(throttlingId);
		if (!averageTransactionCost.present() || !totalQuota.present() || !reservedQuota.present()) {
			return result;
		}
		auto const desiredTps = totalQuota.get() / averageTransactionCost.get();
		auto const reservedTps = reservedQuota.get() / averageTransactionCost.get();

		result.targetTps = getMax(reservedTps, getMin(desiredTps, limitingTps));
		result.isBusy = limitingTps.present() && limitingTps.get() < desiredTps;

		te.detail("ThrottlingId", throttlingId)
		    .detail("TargetTps", result.targetTps)
		    .detail("AverageTransactionCost", averageTransactionCost)
		    .detail("LimitingTps", limitingTps)
		    .detail("ReservedTps", reservedTps)
		    .detail("DesiredTps", desiredTps)
		    .detail("NumStorageServers", serverThroughputTracker.storageServersTracked())
		    .detail("TotalQuota", totalQuota)
		    .detail("ReservedQuota", reservedQuota);

		return result;
	}

public:
	QuotaThrottlerImpl(UID id, int maxFallingBehind, double limitingThreshold)
	  : id(id),
	    maxFallingBehind(maxFallingBehind),
	    limitingThreshold(limitingThreshold) {
		if (CLIENT_KNOBS->TRACK_THROUGHPUT_ON_CLIENTS) {
			clientThroughputTracker = ClientThroughputTracker();
		}
	}

	void addRequests(ThrottlingId throttlingId, int count) {
		auto it = throttlingStatistics.find(throttlingId);
		if (it == throttlingStatistics.end()) {
			if (throttlingStatistics.size() == SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED) {
				CODE_PROBE(
				    true,
				    "Quota throttler ignoring transactions because maximum number of trackable throttling IDs has "
				    "been reached");
				TraceEvent("QuotaThrottler_IgnoringRequests")
				    .suppressFor(60.0)
				    .detail("ThrottlingId", throttlingId)
				    .detail("Count", count);
			} else {
				throttlingStatistics[throttlingId].addTransactions(static_cast<double>(count));
			}
		} else {
			it->second.addTransactions(static_cast<double>(count));
		}
	}

	ThrottlingIdMap<double> getProxyRates(int numProxies,
	                                      IRKThroughputQuotaCache const& quotaCache,
	                                      IRKMetricsTracker const& metricsTracker) {
		ThrottlingIdMap<double> result;
		lastBusyThrottlingIdCount = 0;

		for (auto& [throttlingId, stats] : throttlingStatistics) {
			// Currently there is no differentiation between batch priority and default priority transactions
			TraceEvent te("QuotaThrottler_GotRate", id);
			bool const traceEnabled = stats.canLog();
			if (!traceEnabled) {
				te.disable();
			}
			auto const [targetTps, isBusy] = getTargetTps(throttlingId, quotaCache, metricsTracker, te);
			if (isBusy) {
				++lastBusyThrottlingIdCount;
			}
			if (targetTps.present()) {
				auto const smoothedTargetTps = stats.updateAndGetTargetLimit(targetTps.get());
				te.detail("SmoothedTargetTps", smoothedTargetTps).detail("NumProxies", numProxies);
				result[throttlingId] = std::max(1.0, smoothedTargetTps / numProxies);
				if (traceEnabled) {
					stats.updateLastLogged();
				}
			} else {
				te.disable();
			}
		}

		return result;
	}

	void updateThroughput(ThrottlingIdMap<uint64_t> const& newThroughput) {
		if (clientThroughputTracker.present()) {
			clientThroughputTracker.get().update(newThroughput);
		}
	}

	int64_t throttleCount() const { return lastBusyThrottlingIdCount; }
	int64_t manualThrottleCount() const { return 0; }

	void updateThrottling(Map<UID, StorageQueueInfo> const& sqInfos) { serverThroughputTracker.update(sqInfos); }

	void removeExpiredThrottlingIds() {
		boost::unordered::erase_if(throttlingStatistics,
		                           [](auto const& kv) { return !kv.second.recentTransactionsAdded(); });
	}

	Future<Void> removeExpiredThrottlingIdsActor() {
		return recurring([this] { removeExpiredThrottlingIds(); }, 5.0);
	}

	uint32_t throttlingIdsTracked() const { return throttlingStatistics.size(); }
};

QuotaThrottler::QuotaThrottler(UID id, int maxFallingBehind, double limitingThreshold)
  : impl(PImpl<QuotaThrottlerImpl>::create(id, maxFallingBehind, limitingThreshold)) {}

QuotaThrottler::~QuotaThrottler() = default;

Future<Void> QuotaThrottler::monitorThrottlingChanges() {
	return impl->removeExpiredThrottlingIdsActor();
}

void QuotaThrottler::addRequests(ThrottlingId throttlingId, int count) {
	return impl->addRequests(throttlingId, count);
}
ThrottlingIdMap<double> QuotaThrottler::getProxyRates(int numProxies,
                                                      IRKThroughputQuotaCache const& quotaCache,
                                                      IRKMetricsTracker const& metricsTracker) {
	return impl->getProxyRates(numProxies, quotaCache, metricsTracker);
}
int64_t QuotaThrottler::throttleCount() const {
	return impl->throttleCount();
}
void QuotaThrottler::updateThrottling(Map<UID, StorageQueueInfo> const& sqInfos) {
	return impl->updateThrottling(sqInfos);
}

void QuotaThrottler::updateThroughput(ThrottlingIdMap<uint64_t> const& newThroughput) {
	return impl->updateThroughput(newThroughput);
}

uint32_t QuotaThrottler::throttlingIdsTracked() const {
	return impl->throttlingIdsTracked();
}

void QuotaThrottler::removeExpiredThrottlingIds() {
	return impl->removeExpiredThrottlingIds();
}
