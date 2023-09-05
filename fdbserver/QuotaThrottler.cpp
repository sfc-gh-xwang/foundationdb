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
#include "fdbclient/ThroughputTracker.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbserver/IRKClusterThroughputTracker.h"
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
	class TargetSmoother {
		Smoother target;
		bool isInitialized;

	public:
		TargetSmoother() : target(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TARGET_RATE_FOLDING_TIME), isInitialized(false) {}

		double updateAndGetTarget(double value) {
			if (isInitialized) {
				target.setTotal(value);
			} else {
				isInitialized = true;
				target.reset(value);
			}
			return target.smoothTotal();
		}
	};

	// Track various statistics per throttling ID, aggregated across all storage servers
	class PerThrottlingIdStatistics {
		HoltLinearSmoother transactionCounter;
		TargetSmoother targetRate;
		double transactionsLastAdded;
		double lastLogged;

	public:
		PerThrottlingIdStatistics()
		  : transactionCounter(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRANSACTION_COUNT_FOLDING_TIME,
		                       SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRANSACTION_RATE_FOLDING_TIME),
		    transactionsLastAdded(now()),
		    lastLogged(0) {}

		void addTransactions(int count) {
			transactionsLastAdded = now();
			transactionCounter.addDelta(count);
		}

		double getTransactionRate() const { return transactionCounter.smoothRate(); }

		double updateAndGetTargetRateLimit(double targetTps) { return targetRate.updateAndGetTarget(targetTps); }

		bool recentTransactionsAdded() const {
			return now() - transactionsLastAdded < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER;
		}

		bool canLog() const { return now() - lastLogged > SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRACE_INTERVAL; }

		void updateLastLogged() { lastLogged = now(); }
	};

	class ThroughputStatistics {
		TargetSmoother targetThroughput;
		double throughputLastAdded;
		double lastLogged;

	public:
		ThroughputStatistics() : throughputLastAdded(now()), lastLogged(0) {}

		double updateAndGetTargetThroughputLimit(double targetBps) {
			throughputLastAdded = now();
			return targetThroughput.updateAndGetTarget(targetBps);
		}

		bool recentTransactionsAdded() const {
			return now() - throughputLastAdded < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER;
		}

		bool canLog() const { return now() - lastLogged > SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRACE_INTERVAL; }

		void updateLastLogged() { lastLogged = now(); }
	};

	UID id;
	int maxFallingBehind{ 0 };
	double limitingThreshold{ 0.0 };
	uint32_t lastBusyThrottlingIdCount{ 0 };

	ThrottlingIdMap<PerThrottlingIdStatistics> throttlingStatistics;
	ThrottlingIdMap<ThroughputStatistics> throughputStatistics;

	boost::unordered::unordered_flat_set<ThrottlingId> busyThrottlingIds;

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
	Optional<double> getAverageTransactionCost(ThrottlingId const& throttlingId,
	                                           IRKClusterThroughputTracker const& throughputTracker,
	                                           TraceEvent& te) const {
		auto const cost = throughputTracker.getThroughput(throttlingId);
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

	// Returns the limiting throttling ratio for the provided throttlingId.
	// The throttling ratios from the worst maxFallingBehind zones are
	// ignored, because we do not want non-workload related issues (e.g. slow disks)
	// to affect tag throttling. If more than maxFallingBehind zones are at
	// or near saturation, this indicates that throttling should take place.
	Optional<double> getLimitingThrottlingRatio(ThrottlingId const& throttlingId,
	                                            IRKMetricsTracker const& metricsTracker,
	                                            RKServerThroughputTracker const& serverThroughputTracker) const {
		boost::unordered_flat_map<Optional<Standalone<StringRef>>, double> zoneThrottlingRatios;
		auto const& storageQueueInfo = metricsTracker.getStorageQueueInfo();
		for (auto const& [id, ssInfo] : storageQueueInfo) {
			auto const throttlingRatio =
			    ssInfo.getTagThrottlingRatio(SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES,
			                                 SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_BYTES_STORAGE_SERVER);
			if (!throttlingRatio.present()) {
				continue;
			}

			// Ignore storage servers where throttlingId is not significantly contributing
			auto const throughput = serverThroughputTracker.getThroughput(id, throttlingId);
			if (!throughput.present() || throughput.get() < limitingThreshold) {
				continue;
			}

			auto const [it, inserted] =
			    zoneThrottlingRatios.try_emplace(ssInfo.locality.zoneId(), throttlingRatio.get());
			if (!inserted) {
				auto& zoneThrottlingRatio = it->second;
				zoneThrottlingRatio = std::min<double>(zoneThrottlingRatio, throttlingRatio.get());
			}
		}
		if (zoneThrottlingRatios.size() <= maxFallingBehind) {
			return {};
		} else {
			std::priority_queue<double, std::vector<double>> maxHeap;
			ASSERT_GE(maxFallingBehind, 0);
			for (auto const& [_, throttlingRatio] : zoneThrottlingRatios) {
				if (maxHeap.size() < maxFallingBehind + 1) {
					maxHeap.push(throttlingRatio);
				} else if (maxHeap.top() > throttlingRatio) {
					maxHeap.pop();
					maxHeap.push(throttlingRatio);
				}
			}
			return maxHeap.top();
		}
	}

	Optional<double> getLimitingTps(ThrottlingId const& throttlingId,
	                                IRKMetricsTracker const& metricsTracker,
	                                RKServerThroughputTracker const& serverThroughputTracker) const {
		auto const transactionRate = getTransactionRate(throttlingId);
		if (!transactionRate.present()) {
			return {};
		}
		auto const throttlingRatio = getLimitingThrottlingRatio(throttlingId, metricsTracker, serverThroughputTracker);
		if (!throttlingRatio.present()) {
			return {};
		}
		return transactionRate.get() * throttlingRatio.get();
	}

	Optional<double> getLimitingBps(ThrottlingId const& throttlingId,
	                                IRKMetricsTracker const& metricsTracker,
	                                IRKClusterThroughputTracker const& clusterThroughputTracker,
	                                RKServerThroughputTracker const& serverThroughputTracker) {
		auto const throttlingRatio = getLimitingThrottlingRatio(throttlingId, metricsTracker, serverThroughputTracker);
		if (!throttlingRatio.present()) {
			return {};
		}
		auto const currentThroughput = clusterThroughputTracker.getThroughput(throttlingId);
		return currentThroughput * throttlingRatio.get();
	}

	struct TargetTpsResult {
		Optional<double> targetTps;
		bool isThrottled{ false };
	};

	TargetTpsResult getTargetTps(ThrottlingId const& throttlingId,
	                             IRKThroughputQuotaCache const& quotaCache,
	                             IRKMetricsTracker const& metricsTracker,
	                             IRKClusterThroughputTracker const& clusterThroughputTracker,
	                             RKServerThroughputTracker const& serverThroughputTracker,
	                             TraceEvent& te) {
		TargetTpsResult result;
		auto const limitingTps = getLimitingTps(throttlingId, metricsTracker, serverThroughputTracker);
		auto const averageTransactionCost = getAverageTransactionCost(throttlingId, clusterThroughputTracker, te);
		auto const totalQuota = quotaCache.getTotalQuota(throttlingId);
		auto const reservedQuota = quotaCache.getReservedQuota(throttlingId);
		if (!averageTransactionCost.present()) {
			return result;
		}

		ASSERT_EQ(totalQuota.present(), reservedQuota.present());
		Optional<double> desiredTps, reservedTps;
		if (totalQuota.present()) {
			auto const desiredTps = totalQuota.get() / averageTransactionCost.get();
			auto const reservedTps = reservedQuota.get() / averageTransactionCost.get();
			if (limitingTps.present()) {
				result.targetTps = std::max(reservedTps, std::min(desiredTps, limitingTps.get()));
				result.isThrottled = limitingTps.get() < desiredTps;
			} else {
				result.targetTps = desiredTps;
			}
		} else {
			if (SERVER_KNOBS->THROTTLE_WITH_UNSET_THROUGHPUT_QUOTAS) {
				result.targetTps = limitingTps;
				result.isThrottled = limitingTps.present();
			} else {
				return result;
			}
		}

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

	struct TargetBpsResult {
		Optional<double> targetBps;
		bool isThrottled{ false };
	};

	TargetBpsResult getTargetBps(ThrottlingId const& throttlingId,
	                             IRKThroughputQuotaCache const& quotaCache,
	                             IRKMetricsTracker const& metricsTracker,
	                             IRKClusterThroughputTracker const& clusterThroughputTracker,
	                             RKServerThroughputTracker const& serverThroughputTracker,
	                             TraceEvent& te) {
		TargetBpsResult result;
		auto const limitingBps =
		    getLimitingBps(throttlingId, metricsTracker, clusterThroughputTracker, serverThroughputTracker);
		auto const totalQuota = quotaCache.getTotalQuota(throttlingId);
		auto const reservedQuota = quotaCache.getReservedQuota(throttlingId);

		ASSERT_EQ(totalQuota.present(), reservedQuota.present());
		if (totalQuota.present()) {
			if (limitingBps.present()) {
				result.targetBps =
				    std::max<double>(reservedQuota.get(), std::min<double>(totalQuota.get(), limitingBps.get()));
				result.isThrottled = limitingBps.get() < totalQuota.get();
			} else {
				result.targetBps = totalQuota.castTo<double>();
			}
		} else {
			if (SERVER_KNOBS->THROTTLE_WITH_UNSET_THROUGHPUT_QUOTAS) {
				result.targetBps = limitingBps;
				result.isThrottled = limitingBps.present();
			} else {
				return result;
			}
		}

		te.detail("ThrottlingId", throttlingId)
		    .detail("TargetBps", result.targetBps)
		    .detail("LimitingBps", limitingBps)
		    .detail("NumStorageServers", serverThroughputTracker.storageServersTracked())
		    .detail("TotalQuota", totalQuota)
		    .detail("ReservedQuota", reservedQuota);

		return result;
	}

	// Update the busy ThrottlingId set and returns whether the throttlingId is busy
	bool isThrottlingIdBusy(ThrottlingId const& throttlingId, double metric, double effectiveLimit) {
		if (metric > (SERVER_KNOBS->THROTTLING_ID_BUSY_THRESHOLD * effectiveLimit)) {
			busyThrottlingIds.emplace(throttlingId);
		} else if (metric < (SERVER_KNOBS->THROTTLING_ID_FORGET_THRESHOLD * effectiveLimit)) {
			busyThrottlingIds.erase(throttlingId);
		}

		return busyThrottlingIds.contains(throttlingId);
	}

public:
	QuotaThrottlerImpl(UID id, int maxFallingBehind, double limitingThreshold)
	  : id(id),
	    maxFallingBehind(maxFallingBehind),
	    limitingThreshold(limitingThreshold) {}

	void addRequests(ThrottlingId const& throttlingId, int count) {
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

	ThrottlingIdMap<double> getTargetRates(int numProxies,
	                                       IRKThroughputQuotaCache const& quotaCache,
	                                       IRKMetricsTracker const& metricsTracker,
	                                       IRKClusterThroughputTracker const& clusterThroughputTracker,
	                                       RKServerThroughputTracker const& serverThroughputTracker) {
		ThrottlingIdMap<double> result;
		lastBusyThrottlingIdCount = 0;

		for (auto& [throttlingId, stats] : throttlingStatistics) {
			// Currently there is no differentiation between batch priority and default priority transactions
			auto traceName = std::string("QuotaThrottler_GotTargetTransactionRate-").append(throttlingId.toString());
			TraceEvent te(traceName.c_str(), id);
			bool const traceEnabled = stats.canLog();
			if (!traceEnabled) {
				te.disable();
			}
			auto const [targetTps, isBusy] = getTargetTps(
			    throttlingId, quotaCache, metricsTracker, clusterThroughputTracker, serverThroughputTracker, te);
			if (isBusy) {
				++lastBusyThrottlingIdCount;
			}
			if (targetTps.present()) {
				auto const smoothedTargetTps = stats.updateAndGetTargetRateLimit(targetTps.get());
				te.detail("SmoothedTargetTps", smoothedTargetTps).detail("NumProxies", numProxies);
				auto const targetPerProxyRate = std::max(1.0, smoothedTargetTps / numProxies);
				if (traceEnabled) {
					stats.updateLastLogged();
				}

				auto rate = stats.getTransactionRate();
				if (isBusyThrottlingIdsTrackingEnabled()) {
					// If busy ThrottlingId tracking is enabled, then only report rates for busy Ids
					if (isThrottlingIdBusy(throttlingId, rate, targetPerProxyRate)) {
						result[throttlingId] = targetPerProxyRate;
					}
				} else {
					result[throttlingId] = targetPerProxyRate;
				}

			} else {
				te.disable();
				busyThrottlingIds.erase(throttlingId);
			}
		}

		return result;
	}

	ThrottlingIdMap<double> getTargetThroughput(int numProxies,
	                                            IRKThroughputQuotaCache const& quotaCache,
	                                            IRKMetricsTracker const& metricsTracker,
	                                            IRKClusterThroughputTracker const& clusterThroughputTracker,
	                                            RKServerThroughputTracker const& serverThroughputTracker) {
		ThrottlingIdMap<double> result;
		lastBusyThrottlingIdCount = 0;

		for (const auto& [throttlingId, throughput] :
		     clusterThroughputTracker.getActiveThrottlingIds(SERVER_KNOBS->THROUGHPUT_QUOTA_TRACK_THRESHOLD)) {

			auto& stats = throughputStatistics[throttlingId];

			auto traceName = std::string("QuotaThrottler_GotTargetThroughput-").append(throttlingId.toString());
			TraceEvent te(traceName.c_str(), id);
			te.detail("Throughput", throughput);

			bool const traceEnabled = stats.canLog();
			if (!traceEnabled) {
				te.disable();
			}

			auto const [targetBps, isThrottled] = getTargetBps(
			    throttlingId, quotaCache, metricsTracker, clusterThroughputTracker, serverThroughputTracker, te);
			if (isThrottled) {
				++lastBusyThrottlingIdCount;
			}
			if (targetBps.present()) {
				auto const smoothedTargetBps = stats.updateAndGetTargetThroughputLimit(targetBps.get());

				te.detail("SmoothedTargetBps", smoothedTargetBps).detail("NumProxies", numProxies);
				auto const targetPerProxyThroughput =
				    std::max(CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, (int64_t)smoothedTargetBps / numProxies);
				if (traceEnabled) {
					stats.updateLastLogged();
				}

				if (isBusyThrottlingIdsTrackingEnabled()) {
					// If busy ThrottlingId tracking is enabled, then only report rates for busy Ids
					if (isThrottlingIdBusy(throttlingId, throughput, targetPerProxyThroughput)) {
						result[throttlingId] = targetPerProxyThroughput;
					}
				} else {
					result[throttlingId] = targetPerProxyThroughput;
				}

			} else {
				te.disable();
				busyThrottlingIds.erase(throttlingId);
			}
		}
		return result;
	}

	int64_t throttleCount() const { return lastBusyThrottlingIdCount; }
	int64_t manualThrottleCount() const { return 0; }

	void removeExpiredThrottlingIds() {
		boost::unordered::erase_if(throttlingStatistics, [&](auto const& kv) {
			if (!kv.second.recentTransactionsAdded()) {
				busyThrottlingIds.erase(kv.first);
				return true;
			} else {
				return false;
			}
		});

		boost::unordered::erase_if(throughputStatistics, [&](auto const& kv) {
			if (!kv.second.recentTransactionsAdded()) {
				busyThrottlingIds.erase(kv.first);
				return true;
			} else {
				return false;
			}
		});
	}

	Future<Void> removeExpiredThrottlingIdsActor() {
		return recurring([this] { removeExpiredThrottlingIds(); }, 5.0);
	}

	uint32_t throttlingIdsTracked() const { return throttlingStatistics.size(); }
};

QuotaThrottler::QuotaThrottler(UID id, int maxFallingBehind, double limitingThreshold)
  : impl(PImpl<QuotaThrottlerImpl>::create(id, maxFallingBehind, limitingThreshold)) {}

QuotaThrottler::~QuotaThrottler() = default;

Future<Void> QuotaThrottler::expireUnusedThrottlingIds() {
	return impl->removeExpiredThrottlingIdsActor();
}

void QuotaThrottler::addRequests(ThrottlingId const& throttlingId, int count) {
	return impl->addRequests(throttlingId, count);
}

ThrottlingIdMap<double> QuotaThrottler::getTargetRates(int numProxies,
                                                       IRKThroughputQuotaCache const& quotaCache,
                                                       IRKMetricsTracker const& metricsTracker,
                                                       IRKClusterThroughputTracker const& clusterThroughputTracker,
                                                       RKServerThroughputTracker const& serverThroughputTracker) {
	return impl->getTargetRates(
	    numProxies, quotaCache, metricsTracker, clusterThroughputTracker, serverThroughputTracker);
}

ThrottlingIdMap<double> QuotaThrottler::getTargetThroughput(int numProxies,
                                                            IRKThroughputQuotaCache const& quotaCache,
                                                            IRKMetricsTracker const& metricsTracker,
                                                            IRKClusterThroughputTracker const& clusterThroughputTracker,
                                                            RKServerThroughputTracker const& serverThroughputTracker) {
	return impl->getTargetThroughput(
	    numProxies, quotaCache, metricsTracker, clusterThroughputTracker, serverThroughputTracker);
}

int64_t QuotaThrottler::throttleCount() const {
	return impl->throttleCount();
}

uint32_t QuotaThrottler::throttlingIdsTracked() const {
	return impl->throttlingIdsTracked();
}

void QuotaThrottler::removeExpiredThrottlingIds() {
	return impl->removeExpiredThrottlingIds();
}
