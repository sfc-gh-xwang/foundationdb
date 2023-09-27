/**
 * ThroughputTracker.actor.cpp
 */

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ThroughputTracker.h"
#include "fdbrpc/LoadBalance.actor.h"
#include "flow/actorcompiler.h" // must be last include

class ThroughputTrackerImpl {
public:
	ACTOR static Future<Void> run(ThroughputTracker* self, DatabaseContext* cx) {
		state Future<Void> timer = Never();
		loop {
			timer = delayJittered(CLIENT_KNOBS->CLIENT_THROUGHPUT_REPORT_INTERVAL);
			if (self->throughput.empty()) {
				wait(timer);
				continue;
			}
			try {
				ReportThroughputRequest req(std::move(self->throughput));
				self->throughput.clear();
				choose {
					when(ReportThroughputReply reply =
					         wait(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies::False),
					                               &GrvProxyInterface::reportThroughput,
					                               std::move(req),
					                               TaskPriority::DefaultPromiseEndpoint,
					                               AtMostOnce::True))) {
						++self->throughputReportsSent;
						if (isBusyThrottlingIdsTrackingEnabled()) {
							self->busyThrottlingIds = std::move(reply.busyThrottlingIds);
						}
					}

					when(wait(timer)) {
						++self->throughputReportTimeouts;
						CODE_PROBE(true, "ReportThroughputRequest timed out");
						TraceEvent(SevWarn, "ReportThroughputRequestTimedOut");
					}
				}
			} catch (Error& e) {
				++self->throughputReportErrors;
				if (e.code() == error_code_request_maybe_delivered) {
					CODE_PROBE(true, "ReportThroughputRequest maybe delivered");
					TraceEvent(SevWarn, "ReportThroughputRequestMaybeDelivered");
				} else {
					TraceEvent(SevWarnAlways, "ThroughputTrackerFailedWithError").error(e);
					throw e;
				}
			}
		}
	}
}; // class ThroughputTrackersImpl

Future<Void> ThroughputTracker::run(DatabaseContext& cx) {
	startTime = now();
	return ThroughputTrackerImpl::run(this, &cx);
}

void ThroughputTracker::addReadCost(ThrottlingId const& throttlingId, uint64_t cost) {
	throughput[throttlingId].readThroughput += cost;
	if (isClientAverageCostTrackingEnabled()) {
		smoothThroughput[throttlingId].smoother.addDelta(cost);
	}
}

void ThroughputTracker::addWriteCost(ThrottlingId const& throttlingId, uint64_t cost) {
	throughput[throttlingId].writeThroughput += cost;
	if (isClientAverageCostTrackingEnabled()) {
		smoothThroughput[throttlingId].smoother.addDelta(cost);
	}
}

void ThroughputTracker::addTransaction(ThrottlingId const& throttlingId, uint64_t numTransactions) {
	smoothTransactionRate[throttlingId].smoother.addDelta(numTransactions);
}

double ThroughputTracker::getAverageTransactionCost(ThrottlingId const& throttlingId) const {
	if (now() - startTime < CLIENT_KNOBS->AVERAGE_COST_START_DELAY) {
		return CLIENT_KNOBS->DEFAULT_EXPECTED_TRANSACTION_COST;
	}

	auto throughputIt = smoothThroughput.find(throttlingId);
	auto transactionRateIt = smoothTransactionRate.find(throttlingId);

	if (throughputIt == smoothThroughput.end() || transactionRateIt == smoothTransactionRate.end()) {
		return CLIENT_KNOBS->DEFAULT_EXPECTED_TRANSACTION_COST;
	} else {
		auto const throughput = throughputIt->second.smoother.smoothRate();
		auto const transactionRate = transactionRateIt->second.smoother.smoothRate();

		// In this case, throughput is extremely low, and an
		// average cost estimation cannot be made
		if (throughput <= 0.0 || transactionRate < 1.0) {
			return CLIENT_KNOBS->DEFAULT_EXPECTED_TRANSACTION_COST;
		}
		return throughput / transactionRate;
	}
}

bool isClientAverageCostTrackingEnabled() {
	// TRACK_AVERAGE_COST_ON_CLIENTS depends on TRACK_THROUGHPUT_ON_CLIENTS
	return CLIENT_KNOBS->TRACK_THROUGHPUT_ON_CLIENTS && CLIENT_KNOBS->TRACK_AVERAGE_COST_ON_CLIENTS;
}

bool isBusyThrottlingIdsTrackingEnabled() {
	// TRACK_BUSY_THROTTLING_IDS depends on TRACK_THROUGHPUT_ON_CLIENTS
	return isClientAverageCostTrackingEnabled() && CLIENT_KNOBS->TRACK_BUSY_THROTTLING_IDS;
}

bool ThroughputTracker::isThrottlingIdBusy(ThrottlingId const& throttlingId) const {
	return busyThrottlingIds.contains(throttlingId);
}

uint64_t ThroughputTracker::getThroughputReportsSent() const {
	return throughputReportsSent;
}

uint64_t ThroughputTracker::getThroughputReportErrors() const {
	return throughputReportErrors;
}

uint64_t ThroughputTracker::getThroughputReportTimeouts() const {
	return throughputReportTimeouts;
}
