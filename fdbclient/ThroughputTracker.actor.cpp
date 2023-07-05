/**
 * ThroughputTracker.actor.cpp
 */

#include "fdbclient/ClientKnobs.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ThroughputTracker.h"
#include "fdbrpc/LoadBalance.actor.h"
#include "flow/actorcompiler.h" // must be last include

class ThroughputTrackerImpl {

public:
	ACTOR static Future<Void> run(ThroughputTracker* self, DatabaseContext* cx) {
		loop {
			ReportThroughputRequest req(std::move(self->throughput));
			self->throughput.clear();
			wait(basicLoadBalance(cx->getGrvProxies(UseProvisionalProxies::False),
			                      &GrvProxyInterface::reportThroughput,
			                      std::move(req),
			                      TaskPriority::DefaultPromiseEndpoint,
			                      AtMostOnce::True));
			wait(delayJittered(CLIENT_KNOBS->CLIENT_THROUGHPUT_REPORT_INTERVAL));
		}
	}
}; // class ThroughputTrackersImpl

Future<Void> ThroughputTracker::run(DatabaseContext& cx) {
	startTime = now();
	return ThroughputTrackerImpl::run(this, &cx);
}

void ThroughputTracker::addCost(ThrottlingId const& throttlingId, uint64_t cost) {
	throughput[throttlingId] += cost;
	if (CLIENT_KNOBS->TRACK_AVERAGE_COST_ON_CLIENT) {
		smoothThroughput[throttlingId].smoother.addDelta(cost);
	}
}

void ThroughputTracker::addTransaction(ThrottlingId const& throttlingId, uint64_t numTransactions) {
	smoothTransactionRate[throttlingId].smoother.addDelta(numTransactions);
}

double ThroughputTracker::getAverageTransactionCost(ThrottlingId const& throttlingId) {
	if (now() - startTime < CLIENT_KNOBS->AVERGAE_COST_START_DELAY) {
		return CLIENT_KNOBS->DEFAULT_EXPECTED_TRANSACTION_COST;
	}

	auto throughput = smoothThroughput.find(throttlingId);
	auto transactionRate = smoothTransactionRate.find(throttlingId);

	if (throughput == smoothThroughput.end() || transactionRate == smoothTransactionRate.end()) {
		return CLIENT_KNOBS->DEFAULT_EXPECTED_TRANSACTION_COST;
	} else {
		return throughput->second.smoother.smoothRate() / transactionRate->second.smoother.smoothRate();
	}
}