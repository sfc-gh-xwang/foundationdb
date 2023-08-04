/**
 * ThroughputTracker.h
 */

#pragma once

#include "fdbclient/Knobs.h"
#include "fdbclient/ThrottlingId.h"
#include "fdbrpc/Smoother.h"
#include <boost/unordered/unordered_flat_set.hpp>

// The ThroughputTracker class is responsible for periodically reporting each
// throttlingId's throughput to GRV proxies. This class also tracks the average
// transaction cost for each throttlingId.
class ThroughputTracker {
	friend class ThroughputTrackerImpl;
	ThrottlingIdMap<uint64_t> throughput;

	double startTime;

	struct ThroughputSmoother {
		HoltLinearSmoother smoother;
		ThroughputSmoother()
		  : smoother(CLIENT_KNOBS->AVERAGE_COST_FOLDING_TIME, CLIENT_KNOBS->AVERAGE_COST_FOLDING_TIME) {}
	};

	ThrottlingIdMap<ThroughputSmoother> smoothThroughput;
	ThrottlingIdMap<ThroughputSmoother> smoothTransactionRate;

	boost::unordered::unordered_flat_set<ThrottlingId> busyThrottlingIds;

public:
	Future<Void> run(class DatabaseContext& cx);
	void addCost(ThrottlingId const&, uint64_t cost);
	void addTransaction(ThrottlingId const&, uint64_t numTransactions);
	double getAverageTransactionCost(ThrottlingId const&) const;
	bool isThrottlingIdBusy(ThrottlingId const&) const;
};

bool isClientAverageCostTrackingEnabled();
bool isBusyThrottlingIdsTrackingEnabled();
