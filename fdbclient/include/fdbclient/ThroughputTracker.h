/**
 * ThroughputTracker.h
 */

#pragma once

#include "fdbclient/Knobs.h"
#include "fdbclient/ThrottlingId.h"
#include "fdbrpc/Smoother.h"
#include <boost/unordered/unordered_flat_set.hpp>

struct ThroughputStats {
	uint64_t readThroughput{ 0 };
	uint64_t writeThroughput{ 0 };

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, readThroughput, writeThroughput);
	}
};

// The ThroughputTracker class is responsible for periodically reporting each
// throttlingId's throughput to GRV proxies. This class also tracks the average
// transaction cost for each throttlingId.
class ThroughputTracker {
	friend class ThroughputTrackerImpl;
	ThrottlingIdMap<ThroughputStats> throughput;

	uint64_t throughputReportsSent{ 0 };
	uint64_t throughputReportErrors{ 0 };
	uint64_t throughputReportTimeouts{ 0 };
	double startTime;

	struct ThroughputSmoother {
		HoltLinearSmoother smoother;
		ThroughputSmoother()
		  : smoother(CLIENT_KNOBS->AVERAGE_COST_FOLDING_TIME, CLIENT_KNOBS->AVERAGE_COST_RATE_FOLDING_TIME) {}
	};

	ThrottlingIdMap<ThroughputSmoother> smoothThroughput;
	ThrottlingIdMap<ThroughputSmoother> smoothTransactionRate;

	boost::unordered::unordered_flat_set<ThrottlingId> busyThrottlingIds;

public:
	Future<Void> run(class DatabaseContext& cx);
	void addReadCost(ThrottlingId const&, uint64_t cost);
	void addWriteCost(ThrottlingId const&, uint64_t cost);
	void addTransaction(ThrottlingId const&, uint64_t numTransactions);
	double getAverageTransactionCost(ThrottlingId const&) const;
	bool isThrottlingIdBusy(ThrottlingId const&) const;
	uint64_t getThroughputReportsSent() const;
	uint64_t getThroughputReportErrors() const;
	uint64_t getThroughputReportTimeouts() const;
};

bool isClientAverageCostTrackingEnabled();
bool isBusyThrottlingIdsTrackingEnabled();
