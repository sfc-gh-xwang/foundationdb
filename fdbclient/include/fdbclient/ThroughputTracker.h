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

	struct ThroughputStatistics {
		double startTime;
		double lastLogged;

		HoltLinearSmoother smoothReadThroughput;
		HoltLinearSmoother smoothWriteThroughput;
		HoltLinearSmoother smoothTransactionRate;

		ThroughputStatistics()
		  : startTime(now()),
		    lastLogged(0),
		    smoothReadThroughput(CLIENT_KNOBS->AVERAGE_COST_FOLDING_TIME, CLIENT_KNOBS->AVERAGE_COST_RATE_FOLDING_TIME),
		    smoothWriteThroughput(CLIENT_KNOBS->AVERAGE_COST_FOLDING_TIME,
		                          CLIENT_KNOBS->AVERAGE_COST_RATE_FOLDING_TIME),
		    smoothTransactionRate(CLIENT_KNOBS->AVERAGE_COST_FOLDING_TIME,
		                          CLIENT_KNOBS->AVERAGE_COST_RATE_FOLDING_TIME) {}

		Optional<double> getAverageTransactionCost() const {
			// Wait until we gather enough data
			if (now() - startTime < CLIENT_KNOBS->AVERAGE_COST_START_DELAY) {
				return CLIENT_KNOBS->DEFAULT_EXPECTED_TRANSACTION_COST;
			}

			auto const throughput = smoothReadThroughput.smoothRate() + smoothWriteThroughput.smoothRate();
			auto const transactionRate = smoothTransactionRate.smoothRate();

			// In this case, throughput is extremely low, and an
			// average cost estimation cannot be made
			if (throughput <= 0.0 || transactionRate < 1.0) {
				return CLIENT_KNOBS->DEFAULT_EXPECTED_TRANSACTION_COST;
			}
			return throughput / transactionRate;
		}

		void addReadCost(uint64_t cost) { smoothReadThroughput.addDelta(cost); }
		void addWriteCost(uint64_t cost) { smoothWriteThroughput.addDelta(cost); }
		void addTransaction(uint64_t numTransactions) { smoothTransactionRate.addDelta(numTransactions); }

		bool canLog() const { return now() - lastLogged > CLIENT_KNOBS->CLIENT_THROUGHPUT_TRACE_INTERVAL; }
		void updateLastLogged() { lastLogged = now(); }
	};

	ThrottlingIdMap<ThroughputStatistics> throughputStatistics;

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
