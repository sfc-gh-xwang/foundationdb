/**
 * IRKClusterThroughputTracker.h
 */

#pragma once

#include "fdbclient/ThrottlingId.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/IRKMetricsTracker.h"
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

// Tracks the cluster-wide throughput (in bytes/second) of each throttlingId
class IRKClusterThroughputTracker {
public:
	virtual ~IRKClusterThroughputTracker() = default;

	// Returns the current cluster-wide throughput for the provided throttling ID
	virtual double getThroughput(ThrottlingId const&) const = 0;

	// Returns the set of throttlingIds reporting throughput above the specified threshold
	virtual ThrottlingIdMap<double> getActiveThrottlingIds(double threshold) const = 0;

	// Returns the number of throttlingIds for which metrics are currently being stored
	virtual int throttlingIdsTracked() const = 0;
};

// The RKServerThroughputTracker class is responsible for tracking
// every throttlingId's reported throughput across all storage servers
class RKServerThroughputTracker : public IRKClusterThroughputTracker {
	enum class OpType {
		READ,
		WRITE,
	};

	// Tracks the read and write throughput for a particular
	// combination of storage server and throuttlingId.
	class ThroughputCounters {
		Smoother readThroughput;
		Smoother writeThroughput;

	public:
		ThroughputCounters();
		// Returns the change in throughput for the provided operation type
		[[nodiscard]] double updateThroughput(double newThroughput, OpType);
		double getThroughput() const;
	};

	// Tracks the total throughput for a particular throttlingId,
	// aggregated across reads and writes, and across the entire cluster.
	class ThrottlingIdThroughput {
		Smoother smoother;

	public:
		ThrottlingIdThroughput();
		void addDelta(double);
		double getThroughput() const;
	};

	boost::unordered_flat_map<UID, ThrottlingIdMap<ThroughputCounters>> throughputBySS;
	ThrottlingIdMap<ThrottlingIdThroughput> throughputByThrottlingId;

	// Cluster-wide cleanup
	void cleanup(boost::unordered_flat_set<UID> const& seenStorageServers,
	             boost::unordered_flat_set<ThrottlingId> const& seenThrottlingIds);

	// Cleanup for a single storage server
	void cleanupStorageServer(ThrottlingIdMap<ThroughputCounters>&,
	                          boost::unordered_flat_set<ThrottlingId> const& seenReadThrottlingId,
	                          boost::unordered_flat_set<ThrottlingId> const& seenWriteThrottlingId);

public:
	~RKServerThroughputTracker();

	// Returns all throttling IDs running significant workload on the specified storage server.
	std::vector<ThrottlingId> getThrottlingIdsAffectingStorageServer(UID storageServerId) const;

	// Updates throughput statistics based on new storage queue info
	void update(Map<UID, StorageQueueInfo> const&);

	// Returns the current throughput for the provided throttling ID on the
	// provided storage server
	Optional<double> getThroughput(UID storageServerId, ThrottlingId const&) const;

	double getThroughput(ThrottlingId const&) const override;

	ThrottlingIdMap<double> getActiveThrottlingIds(double threshold) const override;

	int throttlingIdsTracked() const override;

	int storageServersTracked() const;
};

// The RKClientThroughputTracker class is responsible for tracking
// every throttlingId's cluster-wide throughput from statistics
// reported originally from clients
class RKClientThroughputTracker : public IRKClusterThroughputTracker {
	struct ThroughputSmoother {
		HoltLinearSmoother smoother;
		double lastUpdated;
		ThroughputSmoother();

	public:
		void addThroughput(double newThroughput);
		double smoothRate() const;
		bool shouldForget(double expirationCheckInterval) const;
	};

	ThrottlingIdMap<ThroughputSmoother> throughput;
	double lastExpirationCheck;
	double expirationCheckInterval;

	void expireThrottlingIds();

public:
	explicit RKClientThroughputTracker(double expirationCheckInterval);

	~RKClientThroughputTracker();

	double getThroughput(ThrottlingId const&) const override;

	ThrottlingIdMap<double> getActiveThrottlingIds(double threshold) const override;

	int throttlingIdsTracked() const override;

	// Updates per-throttlingId throughput statistics based on newly
	// reported throughput metrics from a GRV proxy
	void update(ThrottlingIdMap<uint64_t> const& newThroughput);
};

class MockThroughputTracker : public IRKClusterThroughputTracker {
	ThrottlingIdMap<double> throughput;

public:
	~MockThroughputTracker();

	double getThroughput(ThrottlingId const&) const override;

	ThrottlingIdMap<double> getActiveThrottlingIds(double threshold) const override;

	int throttlingIdsTracked() const override;

	void update(ThrottlingId const&, double newThroughput);

	void removeThrottlingId(ThrottlingId const&);
};
