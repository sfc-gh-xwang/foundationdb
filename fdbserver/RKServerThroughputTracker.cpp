/**
 * RKServerThroughputTracker.cpp
 */

#include "fdbserver/IRKClusterThroughputTracker.h"
#include "fdbserver/Knobs.h"
#include <boost/unordered/unordered_flat_map.hpp>

namespace {

template <class K, class V, class H>
static Optional<V> tryGet(boost::unordered_flat_map<K, V, H> const& m, K const& k) {
	auto it = m.find(k);
	if (it == m.end()) {
		return {};
	} else {
		return it->second;
	}
}

} // namespace

RKServerThroughputTracker::ThroughputCounters::ThroughputCounters()
  : readThroughput(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME),
    writeThroughput(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME) {}

std::vector<ThrottlingId> RKServerThroughputTracker::getThrottlingIdsAffectingStorageServer(UID storageServerId) const {
	std::vector<ThrottlingId> result;
	auto const throttlingIdToThroughputCounters = tryGet(throughput, storageServerId);
	if (!throttlingIdToThroughputCounters.present()) {
		return {};
	} else {
		result.reserve(throttlingIdToThroughputCounters.get().size());
		for (const auto& [throttlingId, _] : throttlingIdToThroughputCounters.get()) {
			result.push_back(throttlingId);
		}
	}
	return result;
}

void RKServerThroughputTracker::ThroughputCounters::updateThroughput(double newThroughput, OpType opType) {
	if (opType == OpType::READ) {
		readThroughput.setTotal(newThroughput);
	} else {
		writeThroughput.setTotal(newThroughput);
	}
}

double RKServerThroughputTracker::ThroughputCounters::getThroughput() const {
	return readThroughput.smoothTotal() + writeThroughput.smoothTotal();
}

RKServerThroughputTracker::~RKServerThroughputTracker() = default;

// Exponentially decay throughput statistics for throttlingIds that were not reported.
// If reported throughput for a particular throttlingId is too low, stop tracking this
// throttlingId.
void RKServerThroughputTracker::cleanupUnseenThrottlingIds(
    ThrottlingIdMap<ThroughputCounters>& throttlingIdToThroughputCounters,
    std::unordered_set<ThrottlingId> const& seenReadThrottlingIds,
    std::unordered_set<ThrottlingId> const& seenWriteThrottlingIds) {

	boost::unordered::erase_if(throttlingIdToThroughputCounters, [&](auto& kv) {
		auto& [throttlingId, throughputCounters] = kv;
		bool seen = false;
		if (seenReadThrottlingIds.count(throttlingId)) {
			seen = true;
		} else {
			throughputCounters.updateThroughput(0, OpType::READ);
		}
		if (seenWriteThrottlingIds.count(throttlingId)) {
			seen = true;
		} else {
			throughputCounters.updateThroughput(0, OpType::WRITE);
		}
		return !seen &&
		       throughputCounters.getThroughput() < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_THROUGHPUT_THRESHOLD;
	});
}

// For unseen storage servers, exponentially decay throughput statistics for every throttlingId.
// If the reported throughput for a particular throttlingId is too low, stop tracking this
// throttlingId. If no more throttlingIds are being tracked for a particular storage server,
// stop tracking this storage server altogether.
void RKServerThroughputTracker::cleanupUnseenStorageServers(std::unordered_set<UID> const& seen) {
	boost::unordered::erase_if(throughput, [&](auto& kv) {
		auto& [ssId, throttlingIdToThroughputCounters] = kv;

		if (seen.count(ssId)) {
			return false;
		} else {
			boost::unordered::erase_if(throttlingIdToThroughputCounters, [](auto& kv2) {
				auto& throughputCounters = kv2.second;
				throughputCounters.updateThroughput(0, OpType::READ);
				throughputCounters.updateThroughput(0, OpType::WRITE);
				return throughputCounters.getThroughput() <
				       SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_THROUGHPUT_THRESHOLD;
			});
			return throttlingIdToThroughputCounters.empty();
		}
	});
}

void RKServerThroughputTracker::update(Map<UID, StorageQueueInfo> const& sqInfos) {
	std::unordered_set<UID> seenStorageServerIds;
	for (auto it = sqInfos.begin(); it != sqInfos.end(); ++it) {
		auto const& ss = it->value;
		if (!ss.valid) {
			continue;
		}
		seenStorageServerIds.insert(ss.id);
		std::unordered_set<ThrottlingId> seenReadThrottlingIds, seenWriteThrottlingIds;
		auto& throttlingIdToThroughputCounters = throughput[ss.id];
		for (const auto& busyReader : ss.busiestReaders) {
			seenReadThrottlingIds.insert(busyReader.throttlingId);
			throttlingIdToThroughputCounters[busyReader.throttlingId].updateThroughput(busyReader.rate, OpType::READ);
		}
		for (const auto& busyWriter : ss.busiestWriters) {
			seenWriteThrottlingIds.insert(busyWriter.throttlingId);
			throttlingIdToThroughputCounters[busyWriter.throttlingId].updateThroughput(busyWriter.rate, OpType::WRITE);
		}

		cleanupUnseenThrottlingIds(throttlingIdToThroughputCounters, seenReadThrottlingIds, seenWriteThrottlingIds);
	}
	cleanupUnseenStorageServers(seenStorageServerIds);
}

Optional<double> RKServerThroughputTracker::getThroughput(UID storageServerId, ThrottlingId const& throttlingId) const {
	auto const throttlingIdToThroughputCounters = tryGet(throughput, storageServerId);
	if (!throttlingIdToThroughputCounters.present()) {
		return {};
	}
	auto const throughputCounter = tryGet(throttlingIdToThroughputCounters.get(), throttlingId);
	if (!throughputCounter.present()) {
		return {};
	}
	return throughputCounter.get().getThroughput();
}

double RKServerThroughputTracker::getThroughput(ThrottlingId const& throttlingId) const {
	double result{ 0.0 };
	for (auto const& [ssId, _] : throughput) {
		result += getThroughput(ssId, throttlingId).orDefault(0);
	}
	return result;
}

std::unordered_set<ThrottlingId> RKServerThroughputTracker::getActiveThrottlingIds(double threshold) const {
	ThrottlingIdMap<double> throttlingIdToThroughput;
	for (auto const& [ss, throttlingIdToThroughputCounters] : throughput) {
		for (auto const& [throttlingId, throughputCounters] : throttlingIdToThroughputCounters) {
			throttlingIdToThroughput[throttlingId] += throughputCounters.getThroughput();
		}
	}
	std::unordered_set<ThrottlingId> result;
	for (auto const& [throttlingId, clusterWideThroughput] : throttlingIdToThroughput) {
		if (clusterWideThroughput > threshold) {
			result.insert(throttlingId);
		}
	}
	return result;
}

int RKServerThroughputTracker::storageServersTracked() const {
	return throughput.size();
}

MockThroughputTracker::~MockThroughputTracker() = default;

double MockThroughputTracker::getThroughput(ThrottlingId const& throttlingId) const {
	auto it = throughput.find(throttlingId);
	if (it == throughput.end()) {
		return 0.0;
	} else {
		return it->second;
	}
}

std::unordered_set<ThrottlingId> MockThroughputTracker::getActiveThrottlingIds(double threshold) const {
	std::unordered_set<ThrottlingId> result;
	for (auto const& [throttlingId, throughputValue] : throughput) {
		if (throughputValue > threshold) {
			result.insert(throttlingId);
		}
	}
	return result;
}

void MockThroughputTracker::update(ThrottlingId const& throttlingId, double newThroughput) {
	throughput[throttlingId] = newThroughput;
}

void MockThroughputTracker::removeThrottlingId(ThrottlingId const& throttlingId) {
	throughput.erase(throttlingId);
}
