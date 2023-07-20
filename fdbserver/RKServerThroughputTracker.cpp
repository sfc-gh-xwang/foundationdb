/**
 * RKServerThroughputTracker.cpp
 */

#include "fdbserver/IRKClusterThroughputTracker.h"
#include "fdbserver/Knobs.h"

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

double RKServerThroughputTracker::ThroughputCounters::updateThroughput(double newThroughput, OpType opType) {
	if (opType == OpType::READ) {
		auto const oldThroughput = readThroughput.getTotal();
		readThroughput.setTotal(newThroughput);
		return newThroughput - oldThroughput;
	} else {
		auto const oldThroughput = writeThroughput.getTotal();
		writeThroughput.setTotal(newThroughput);
		return newThroughput - oldThroughput;
	}
}

double RKServerThroughputTracker::ThroughputCounters::getThroughput() const {
	return readThroughput.smoothTotal() + writeThroughput.smoothTotal();
}

RKServerThroughputTracker::ThrottlingIdThroughput::ThrottlingIdThroughput()
  : smoother(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME) {}

void RKServerThroughputTracker::ThrottlingIdThroughput::addDelta(double delta) {
	smoother.addDelta(delta);
}

double RKServerThroughputTracker::ThrottlingIdThroughput::getThroughput() const {
	return smoother.smoothTotal();
}

RKServerThroughputTracker::~RKServerThroughputTracker() = default;

// Exponentially decay throughput statistics for throttlingIds that were not reported.
// If reported throughput for a particular throttlingId is too low, stop tracking this
// throttlingId.
void RKServerThroughputTracker::cleanupStorageServer(
    ThrottlingIdMap<ThroughputCounters>& throttlingIdToThroughputCounters,
    boost::unordered_flat_set<ThrottlingId> const& seenReadThrottlingIds,
    boost::unordered_flat_set<ThrottlingId> const& seenWriteThrottlingIds) {

	for (auto& [throttlingId, throughputCounters] : throttlingIdToThroughputCounters) {
		if (!seenReadThrottlingIds.count(throttlingId)) {
			auto const delta = throughputCounters.updateThroughput(0, OpType::READ);
			throughputByThrottlingId[throttlingId].addDelta(delta);
		}
		if (!seenWriteThrottlingIds.count(throttlingId)) {
			auto const delta = throughputCounters.updateThroughput(0, OpType::WRITE);
			throughputByThrottlingId[throttlingId].addDelta(delta);
		}
	}

	boost::unordered::erase_if(
	    throttlingIdToThroughputCounters, [&seenReadThrottlingIds, &seenWriteThrottlingIds](auto const& kv) {
		    auto const& [throttlingId, throughputCounters] = kv;
		    return (!seenReadThrottlingIds.count(throttlingId) && !seenWriteThrottlingIds.count(throttlingId) &&
		            throughputCounters.getThroughput() <
		                SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_THROUGHPUT_THRESHOLD);
	    });
}

// For unseen storage servers, exponentially decay throughput statistics for every throttlingId.
// If the reported throughput for a particular throttlingId is too low, stop tracking this
// throttlingId. If no more throttlingIds are being tracked for a particular storage server,
// stop tracking this storage server altogether. Stop tracking throttling IDs that are reporting
// insufficient throughput cluster-wide.
void RKServerThroughputTracker::cleanup(boost::unordered_flat_set<UID> const& seenStorageServers,
                                        boost::unordered_flat_set<ThrottlingId> const& seenThrottlingIds) {

	// Set throughput to 0 for storage servers that didn't report throughput.
	for (auto& [ssId, throttlingIdToThroughputCounters] : throughput) {
		if (!seenStorageServers.count(ssId)) {
			for (auto& [throttlingId, throughputCounters] : throttlingIdToThroughputCounters) {
				auto const delta = throughputCounters.updateThroughput(0, OpType::READ) +
				                   throughputCounters.updateThroughput(0, OpType::WRITE);
				throughputByThrottlingId[throttlingId].addDelta(delta);
			}
		}
	}

	// Erase statistics for combinations of throttlingIds and storage servers reporting
	// insufficient throughput.
	boost::unordered::erase_if(throughput, [&seenStorageServers](auto& kv) {
		auto& [ssId, throttlingIdToThroughputCounters] = kv;
		if (seenStorageServers.count(ssId)) {
			return false;
		} else {
			boost::unordered::erase_if(throttlingIdToThroughputCounters, [](auto& kv2) {
				return kv2.second.getThroughput() < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_THROUGHPUT_THRESHOLD;
			});
			return throttlingIdToThroughputCounters.empty();
		}
	});

	// Erase cluster-wide throughput statistics for throttlingIds reporting insufficient throughput.
	boost::unordered::erase_if(throughputByThrottlingId, [&seenThrottlingIds](auto const& kv) {
		auto const& [throttlingId, throttlingIdThroughput] = kv;
		return !seenThrottlingIds.count(throttlingId) &&
		       (throttlingIdThroughput.getThroughput() <
		        SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_THROUGHPUT_THRESHOLD);
	});
}

void RKServerThroughputTracker::update(Map<UID, StorageQueueInfo> const& sqInfos) {
	boost::unordered_flat_set<UID> seenStorageServerIds;
	boost::unordered_flat_set<ThrottlingId> seenThrottlingIds;
	for (auto it = sqInfos.begin(); it != sqInfos.end(); ++it) {
		auto const& ss = it->value;
		if (!ss.valid) {
			continue;
		}
		seenStorageServerIds.insert(ss.id);
		boost::unordered_flat_set<ThrottlingId> seenReadThrottlingIds, seenWriteThrottlingIds;
		auto& throttlingIdToThroughputCounters = throughput[ss.id];
		for (const auto& busyReader : ss.busiestReaders) {
			seenReadThrottlingIds.insert(busyReader.throttlingId);
			seenThrottlingIds.insert(busyReader.throttlingId);
			auto const delta = throttlingIdToThroughputCounters[busyReader.throttlingId].updateThroughput(
			    busyReader.rate, OpType::READ);
			throughputByThrottlingId[busyReader.throttlingId].addDelta(delta);
		}
		for (const auto& busyWriter : ss.busiestWriters) {
			seenWriteThrottlingIds.insert(busyWriter.throttlingId);
			seenThrottlingIds.insert(busyWriter.throttlingId);
			auto const delta = throttlingIdToThroughputCounters[busyWriter.throttlingId].updateThroughput(
			    busyWriter.rate, OpType::WRITE);
			throughputByThrottlingId[busyWriter.throttlingId].addDelta(delta);
		}

		cleanupStorageServer(throttlingIdToThroughputCounters, seenReadThrottlingIds, seenWriteThrottlingIds);
	}
	cleanup(seenStorageServerIds, seenThrottlingIds);
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
	auto const throttlingIdThroughput = tryGet(throughputByThrottlingId, throttlingId);
	if (throttlingIdThroughput.present()) {
		return throttlingIdThroughput.get().getThroughput();
	} else {
		return 0.0;
	}
}

boost::unordered_flat_set<ThrottlingId> RKServerThroughputTracker::getActiveThrottlingIds(double threshold) const {
	boost::unordered_flat_set<ThrottlingId> result;
	for (auto const& [throttlingId, throttlingIdThroughput] : throughputByThrottlingId) {
		if (throttlingIdThroughput.getThroughput() > threshold) {
			result.insert(throttlingId);
		}
	}
	return result;
}

int RKServerThroughputTracker::storageServersTracked() const {
	return throughput.size();
}

int RKServerThroughputTracker::throttlingIdsTracked() const {
	return throughputByThrottlingId.size();
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

boost::unordered_flat_set<ThrottlingId> MockThroughputTracker::getActiveThrottlingIds(double threshold) const {
	boost::unordered_flat_set<ThrottlingId> result;
	for (auto const& [throttlingId, throughputValue] : throughput) {
		if (throughputValue > threshold) {
			result.insert(throttlingId);
		}
	}
	return result;
}

int MockThroughputTracker::throttlingIdsTracked() const {
	return throughput.size();
}

void MockThroughputTracker::update(ThrottlingId const& throttlingId, double newThroughput) {
	throughput[throttlingId] = newThroughput;
}

void MockThroughputTracker::removeThrottlingId(ThrottlingId const& throttlingId) {
	throughput.erase(throttlingId);
}
