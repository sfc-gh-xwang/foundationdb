/**
 * RKServerThroughputTracker.cpp
 */

#include "fdbclient/ThrottlingId.h"
#include "fdbserver/IRKClusterThroughputTracker.h"
#include "fdbserver/Knobs.h"

namespace {

template <class K, class V, class H>
static V const* tryGet(boost::unordered_flat_map<K, V, H> const& m, K const& k) {
	auto it = m.find(k);
	if (it == m.end()) {
		return nullptr;
	} else {
		return &it->second;
	}
}

} // namespace

RKServerThroughputTracker::ThroughputCounters::ThroughputCounters()
  : readThroughput(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME),
    writeThroughput(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME) {}

std::vector<ThrottlingId> RKServerThroughputTracker::getThrottlingIdsAffectingStorageServer(UID storageServerId) const {
	std::vector<ThrottlingId> result;
	auto const throttlingIdToThroughputCounters = tryGet(throughputBySS, storageServerId);
	if (!throttlingIdToThroughputCounters) {
		return {};
	} else {
		result.reserve(throttlingIdToThroughputCounters->size());
		for (const auto& [throttlingId, _] : *throttlingIdToThroughputCounters) {
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
  : readSmoother(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME),
    writeSmoother(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME) {}

void RKServerThroughputTracker::ThrottlingIdThroughput::addReadDelta(double delta) {
	readSmoother.addDelta(delta);
}

void RKServerThroughputTracker::ThrottlingIdThroughput::addWriteDelta(double delta) {
	writeSmoother.addDelta(delta);
}

IRKClusterThroughputTracker::Throughput RKServerThroughputTracker::ThrottlingIdThroughput::getThroughput() const {
	return IRKClusterThroughputTracker::Throughput(readSmoother.smoothTotal(), writeSmoother.smoothTotal());
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
			throughputByThrottlingId[throttlingId].addReadDelta(delta);
		}
		if (!seenWriteThrottlingIds.count(throttlingId)) {
			auto const delta = throughputCounters.updateThroughput(0, OpType::WRITE);
			throughputByThrottlingId[throttlingId].addWriteDelta(delta);
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
	for (auto& [ssId, throttlingIdToThroughputCounters] : throughputBySS) {
		if (!seenStorageServers.count(ssId)) {
			for (auto& [throttlingId, throughputCounters] : throttlingIdToThroughputCounters) {
				auto const readDelta = throughputCounters.updateThroughput(0, OpType::READ);
				auto const writeDelta = throughputCounters.updateThroughput(0, OpType::WRITE);
				throughputByThrottlingId[throttlingId].addReadDelta(readDelta);
				throughputByThrottlingId[throttlingId].addWriteDelta(writeDelta);
			}
		}
	}

	// Erase statistics for combinations of throttlingIds and storage servers reporting
	// insufficient throughput.
	boost::unordered::erase_if(throughputBySS, [&seenStorageServers](auto& kv) {
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
		auto const totalThroughput = throttlingIdThroughput.getThroughput().readThroughput +
		                             throttlingIdThroughput.getThroughput().writeThroughput;
		return !seenThrottlingIds.count(throttlingId) &&
		       (totalThroughput < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_THROUGHPUT_THRESHOLD);
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
		auto& throttlingIdToThroughputCounters = throughputBySS[ss.id];
		for (const auto& busyReader : ss.busiestReaders) {
			seenReadThrottlingIds.insert(busyReader.throttlingId);
			seenThrottlingIds.insert(busyReader.throttlingId);
			auto const delta = throttlingIdToThroughputCounters[busyReader.throttlingId].updateThroughput(
			    busyReader.rate, OpType::READ);
			throughputByThrottlingId[busyReader.throttlingId].addReadDelta(delta);
		}
		for (const auto& busyWriter : ss.busiestWriters) {
			seenWriteThrottlingIds.insert(busyWriter.throttlingId);
			seenThrottlingIds.insert(busyWriter.throttlingId);
			auto const delta = throttlingIdToThroughputCounters[busyWriter.throttlingId].updateThroughput(
			    busyWriter.rate, OpType::WRITE);
			throughputByThrottlingId[busyWriter.throttlingId].addWriteDelta(delta);
		}

		cleanupStorageServer(throttlingIdToThroughputCounters, seenReadThrottlingIds, seenWriteThrottlingIds);
	}
	cleanup(seenStorageServerIds, seenThrottlingIds);
}

double RKServerThroughputTracker::getThroughput(UID storageServerId) const {
	auto const throttlingIdToThroughputCounters = tryGet(throughputBySS, storageServerId);
	if (!throttlingIdToThroughputCounters) {
		return {};
	}
	double result{ 0.0 };
	for (auto const& [_, throughputCounter] : *throttlingIdToThroughputCounters) {
		result += throughputCounter.getThroughput();
	}
	return result;
}

Optional<double> RKServerThroughputTracker::getThroughput(UID storageServerId, ThrottlingId const& throttlingId) const {
	auto const throttlingIdToThroughputCounters = tryGet(throughputBySS, storageServerId);
	if (!throttlingIdToThroughputCounters) {
		return {};
	}
	auto const throughputCounter = tryGet(*throttlingIdToThroughputCounters, throttlingId);
	if (!throughputCounter) {
		return {};
	}
	return throughputCounter->getThroughput();
}

IRKClusterThroughputTracker::Throughput RKServerThroughputTracker::getThroughput(
    ThrottlingId const& throttlingId) const {
	auto const throttlingIdThroughput = tryGet(throughputByThrottlingId, throttlingId);
	if (throttlingIdThroughput) {
		return throttlingIdThroughput->getThroughput();
	} else {
		return IRKClusterThroughputTracker::Throughput(0.0, 0.0);
	}
}

ThrottlingIdMap<IRKClusterThroughputTracker::Throughput> RKServerThroughputTracker::getActiveThrottlingIds(
    double threshold) const {
	ThrottlingIdMap<IRKClusterThroughputTracker::Throughput> result;
	for (auto const& [throttlingId, throttlingIdThroughput] : throughputByThrottlingId) {
		auto const throughput = throttlingIdThroughput.getThroughput();

		if (throughput.readThroughput + throughput.writeThroughput > threshold) {
			result.emplace(throttlingId, throughput);
		}
	}
	return result;
}

int RKServerThroughputTracker::storageServersTracked() const {
	return throughputBySS.size();
}

int RKServerThroughputTracker::throttlingIdsTracked() const {
	return throughputByThrottlingId.size();
}

MockThroughputTracker::~MockThroughputTracker() = default;

IRKClusterThroughputTracker::Throughput MockThroughputTracker::getThroughput(ThrottlingId const& throttlingId) const {
	auto it = throughput.find(throttlingId);
	auto const readThroughput = (it == throughput.end()) ? 0.0 : it->second;
	return IRKClusterThroughputTracker::Throughput(readThroughput, 0.0);
}

ThrottlingIdMap<IRKClusterThroughputTracker::Throughput> MockThroughputTracker::getActiveThrottlingIds(
    double threshold) const {
	ThrottlingIdMap<IRKClusterThroughputTracker::Throughput> result;
	for (auto const& [throttlingId, readThroughputValue] : throughput) {
		if (readThroughputValue > threshold) {
			result.emplace(throttlingId,
			               IRKClusterThroughputTracker::Throughput(readThroughputValue, /*writeThroughput=*/0.0));
		}
	}
	return result;
}

int MockThroughputTracker::throttlingIdsTracked() const {
	return throughput.size();
}

void MockThroughputTracker::update(ThrottlingId const& throttlingId, double newReadThroughput) {
	throughput[throttlingId] = newReadThroughput;
}

void MockThroughputTracker::removeThrottlingId(ThrottlingId const& throttlingId) {
	throughput.erase(throttlingId);
}
