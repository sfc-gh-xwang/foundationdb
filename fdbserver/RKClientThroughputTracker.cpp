/**
 * RKClientThroughputTracker.h
 */

#include "fdbserver/IRKClusterThroughputTracker.h"
#include "fdbserver/Knobs.h"

RKClientThroughputTracker::ThroughputSmoother::ThroughputSmoother()
  : readSmoother(SERVER_KNOBS->CLIENT_THROUGHPUT_TRACKER_DATA_FOLDING_TIME,
                 SERVER_KNOBS->CLIENT_THROUGHPUT_TRACKER_RATE_FOLDING_TIME),
    writeSmoother(SERVER_KNOBS->CLIENT_THROUGHPUT_TRACKER_DATA_FOLDING_TIME,
                  SERVER_KNOBS->CLIENT_THROUGHPUT_TRACKER_RATE_FOLDING_TIME),
    lastUpdated(now()) {}

void RKClientThroughputTracker::ThroughputSmoother::addThroughput(ThroughputStats const& newThroughput) {
	readSmoother.addDelta(newThroughput.readThroughput);
	writeSmoother.addDelta(newThroughput.writeThroughput);
	lastUpdated = now();
}

double RKClientThroughputTracker::ThroughputSmoother::smoothReadRate() const {
	return readSmoother.smoothRate();
}

double RKClientThroughputTracker::ThroughputSmoother::smoothWriteRate() const {
	return writeSmoother.smoothRate();
}

bool RKClientThroughputTracker::ThroughputSmoother::shouldForget(double expirationCheckInterval) const {
	return (now() > lastUpdated + expirationCheckInterval &&
	        readSmoother.smoothRate() + writeSmoother.smoothRate() <
	            SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_THROUGHPUT_THRESHOLD);
}

void RKClientThroughputTracker::expireThrottlingIds() {
	boost::unordered::erase_if(throughput, [expirationCheckInterval = this->expirationCheckInterval](auto const& kv) {
		return kv.second.shouldForget(expirationCheckInterval);
	});
	lastExpirationCheck = now();
}

RKClientThroughputTracker::RKClientThroughputTracker(double expirationCheckInterval)
  : lastExpirationCheck(now()),
    expirationCheckInterval(expirationCheckInterval) {}

RKClientThroughputTracker::~RKClientThroughputTracker() = default;

IRKClusterThroughputTracker::Throughput RKClientThroughputTracker::getThroughput(
    ThrottlingId const& throttlingId) const {
	auto it = throughput.find(throttlingId);
	if (it == throughput.end()) {
		return IRKClusterThroughputTracker::Throughput(0.0, 0.0);
	} else {
		return IRKClusterThroughputTracker::Throughput(it->second.smoothReadRate(), it->second.smoothWriteRate());
	}
}

IRKClusterThroughputTracker::Throughput RKClientThroughputTracker::getAggregateThroughput() const {
	IRKClusterThroughputTracker::Throughput result{ 0.0, 0.0 };
	for (const auto& [_, smoother] : throughput) {
		result.readThroughput += smoother.smoothReadRate();
		result.writeThroughput += smoother.smoothWriteRate();
	}

	return result;
}

ThrottlingIdMap<IRKClusterThroughputTracker::Throughput> RKClientThroughputTracker::getActiveThrottlingIds(
    double threshold) const {
	ThrottlingIdMap<IRKClusterThroughputTracker::Throughput> result;
	for (auto const& [throttlingId, throughputSmoother] : throughput) {
		auto const smoothReadThroughput = throughputSmoother.readSmoother.smoothRate();
		auto const smoothWriteThroughput = throughputSmoother.writeSmoother.smoothRate();
		if (smoothReadThroughput + smoothWriteThroughput > threshold) {
			result.emplace(throttlingId,
			               IRKClusterThroughputTracker::Throughput(smoothReadThroughput, smoothWriteThroughput));
		}
	}
	return result;
}

int RKClientThroughputTracker::throttlingIdsTracked() const {
	return throughput.size();
}

void RKClientThroughputTracker::update(ThrottlingIdMap<ThroughputStats> const& newThroughput) {
	for (auto const& [throttlingId, newPerThrottlingIdThroughput] : newThroughput) {
		throughput[throttlingId].addThroughput(newPerThrottlingIdThroughput);
	}
	if (now() > lastExpirationCheck + expirationCheckInterval) {
		expireThrottlingIds();
	}
}
