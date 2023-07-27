/**
 * RKClientThroughputTracker.h
 */

#include "fdbserver/IRKClusterThroughputTracker.h"
#include "fdbserver/Knobs.h"

RKClientThroughputTracker::ThroughputSmoother::ThroughputSmoother()
  : smoother(SERVER_KNOBS->CLIENT_THROUGHPUT_TRACKER_DATA_FOLDING_TIME,
             SERVER_KNOBS->CLIENT_THROUGHPUT_TRACKER_RATE_FOLDING_TIME),
    lastUpdated(now()) {}

void RKClientThroughputTracker::ThroughputSmoother::addThroughput(double newThroughput) {
	smoother.addDelta(newThroughput);
	lastUpdated = now();
}

double RKClientThroughputTracker::ThroughputSmoother::smoothRate() const {
	return smoother.smoothRate();
}

bool RKClientThroughputTracker::ThroughputSmoother::shouldForget(double expirationCheckInterval) const {
	return (now() > lastUpdated + expirationCheckInterval &&
	        smoother.smoothRate() < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_THROUGHPUT_THRESHOLD);
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

double RKClientThroughputTracker::getThroughput(ThrottlingId const& throttlingId) const {
	auto it = throughput.find(throttlingId);
	if (it == throughput.end()) {
		return 0.0;
	} else {
		return it->second.smoothRate();
	}
}

ThrottlingIdMap<double> RKClientThroughputTracker::getActiveThrottlingIds(double threshold) const {
	ThrottlingIdMap<double> result;
	for (auto const& [throttlingId, throughputSmoother] : throughput) {
		auto const smoothThroughput = throughputSmoother.smoother.smoothRate();
		if (smoothThroughput > threshold) {
			result.emplace(throttlingId, smoothThroughput);
		}
	}
	return result;
}

int RKClientThroughputTracker::throttlingIdsTracked() const {
	return throughput.size();
}

void RKClientThroughputTracker::update(ThrottlingIdMap<uint64_t> const& newThroughput) {
	for (auto const& [throttlingId, newPerThrottlingIdThroughput] : newThroughput) {
		throughput[throttlingId].addThroughput(newPerThrottlingIdThroughput);
	}
	if (now() > lastExpirationCheck + expirationCheckInterval) {
		expireThrottlingIds();
	}
}
