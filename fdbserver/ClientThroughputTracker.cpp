/**
 * ClientThroughputTracker.h
 */

#include "fdbserver/IRKThroughputTracker.h"
#include "fdbserver/Knobs.h"

ClientThroughputTracker::ThroughputSmoother::ThroughputSmoother()
  : smoother(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME,
             SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME),
    lastUpdated(now()) {}

void ClientThroughputTracker::ThroughputSmoother::addThroughput(double newThroughput) {
	smoother.addDelta(newThroughput);
	lastUpdated = now();
}

double ClientThroughputTracker::ThroughputSmoother::smoothRate() const {
	return smoother.smoothRate();
}

bool ClientThroughputTracker::ThroughputSmoother::shouldForget(double expirationCheckInterval) const {
	return (now() > lastUpdated + expirationCheckInterval &&
	        smoother.smoothRate() < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_THROUGHPUT_THRESHOLD);
}

void ClientThroughputTracker::expireThrottlingIds() {
	auto it = throughput.begin();
	while (it != throughput.end()) {
		auto const& [_, throughputSmoother] = *it;
		if (throughputSmoother.shouldForget(expirationCheckInterval)) {
			it = throughput.erase(it);
		} else {
			++it;
		}
	}
	lastExpirationCheck = now();
}

ClientThroughputTracker::ClientThroughputTracker(double expirationCheckInterval)
  : lastExpirationCheck(now()),
    expirationCheckInterval(expirationCheckInterval) {}

ClientThroughputTracker::~ClientThroughputTracker() = default;

double ClientThroughputTracker::getThroughput(ThrottlingId const& throttlingId) const {
	auto it = throughput.find(throttlingId);
	if (it == throughput.end()) {
		return 0.0;
	} else {
		return it->second.smoothRate();
	}
}

void ClientThroughputTracker::update(ThrottlingIdMap<uint64_t> const& newThroughput) {
	for (auto const& [throttlingId, newPerThrottlingIdThroughput] : newThroughput) {
		throughput[throttlingId].addThroughput(newPerThrottlingIdThroughput);
	}
	if (now() > lastExpirationCheck + expirationCheckInterval) {
		expireThrottlingIds();
	}
}
