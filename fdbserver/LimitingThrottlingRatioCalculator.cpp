/*
 * LimitingThrottlingRatioCalculator.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbserver/LimitingThrottlingRatioCalculator.h"
#include "fdbserver/Knobs.h"

namespace {

Optional<double> getMin(Optional<double> a, Optional<double> b) {
	if (!a.present()) {
		return b;
	} else if (!b.present()) {
		return a;
	} else {
		return std::min(a.get(), b.get());
	}
}

} // namespace

Optional<double> LimitingThrottlingRatioCalculator::getRatio(double throughputValue,
                                                             double storageQueueValue,
                                                             double durabilityLagValue) const {
	auto const throughputRatio = throughput.getRatio(throughputValue);
	auto const storageRatio = storageQueue.getRatio(storageQueueValue);
	auto const durabilityLagRatio = durabilityLag.getRatio(durabilityLagValue);

	Optional<double> result = throughputRatio;
	result = getMin(result, storageRatio);
	result = getMin(result, durabilityLagRatio);
	return result;
}

LimitingThrottlingRatioCalculator::Builder::Builder()
  : throughputTargetBps(SERVER_KNOBS->AUTO_TAG_THROTTLE_TARGET_SS_BPS),
    throughputSpringBps(SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_SS_BPS),
    storageTargetBytes(SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES),
    storageSpringBytes(SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_BYTES_STORAGE_SERVER),
    targetDurabilityLag(SERVER_KNOBS->AUTO_TAG_THROTTLE_DURABILITY_LAG_VERSIONS),
    springDurabilityLag(SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_DURABILITY_LAG) {}

LimitingThrottlingRatioCalculator::Builder& LimitingThrottlingRatioCalculator::Builder::setThroughputParameters(
    double targetBps,
    double springBps) {
	throughputTargetBps = targetBps;
	throughputSpringBps = springBps;
	return *this;
}

LimitingThrottlingRatioCalculator::Builder& LimitingThrottlingRatioCalculator::Builder::setStorageQueueParameters(
    int64_t targetBytes,
    int64_t springBytes) {
	storageTargetBytes = targetBytes;
	storageSpringBytes = springBytes;
	return *this;
}

LimitingThrottlingRatioCalculator::Builder& LimitingThrottlingRatioCalculator::Builder::setDurabilityLagParameters(
    int64_t targetVersions,
    int64_t springVersions) {
	targetDurabilityLag = targetVersions;
	springDurabilityLag = springVersions;
	return *this;
}

LimitingThrottlingRatioCalculator LimitingThrottlingRatioCalculator::Builder::build() const {
	return LimitingThrottlingRatioCalculator(throughputTargetBps,
	                                         throughputSpringBps,
	                                         storageTargetBytes,
	                                         storageSpringBytes,
	                                         targetDurabilityLag,
	                                         springDurabilityLag);
}
