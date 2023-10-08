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

LimitingThrottlingRatioCalculator LimitingThrottlingRatioCalculator::fromKnobs() {
	return LimitingThrottlingRatioCalculator(SERVER_KNOBS->AUTO_TAG_THROTTLE_TARGET_SS_BPS,
	                                         SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_SS_BPS,
	                                         SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES,
	                                         SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_BYTES_STORAGE_SERVER,
	                                         SERVER_KNOBS->AUTO_TAG_THROTTLE_DURABILITY_LAG_VERSIONS,
	                                         SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_DURABILITY_LAG);
}
