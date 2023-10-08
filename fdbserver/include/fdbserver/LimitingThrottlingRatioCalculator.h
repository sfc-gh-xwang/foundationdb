/*
 * LimitingThrottlingRatioCalculator.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once

#include "fdbserver/Knobs.h"
#include "flow/Optional.h"

struct LimitingThrottlingRatioCalculator {

	static Optional<double> getMin(Optional<double> a, Optional<double> b) {
		if (!a.present()) {
			return b;
		} else if (!b.present()) {
			return a;
		} else {
			return std::min(a.get(), b.get());
		}
	}

	template <class T>
	struct TargetAndSpring {
		T target, spring;

	public:
		TargetAndSpring(T target, T spring) : target(target), spring(spring) {}
		Optional<double> getRatio(T value) const {
			// TODO: Remove duplicate calculation from RKRateUpdater::updateRate
			double inverseResult = std::min(2.0, (value - target + spring) / static_cast<double>(spring));
			if (inverseResult > 0) {
				return 1.0 / inverseResult;
			} else {
				return {};
			}
		}
	};

	TargetAndSpring<double> throughput;
	TargetAndSpring<int64_t> storageQueue;
	TargetAndSpring<int64_t> durabilityLag;

public:
	LimitingThrottlingRatioCalculator(double throughputTargetBps,
	                                  double throughputSpringBps,
	                                  int64_t storageTargetBytes,
	                                  int64_t storageSpringBytes,
	                                  int64_t targetDurabilityLag,
	                                  int64_t springDurabilityLag)
	  : throughput(throughputTargetBps, throughputSpringBps),
	    storageQueue(storageTargetBytes, storageSpringBytes),
	    durabilityLag(targetDurabilityLag, springDurabilityLag) {}

	Optional<double> getRatio(double throughputValue, double storageQueueValue, double durabilityLagValue) const {
		auto const throughputRatio = throughput.getRatio(throughputValue);
		auto const storageRatio = storageQueue.getRatio(storageQueueValue);
		auto const durabilityLagRatio = durabilityLag.getRatio(durabilityLagValue);

		Optional<double> result = throughputRatio;
		result = getMin(result, storageRatio);
		result = getMin(result, durabilityLagRatio);
		return result;
	}

	static LimitingThrottlingRatioCalculator fromKnobs() {
		return LimitingThrottlingRatioCalculator(SERVER_KNOBS->AUTO_TAG_THROTTLE_TARGET_SS_BPS,
		                                         SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_SS_BPS,
		                                         SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES,
		                                         SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_BYTES_STORAGE_SERVER,
		                                         SERVER_KNOBS->AUTO_TAG_THROTTLE_DURABILITY_LAG_VERSIONS,
		                                         SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_DURABILITY_LAG);
	}
};
