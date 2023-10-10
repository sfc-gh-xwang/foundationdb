/*
 * LimitingThrottlingRatioCalculator.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once

#include "flow/Optional.h"

struct LimitingThrottlingRatioCalculator {
	template <class T>
	class TargetAndSpring {
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

	LimitingThrottlingRatioCalculator(double throughputTargetBps,
	                                  double throughputSpringBps,
	                                  int64_t storageTargetBytes,
	                                  int64_t storageSpringBytes,
	                                  int64_t targetDurabilityLag,
	                                  int64_t springDurabilityLag)
	  : throughput(throughputTargetBps, throughputSpringBps),
	    storageQueue(storageTargetBytes, storageSpringBytes),
	    durabilityLag(targetDurabilityLag, springDurabilityLag) {}

public:
	class Builder {
		double throughputTargetBps;
		double throughputSpringBps;
		int64_t storageTargetBytes;
		int64_t storageSpringBytes;
		int64_t targetDurabilityLag;
		int64_t springDurabilityLag;

	public:
		Builder();
		Builder& setThroughputParameters(double targetBps, double springBps);
		Builder& setStorageQueueParameters(int64_t targetBytes, int64_t springBytes);
		Builder& setDurabilityLagParameters(int64_t targetVersions, int64_t springVersions);
		LimitingThrottlingRatioCalculator build() const;
	};

	Optional<double> getRatio(double throughputValue, double storageQueueValue, double durabilityLagValue) const;
};
