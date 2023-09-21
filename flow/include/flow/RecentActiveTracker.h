/*
 * RecentActiveTracker.h
 * Utility class for tracking how many operations of a certain type were active in a recent time interval.
 * This is primarily intended for use in metrics classes.
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef __FLOW_RECENT_ACTIVE_TRACKER_H__
#define __FLOW_RECENT_ACTIVE_TRACKER_H__

#pragma once

namespace RecentActive {
struct Tracker {
	int64_t activeThisInterval;
	int64_t epoch;

	Tracker() : activeThisInterval(0), epoch(0) {}

	int64_t getCountAndReset() {
		int64_t count = activeThisInterval;
		epoch++;
		activeThisInterval = 0;
		return count;
	}
};

struct Handle {
	int64_t lastEpochCounted;

	Handle() : lastEpochCounted(-1) {}

	void count(Tracker* tracker) {
		if (lastEpochCounted < tracker->epoch) {
			tracker->activeThisInterval++;
			lastEpochCounted = tracker->epoch;
		}
	}
};

} // namespace RecentActive

#endif