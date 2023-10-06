/*
 * ConsistencyScanInterface.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/ConsistencyScanInterface.actor.h"
#include "fdbclient/Knobs.h"

// Returns how many seconds to wait before starting the next round or non-present if a new round
// should not be started
Optional<int64_t> ConsistencyScanState::RoundStats::nextRoundStartDelaySeconds(
    const ConsistencyScanState::Config& config,
    Version currentVersion) {
	// If the feature is not enabled, then should not start next round
	if (!config.enabled) {
		return Optional<int64_t>();
	}
	// if the current round is running, but the configured minStartVersion is newer than the current round, it
	// should start a new round.
	// TODO: when configuring minStartVersion (currently not supported), it should not be a future version,
	//  otherwise it will keep restarting the new round.
	if (!complete && startVersion != 0 && startVersion < config.minStartVersion) {
		return 0;
	}
	// if the current round is complete and at least minRoundTimeSeconds has elapsed, then start a new round.
	// Note: it uses version instead of time to avoid local clock time skew on certain server
	if (complete) {
		Version nextRoundStartVersion =
		    startVersion + (config.minRoundTimeSeconds * CLIENT_KNOBS->CORE_VERSIONSPERSECOND);
		int64_t delayTime =
		    std::max((nextRoundStartVersion - currentVersion) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND, (int64_t)0);
		return delayTime;
	}

	return Optional<int64_t>();
}
