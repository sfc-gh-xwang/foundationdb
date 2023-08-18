/*
 * ConsistencyScanInterface.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/ConsistencyScanInterface.actor.h"
#include "fdbclient/Knobs.h"

bool ConsistencyScanState::RoundStats::shouldStartNewRound(const ConsistencyScanState::Config& config,
                                                           Version currentVersion) {
	// if the current round is running, but the configured minStartVersion is newer than the current round, it
	// should start a new round.
	// TODO: when configuring minStartVersion (currently not supported), it should not be a future version,
	//  otherwise it will keep restarting the new round.
	if (!complete && startVersion != 0 && startVersion < config.minStartVersion) {
		return true;
	}
	// if the current round is complete and based on minRoundTimeSeconds, it should start a new round.
	// Note: it uses version instead of time to avoid local clock time skew on certain server
	if (complete &&
	    currentVersion >= startVersion + (config.minRoundTimeSeconds * CLIENT_KNOBS->CORE_VERSIONSPERSECOND)) {
		return true;
	}
	return false;
}
