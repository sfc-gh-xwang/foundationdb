/**
 * QuotaThrottlingConfigUpdater.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once

#include "flow/flow.h"

// Used internally by DatabaseContext to update quota throttling
// parameters based on the global configuration. Default parameter
// values are knob-determined.
//
// Relies on the DatabaseContext object used by GlobalConfig being
// alive. Not to be used externally.
class QuotaThrottlingConfigUpdater {
	friend class QuotaThrottlingConfigUpdaterImpl;

	std::unique_ptr<class IQuotaCostCalculator const> quotaCostCalculator;
	double readSampleRate;
	uint64_t commitSampleCost;
	Future<Void> updaterFuture;

public:
	QuotaThrottlingConfigUpdater();
	~QuotaThrottlingConfigUpdater();
	void start(Reference<class GlobalConfig const>);
	uint64_t getClearCost() const;
	uint64_t getReadCost(uint64_t bytes) const;
	uint64_t getWriteCost(uint64_t bytes) const;
	double getReadSampleRate() const;
	uint64_t getCommitSampleCost() const;
};
