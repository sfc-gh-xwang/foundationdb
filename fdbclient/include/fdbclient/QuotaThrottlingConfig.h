/**
 * QuotaThrottlingConfig.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once

#include "flow/flow.h"

class QuotaThrottlingConfig {
	friend class QuotaThrottlingConfigImpl;

	std::unique_ptr<class IQuotaCostCalculator const> quotaCostCalculator;
	Future<Void> updaterFuture;

public:
	QuotaThrottlingConfig();
	~QuotaThrottlingConfig();
	void startUpdater(Reference<class GlobalConfig const>);
	uint64_t getClearCost() const;
	uint64_t getReadCost(uint64_t bytes) const;
	uint64_t getWriteCost(uint64_t bytes) const;
};
