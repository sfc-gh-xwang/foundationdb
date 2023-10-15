/**
 * IQuotaThrottlingConfig.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once

#include "fdbclient/NativeAPI.actor.h"

#include <memory>

// This read-only interface is used for accessing quota throttling
// parameters
class IQuotaThrottlingConfig {
public:
	virtual uint64_t getClearCost() const = 0;
	virtual uint64_t getReadCost(uint64_t bytes) const = 0;
	virtual uint64_t getWriteCost(uint64_t bytes) const = 0;
};

// Exposes quota throttling parameters from a database. Takes
// ownership of a reference to the database.
class QuotaThrottlingConfig : public IQuotaThrottlingConfig {
	Database db;

public:
	QuotaThrottlingConfig(Database const& db);
	uint64_t getClearCost() const override;
	uint64_t getReadCost(uint64_t bytes) const override;
	uint64_t getWriteCost(uint64_t bytes) const override;
};

// Used for mocking the quota throttling config parameters when
// no database is available in a test environment.
class MockQuotaThrottlingConfig : public IQuotaThrottlingConfig {
	std::unique_ptr<class IQuotaCostCalculator> quotaCostCalculator;

public:
	MockQuotaThrottlingConfig();
	uint64_t getClearCost() const override;
	uint64_t getReadCost(uint64_t bytes) const override;
	uint64_t getWriteCost(uint64_t bytes) const override;
};
