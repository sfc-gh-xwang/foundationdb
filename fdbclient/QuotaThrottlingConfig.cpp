/**
 * QuotaThrottlingConfig.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/IQuotaThrottlingConfig.h"
#include "fdbclient/IQuotaCostCalculator.h"

IQuotaThrottlingConfig::~IQuotaThrottlingConfig() = default;

QuotaThrottlingConfig::QuotaThrottlingConfig(Database const& db) : db(db) {}

uint64_t QuotaThrottlingConfig::getClearCost() const {
	return db->getClearOperationCost();
}

uint64_t QuotaThrottlingConfig::getReadCost(uint64_t bytes) const {
	return db->getReadOperationCost(bytes);
}

uint64_t QuotaThrottlingConfig::getWriteCost(uint64_t bytes) const {
	return db->getWriteOperationCost(bytes);
}

double QuotaThrottlingConfig::getReadSampleRate() const {
	return db->getReadSampleRate();
}

MockQuotaThrottlingConfig::MockQuotaThrottlingConfig()
  : quotaCostCalculator(getQuotaCostCalculatorFromKnobs()),
    readSampleRate(CLIENT_KNOBS->READ_TAG_SAMPLE_RATE) {}

MockQuotaThrottlingConfig::~MockQuotaThrottlingConfig() = default;

uint64_t MockQuotaThrottlingConfig::getClearCost() const {
	return quotaCostCalculator->getClearCost();
}

uint64_t MockQuotaThrottlingConfig::getReadCost(uint64_t bytes) const {
	return quotaCostCalculator->getReadCost(bytes);
}

uint64_t MockQuotaThrottlingConfig::getWriteCost(uint64_t bytes) const {
	return quotaCostCalculator->getWriteCost(bytes);
}

double MockQuotaThrottlingConfig::getReadSampleRate() const {
	return readSampleRate;
}
