/**
 * QuotaThrottlingConfig.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/IQuotaCostCalculator.h"
#include "fdbclient/QuotaThrottlingConfig.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "flow/actorcompiler.h" // must be last include

class QuotaThrottlingConfigImpl {
public:
	ACTOR static Future<Void> update(QuotaThrottlingConfig* self, Reference<GlobalConfig const> globalConfig) {
		wait(globalConfig->onInitialized());
		loop {
			auto const fungibilityRatio = globalConfig->get<double>(
			    quotaThrottlingFungibilityRatio, CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO);
			auto const pageSize =
			    globalConfig->get<int64_t>(quotaThrottlingPageSize, CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
			self->quotaCostCalculator =
			    IQuotaCostCalculator::create(CLIENT_KNOBS->QUOTA_THROTTLING_COST_FORMULA, fungibilityRatio, pageSize);
			wait(globalConfig->onChange());
		}
	}
};

QuotaThrottlingConfig::QuotaThrottlingConfig() : quotaCostCalculator(getQuotaCostCalculatorFromKnobs()) {}

QuotaThrottlingConfig::~QuotaThrottlingConfig() = default;

void QuotaThrottlingConfig::startUpdater(Reference<GlobalConfig const> globalConfig) {
	ASSERT(!updaterFuture.isValid());
	updaterFuture = QuotaThrottlingConfigImpl::update(this, globalConfig);
}

uint64_t QuotaThrottlingConfig::getClearCost() const {
	return quotaCostCalculator->getClearCost();
}

uint64_t QuotaThrottlingConfig::getReadCost(uint64_t bytes) const {
	return quotaCostCalculator->getReadCost(bytes);
}

uint64_t QuotaThrottlingConfig::getWriteCost(uint64_t bytes) const {
	return quotaCostCalculator->getWriteCost(bytes);
}
