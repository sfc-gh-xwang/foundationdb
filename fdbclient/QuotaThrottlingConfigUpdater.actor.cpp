/**
 * QuotaThrottlingConfigUpdater.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/IQuotaCostCalculator.h"
#include "fdbclient/QuotaThrottlingConfigUpdater.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "flow/actorcompiler.h" // must be last include

class QuotaThrottlingConfigUpdaterImpl {
	static std::string getQuotaCostFormula(GlobalConfig const& globalConfig) {
		Reference<ConfigValue const> const configValue = globalConfig.get(quotaThrottlingCostFormula);
		try {
			if (configValue.isValid() && configValue->value.has_value()) {
				return std::any_cast<StringRef>(configValue->value).toString();
			} else {
				return CLIENT_KNOBS->QUOTA_THROTTLING_COST_FORMULA;
			}
		} catch (...) {
			TraceEvent(SevWarnAlways, "ErrorGettingQuotaCostFormula");
			throw;
		}
	}

public:
	ACTOR static Future<Void> update(QuotaThrottlingConfigUpdater* self, Reference<GlobalConfig const> globalConfig) {
		wait(globalConfig->onInitialized());
		loop {
			auto const fungibilityRatio = globalConfig->get<double>(
			    quotaThrottlingFungibilityRatio, CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO);
			auto const pageSize =
			    globalConfig->get<int64_t>(quotaThrottlingPageSize, CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
			auto const quotaCostFormula = getQuotaCostFormula(*globalConfig);
			self->quotaCostCalculator = IQuotaCostCalculator::create(quotaCostFormula, fungibilityRatio, pageSize);
			self->readSampleRate =
			    globalConfig->get<double>(quotaThrottlingReadSampleRate, CLIENT_KNOBS->READ_TAG_SAMPLE_RATE);
			self->commitSampleCost =
			    globalConfig->get<int64_t>(quotaThrottlingCommitSampleCost, CLIENT_KNOBS->COMMIT_SAMPLE_COST);
			wait(globalConfig->onChange());
		}
	}
};

QuotaThrottlingConfigUpdater::QuotaThrottlingConfigUpdater() : quotaCostCalculator(getQuotaCostCalculatorFromKnobs()) {}

QuotaThrottlingConfigUpdater::~QuotaThrottlingConfigUpdater() = default;

void QuotaThrottlingConfigUpdater::start(Reference<GlobalConfig const> globalConfig) {
	ASSERT(!updaterFuture.isValid());
	updaterFuture = QuotaThrottlingConfigUpdaterImpl::update(this, globalConfig);
}

uint64_t QuotaThrottlingConfigUpdater::getClearCost() const {
	return quotaCostCalculator->getClearCost();
}

uint64_t QuotaThrottlingConfigUpdater::getReadCost(uint64_t bytes) const {
	return quotaCostCalculator->getReadCost(bytes);
}

uint64_t QuotaThrottlingConfigUpdater::getWriteCost(uint64_t bytes) const {
	return quotaCostCalculator->getWriteCost(bytes);
}

double QuotaThrottlingConfigUpdater::getReadSampleRate() const {
	return readSampleRate;
}

uint64_t QuotaThrottlingConfigUpdater::getCommitSampleCost() const {
	return commitSampleCost;
}
