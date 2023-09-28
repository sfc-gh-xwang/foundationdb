/**
 * QuotaThrottlingConfig.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/QuotaThrottlingConfig.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "flow/actorcompiler.h" // must be last include

class QuotaThrottlingConfigImpl {
public:
	ACTOR static Future<Void> update(QuotaThrottlingConfig* self, GlobalConfig const* globalConfig) {
		wait(globalConfig->onInitialized());
		loop {
			self->pageSize =
			    globalConfig->get<int64_t>(quotaThrottlingPageSize, CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
			wait(globalConfig->onChange());
		}
	}
};

QuotaThrottlingConfig::QuotaThrottlingConfig(GlobalConfig const& globalConfig)
  : pageSize(CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE) {
	updaterFuture = QuotaThrottlingConfigImpl::update(this, &globalConfig);
}

int64_t QuotaThrottlingConfig::getPageSize() const {
	return pageSize;
}
