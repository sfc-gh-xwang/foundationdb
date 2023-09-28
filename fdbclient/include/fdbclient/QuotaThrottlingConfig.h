/**
 * QuotaThrottlingConfig.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "flow/flow.h"

class QuotaThrottlingConfig {
	friend class QuotaThrottlingConfigImpl;

	int64_t pageSize;
	Future<Void> updaterFuture;

public:
	QuotaThrottlingConfig(class GlobalConfig const& globalConfig);
	int64_t getPageSize() const;
};
