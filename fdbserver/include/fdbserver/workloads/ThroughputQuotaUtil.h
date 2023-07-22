/**
 * ThroughputQuotaUtil.h
 */

#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Tenant.h"
#include "flow/flow.h"

/**
 * The ThroughputQuotaUtil namespace provides several functions
 * that are useful for testing throughput quotas.
 */
namespace ThroughputQuotaUtil {

Future<Void> setTagQuota(Database const&, TransactionTag const&, int64_t reservedQuota, int64_t totalQuota);

Future<Void> removeTagQuota(Database const&, TransactionTag const&);

Future<Void> setTenantGroupQuota(Database const&, TenantGroupName const&, int64_t reservedQuota, int64_t totalQuota);

Future<Void> removeTenantGroupQuota(Database const&, TenantGroupName const&);

Standalone<StringRef> getThrottlingIdName(StringRef const& prefix, int index);

} // namespace ThroughputQuotaUtil
