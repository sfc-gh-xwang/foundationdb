/*
 * ThrottleApi.cpp
 */

#include "fdbclient/SystemData.h"
#include "fdbclient/ThrottleApi.h"
#include "fdbclient/Tuple.h"

// Used to store per-tag throughput quotas:
KeyRangeRef const tagQuotaKeys = KeyRangeRef("\xff/tagQuota/"_sr, "\xff/tagQuota0"_sr);
KeyRef const tagQuotaPrefix = tagQuotaKeys.begin;

// Used to store updates to per-tag throughput quotas:
KeyRef const throughputQuotaUpdatePrefix = "\xff/throughputQuotaUpdates/"_sr;

Key ThrottleApi::getTagQuotaKey(TransactionTagRef tag) {
	return tag.withPrefix(tagQuotaPrefix);
}

bool ThrottleApi::ThroughputQuotaValue::isValid() const {
	return reservedQuota <= totalQuota && reservedQuota >= 0;
}

Tuple ThrottleApi::ThroughputQuotaValue::pack() const {
	return Tuple::makeTuple(reservedQuota, totalQuota);
}

ThrottleApi::ThroughputQuotaValue ThrottleApi::ThroughputQuotaValue::unpack(Tuple const& tuple) {
	if (tuple.size() != 2) {
		throw invalid_throttle_quota_value();
	}
	ThroughputQuotaValue result;
	try {
		result.reservedQuota = tuple.getInt(0);
		result.totalQuota = tuple.getInt(1);
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "ThroughputQuotaValueFailedToDeserialize").error(e);
		throw invalid_throttle_quota_value();
	}
	if (!result.isValid()) {
		TraceEvent(SevWarnAlways, "ThrougputQuotaValueInvalidQuotas")
		    .detail("ReservedQuota", result.reservedQuota)
		    .detail("TotalQuota", result.totalQuota);
		throw invalid_throttle_quota_value();
	}
	return result;
}

bool ThrottleApi::ThroughputQuotaValue::operator==(ThrottleApi::ThroughputQuotaValue const& rhs) const {
	return reservedQuota == rhs.reservedQuota && totalQuota == rhs.totalQuota;
}
