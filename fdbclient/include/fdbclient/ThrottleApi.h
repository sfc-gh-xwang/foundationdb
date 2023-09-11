/**
 * ThrottleApi.h
 */

#pragma once

#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/flow.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ThroughputQuotaValue.h"
#include "fdbclient/Tuple.h"
#include "fdbclient/UpdateStream.h"
#include "fdbclient/SystemData.h"

namespace ThrottleApi {

Key getTagQuotaKey(TransactionTagRef);

template <class Tr>
void setTagQuota(Reference<Tr> tr, TransactionTagRef tag, int64_t reservedQuota, int64_t totalQuota) {
	ThroughputQuotaValue tagQuotaValue;
	tagQuotaValue.reservedQuota = reservedQuota;
	tagQuotaValue.totalQuota = totalQuota;
	if (!tagQuotaValue.isValid()) {
		throw invalid_throttle_quota_value();
	}
	tr->set(getTagQuotaKey(tag), tagQuotaValue.pack().pack());
}

template <class Tr>
void removeTagQuota(Reference<Tr> tr, TransactionTagRef tag) {
	tr->clear(getTagQuotaKey(tag));
}

template <class Tr>
Future<Void> setTagQuotaWithUpdateStream(Reference<Tr> tr,
                                         TransactionTagRef tag,
                                         int64_t reservedQuota,
                                         int64_t totalQuota) {
	ThroughputQuotaValue tagQuotaValue;
	tagQuotaValue.reservedQuota = reservedQuota;
	tagQuotaValue.totalQuota = totalQuota;
	if (!tagQuotaValue.isValid()) {
		throw invalid_throttle_quota_value();
	}
	return UpdateStream::update(tr, getTagQuotaKey(tag), tagQuotaValue.pack().pack(), throughputQuotaUpdatePrefix);
}

template <class Tr>
Future<Void> removeTagQuotaWithUpdateStream(Reference<Tr> tr, TransactionTagRef tag) {
	return UpdateStream::update(tr, getTagQuotaKey(tag), Optional<Value>(), throughputQuotaUpdatePrefix);
}

} // namespace ThrottleApi
