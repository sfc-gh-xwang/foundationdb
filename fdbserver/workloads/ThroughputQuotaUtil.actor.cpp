/**
 * ThroughputQuotaUtil.actor.cpp
 */

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ThrottleApi.h"
#include "fdbclient/UpdateStream.h"
#include "fdbserver/workloads/ThroughputQuotaUtil.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

ACTOR static Future<Void> _setTagQuota(Database db, TransactionTag tag, int64_t reservedQuota, int64_t totalQuota) {
	state Reference<ReadYourWritesTransaction> tr = db->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(ThrottleApi::setTagQuotaWithUpdateStream(tr, tag, reservedQuota, totalQuota));
			wait(tr->commit());
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR static Future<Void> _removeTagQuota(Database db, TransactionTag tag) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(ThrottleApi::removeTagQuotaWithUpdateStream(tr, tag));
			wait(tr->commit());
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR static Future<Void> _setTenantGroupQuota(Database db,
                                               TenantGroupName tenantGroup,
                                               int64_t reservedQuota,
                                               int64_t totalQuota) {
	state Reference<ReadYourWritesTransaction> tr = db->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			ThrottleApi::ThroughputQuotaValue throughputQuotaValue;
			throughputQuotaValue.reservedQuota = reservedQuota;
			throughputQuotaValue.totalQuota = totalQuota;
			if (!throughputQuotaValue.isValid()) {
				throw invalid_throttle_quota_value();
			}
			wait(UpdateStream::update(tr,
			                          TenantMetadata::throughputQuota().packKey(tenantGroup),
			                          throughputQuotaValue.pack().pack(),
			                          throughputQuotaUpdatePrefix));
			wait(tr->commit());
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR static Future<Void> _removeTenantGroupQuota(Database db, TenantGroupName tenantGroup) {
	state Reference<ReadYourWritesTransaction> tr = db->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			wait(UpdateStream::update(tr,
			                          TenantMetadata::throughputQuota().packKey(tenantGroup),
			                          Optional<Value>(),
			                          throughputQuotaUpdatePrefix));
			wait(tr->commit());
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

} // namespace

namespace ThroughputQuotaUtil {

Future<Void> setTagQuota(Database const& db, TransactionTag const& tag, int64_t reservedQuota, int64_t totalQuota) {
	return _setTagQuota(db, tag, reservedQuota, totalQuota);
}

Future<Void> removeTagQuota(Database const& db, TransactionTag const& tag) {
	return _removeTagQuota(db, tag);
}

Future<Void> setTenantGroupQuota(Database const& db,
                                 TenantGroupName const& tenantGroup,
                                 int64_t reservedQuota,
                                 int64_t totalQuota) {
	return _setTenantGroupQuota(db, tenantGroup, reservedQuota, totalQuota);
}

Future<Void> removeTenantGroupQuota(Database const& db, TenantGroupName const& tenantGroup) {
	return _removeTenantGroupQuota(db, tenantGroup);
}

TransactionTag getTransactionTag(StringRef const& prefix, int index) {
	return TransactionTag(fmt::format("{}", index)).withPrefix(prefix);
}

} // namespace ThroughputQuotaUtil
