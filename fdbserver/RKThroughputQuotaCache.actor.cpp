/**
 * RKThroughputQuotaCache.actor.cpp
 */

#include "fdbclient/UpdateStream.h"
#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // must be last include

class RKThroughputQuotaCacheImpl {

	ACTOR static Future<Void> expireOldThrottlingIds(RKThroughputQuotaCache* self) {
		loop {
			wait(delayJittered(self->expirationCheckInterval));
			auto it = self->quotas.begin();
			while (it != self->quotas.end()) {
				auto const lastActiveTime = it->second.lastActiveTime;
				if (lastActiveTime + self->expirationThreshold < now()) {
					it = self->quotas.erase(it);
				} else {
					++it;
				}
			}
		}
	}

	// Reads and deserializes the quota for the provided throttlingId with the provided transaction
	ACTOR static Future<Optional<ThrottleApi::ThroughputQuotaValue>> getQuota(Reference<ReadYourWritesTransaction> tr,
	                                                                          ThrottlingId throttlingId) {
		if (throttlingId.isTag()) {
			ValueReadResult value = wait(tr->get(throttlingId.getTag().withPrefix(tagQuotaPrefix)));
			if (value.contents().present()) {
				return ThrottleApi::ThroughputQuotaValue::unpack(Tuple::unpack(value.contents().get()));
			} else {
				return Optional<ThrottleApi::ThroughputQuotaValue>();
			}
		} else {
			Optional<ThrottleApi::ThroughputQuotaValue> result =
			    wait(TenantMetadata::throughputQuota().get(tr, throttlingId.getTenantGroup()));
			return result;
		}
	}

	// Update the lastActiveTime for the provided throttlingId, and if it is not currently being tracked,
	// read and store its quota.
	ACTOR static Future<Void> markActive(RKThroughputQuotaCache* self, ThrottlingId throttlingId) {
		state ThrottlingIdMap<RKThroughputQuotaCache::QuotaData>::iterator it;
		state Reference<ReadYourWritesTransaction> tr;
		auto const [_it, inserted] = self->quotas.try_emplace(throttlingId);
		it = _it;
		it->second.lastActiveTime = now();
		if (inserted) {
			tr = makeReference<ReadYourWritesTransaction>(self->db);
			loop {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				try {
					wait(store(it->second.quota, getQuota(tr, throttlingId)));
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}
		}
		return Void();
	}

	// TODO: Read multiple at once
	ACTOR static Future<Void> readSnapshot(RKThroughputQuotaCache* self) {
		state ThrottlingIdMap<RKThroughputQuotaCache::QuotaData> newSnapshot = self->quotas;
		state ThrottlingIdMap<RKThroughputQuotaCache::QuotaData>::iterator it = newSnapshot.begin();
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);
		while (it != self->quotas.end()) {
			loop {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				try {
					auto& [throttlingId, quotaData] = *it;
					wait(store(quotaData.quota, getQuota(tr, throttlingId)));
					break;
				} catch (Error& e) {
					TraceEvent(SevDebug, "RKThroughputQuotaCacheReadSnapshotError").error(e);
					wait(tr->onError(e));
				}
			}
			++it;
		}
		self->quotas = std::move(newSnapshot);
		return Void();
	}

public:
	ACTOR static Future<Void> run(RKThroughputQuotaCache* self) {
		state Reference<UpdateStream> updateStream;
		state FutureStream<UpdateStream::Update> updates;
		state Future<Void> cleanerActor = expireOldThrottlingIds(self);
		loop {
			updateStream = makeReference<UpdateStream>(throughputQuotaUpdatePrefix,
			                                           SERVER_KNOBS->THROUGHPUT_QUOTA_CACHE_BATCH_SIZE,
			                                           /*accessSystemKeys=*/true,
			                                           /*immediatePriority=*/true);
			wait(success(updateStream->start(self->db)));
			wait(readSnapshot(self));
			updates = updateStream->getUpdates(self->db);

			try {
				loop choose {
					when(UpdateStream::Update update = waitNext(updates)) {
						self->applyUpdate(update.key, update.value);
					}
					when(ThrottlingId throttlingId = waitNext(self->activeThrottlingIdStream.getFuture())) {
						wait(markActive(self, throttlingId));
					}
				}
			} catch (Error& e) {
				TraceEvent("RKThroughputQuotaCacheRunError").error(e);
				if (e.code() == error_code_restart_update_stream) {
					continue;
				} else {
					throw e;
				}
			}
		}
	}
}; // class RKThroughputQuotaCacheImpl

RKThroughputQuotaCache::RKThroughputQuotaCache(UID id,
                                               Database db,
                                               double throughputThreshold,
                                               double expirationCheckInterval,
                                               double expirationThreshold)
  : id(id), db(db), throughputThreshold(throughputThreshold), expirationCheckInterval(expirationCheckInterval),
    expirationThreshold(expirationThreshold) {}

RKThroughputQuotaCache::~RKThroughputQuotaCache() = default;

Optional<int64_t> RKThroughputQuotaCache::getTotalQuota(ThrottlingId const& throttlingId) const {
	auto it = quotas.find(throttlingId);
	if (it == quotas.end() || !it->second.quota.present()) {
		return {};
	} else {
		return it->second.quota.get().totalQuota;
	}
}

Optional<int64_t> RKThroughputQuotaCache::getReservedQuota(ThrottlingId const& throttlingId) const {
	auto it = quotas.find(throttlingId);
	if (it == quotas.end() || !it->second.quota.present()) {
		return {};
	} else {
		return it->second.quota.get().reservedQuota;
	}
}

int RKThroughputQuotaCache::size() const {
	int result = 0;
	for (auto const& [_, quotaData] : quotas) {
		if (quotaData.quota.present()) {
			++result;
		}
	}
	return result;
}

Future<Void> RKThroughputQuotaCache::run() {
	return RKThroughputQuotaCacheImpl::run(this);
}

void RKThroughputQuotaCache::applyUpdate(Key const& key, Optional<Value> const& value) {
	ThrottlingId throttlingId;
	if (key.startsWith(tagQuotaPrefix)) {
		throttlingId = ThrottlingIdRef::fromTag(key.removePrefix(tagQuotaPrefix));
	} else {
		throttlingId = ThrottlingIdRef::fromTenantGroup(TenantMetadata::throughputQuota().unpackKey(key));
	}
	auto it = quotas.find(throttlingId);
	if (it != quotas.end()) {
		if (value.present()) {
			it->second.quota = ThrottleApi::ThroughputQuotaValue::unpack(Tuple::unpack(value.get()));
		} else {
			it->second.quota = {};
		}
	}
}

void RKThroughputQuotaCache::updateThroughput(IRKThroughputTracker const& throughputTracker) {
	auto const activeThrottlingIds = throughputTracker.getActiveThrottlingIds(throughputThreshold);
	for (auto const& throttlingId : activeThrottlingIds) {
		activeThrottlingIdStream.send(throttlingId);
	}
}

int RKThroughputQuotaCache::activeThrottlingIds() const {
	return quotas.size();
}
