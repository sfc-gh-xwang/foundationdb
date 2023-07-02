/**
 * UpdateStream.actor.cpp
 */

#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/UpdateStream.h"
#include "flow/actorcompiler.h" // must be last include

class UpdateStreamImpl {

	ACTOR static Future<Optional<Value>> getValue(Reference<ReadYourWritesTransaction> tr, Key key) {
		ValueReadResult value = wait(tr->get(key));
		return value.contents();
	}

	ACTOR static Future<RangeResult> getRange(Reference<ReadYourWritesTransaction> tr,
	                                          KeyRange keys,
	                                          int limit,
	                                          Reverse reverse = Reverse::False) {
		RangeReadResult result = wait(tr->getRange(keys, limit, Snapshot::False, reverse));
		return result.contents();
	}

	static uint64_t getNextIndex(Optional<uint64_t> currentIndex) {
		uint64_t result = 0;
		if (currentIndex.present()) {
			result = currentIndex.get() + 1;
		}
		return result;
	}

	static Key getUpdateKey(Key metadataPrefix, uint64_t index) {
		return BinaryWriter::toValue(bigEndian64(index), Unversioned())
		    .withPrefix("updates/"_sr)
		    .withPrefix(metadataPrefix);
	}

	static uint64_t indexFromUpdateKey(Key metadataPrefix, Key key) {
		return fromBigEndian64(BinaryReader::fromStringRef<uint64_t>(
		    key.removePrefix(metadataPrefix).removePrefix("updates/"_sr), Unversioned()));
	}

	static KeyRange getUpdateKeyRange(Key metadataPrefix,
	                                  uint64_t startIndex,
	                                  uint64_t endIndex = std::numeric_limits<uint64_t>::max()) {
		return KeyRangeRef(getUpdateKey(metadataPrefix, startIndex), getUpdateKey(metadataPrefix, endIndex));
	}

	// Verifies that the current UpdateStream object's lease is still valid. If the lease has not been
	// modified yet, but will be before the transaction commits, then the transaction will fail to commit
	// with a conflict, because the lease key is added to the write conflict range. Then, the next time
	// validateLease is called, the stale lease will be detected.
	ACTOR static Future<Void> validateLease(UpdateStream* self, Reference<ReadYourWritesTransaction> tr) {
		Optional<Value> value = wait(getValue(tr, self->getLeaseKey()));
		Optional<UID> currentLeaseId;
		if (value.present()) {
			currentLeaseId = BinaryReader::fromStringRef<UID>(value.get(), Unversioned());
		}
		if (!currentLeaseId.present() || self->leaseId != currentLeaseId) {
			TraceEvent(SevWarn, "UpdateStreamLostLease")
			    .detail("LeaseId", self->leaseId)
			    .detail("CurrentLeaseId", currentLeaseId);
			CODE_PROBE(true, "UpdateStream lost lease");
			throw restart_update_stream();
		}
		tr->addWriteConflictRange(singleKeyRange(self->getLeaseKey()));
		return Void();
	}

	// Reads the last populated key in the persistent update queue keyrange. If no such key exists,
	// there have never been any updates in the queue.
	ACTOR static Future<Optional<uint64_t>> getLastUpdateIndex(Reference<ReadYourWritesTransaction> tr,
	                                                           Key metadataPrefix) {
		state KeyRange updateKeys = getUpdateKeyRange(metadataPrefix, /*startIndex=*/0);
		RangeResult rangeResult = wait(getRange(tr, updateKeys, /*limit=*/1, Reverse::True));
		if (rangeResult.size() == 0) {
			return Optional<uint64_t>();
		} else {
			return indexFromUpdateKey(metadataPrefix, rangeResult[0].key);
		}
	}

	// Blocks until some new update is ready to consume, or the lease has been invalidated.
	//
	// The ABA problem is circumvented when waiting on the watches, because if there is a new
	// update to the next update key, there are two possiblities:
	//
	//   1) The current UpdateStream object still has the lease. In this case, the new update will
	//      not be cleared until control returns to the getUpdates actor. But that cannot happen while
	//      blocked waiting on the watches in waitForUpdates. So eventually, watchUpdatesFuture will
	//      be ready.
	//   2) The current UpdateStream object no longer has the lease. In this case, the watchLeaseFuture
	//      will eventually become ready, because it is impossible for the lease to be reobtained by
	//      the current UpdateStream object.
	ACTOR static Future<Void> waitForUpdates(UpdateStream* self, Database db) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
		loop {
			try {
				self->applyTransactionOptions(tr);
				Optional<uint64_t> lastUpdateIndex = wait(getLastUpdateIndex(tr, self->metadataPrefix));
				if (lastUpdateIndex != self->lastUpdateIndex) {
					return Void();
				}
				auto const nextUpdateIndex = getNextIndex(lastUpdateIndex);
				state Future<Void> watchUpdatesFuture = tr->watch(getUpdateKey(self->metadataPrefix, nextUpdateIndex));
				state Future<Void> watchLeaseFuture = tr->watch(self->getLeaseKey());
				wait(validateLease(self, tr));
				wait(tr->commit());
				wait(watchUpdatesFuture || watchLeaseFuture);
				return Void();
			} catch (Error& e) {
				TraceEvent("UpdateStreamWaitForUpdatesError").error(e).detail("LeaseId", self->leaseId);
				if (e.code() == error_code_restart_update_stream) {
					CODE_PROBE(true, "UpdateStream lease invalidated while waiting for updates");
					self->results.sendError(e);
				}
				wait(tr->onError(e));
			}
		}
	}

public:
	ACTOR static Future<Optional<uint64_t>> start(UpdateStream* self, Database db) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
		loop {
			try {
				self->applyTransactionOptions(tr);
				state Optional<uint64_t> lastUpdateIndex = wait(getLastUpdateIndex(tr, self->metadataPrefix));
				tr->set(self->getLeaseKey(), BinaryWriter::toValue(self->leaseId, Unversioned()));
				wait(tr->commit());
				self->lastUpdateIndex = lastUpdateIndex;
				return self->lastUpdateIndex;
			} catch (Error& e) {
				TraceEvent(SevDebug, "UpdateStreamStartError").error(e);
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> getUpdates(UpdateStream* self, Database db) {
		state Reference<ReadYourWritesTransaction> tr;
		state std::deque<UpdateStream::Update> toSend;
		loop {
			// Block until some update is ready to consume
			wait(waitForUpdates(self, db));
			tr = makeReference<ReadYourWritesTransaction>(db);
			loop {
				try {
					self->applyTransactionOptions(tr);
					wait(validateLease(self, tr));
					state uint64_t startIndex = getNextIndex(self->lastUpdateIndex);
					state KeyRangeRef updateKeys =
					    getUpdateKeyRange(self->metadataPrefix, startIndex, startIndex + self->batchSize + 1);
					state RangeResult updates = wait(getRange(tr, updateKeys, /*limit=*/self->batchSize + 1));

					// Queue updates to send assuming transaction successfully commits
					state int i = 0;
					toSend.clear();
					while (i < updates.size() && i < self->batchSize) {
						state uint64_t updateIndex = indexFromUpdateKey(self->metadataPrefix, updates[i].key);
						ASSERT_EQ(updateIndex, startIndex + i);
						Optional<Value> value = wait(getValue(tr, updates[i].value));
						toSend.emplace_back(updates[i].value, value, updateIndex);
						++i;
					}

					// Clear updates that have been applied (except for the last key, which
					// is kept around to indicate the last applied update).
					if (self->lastUpdateIndex.present()) {
						tr->clear(getUpdateKeyRange(self->metadataPrefix,
						                            /*startIndex=*/0,
						                            /*endIndex=*/self->lastUpdateIndex.get() + updates.size()));
					} else {
						tr->clear(
						    getUpdateKeyRange(self->metadataPrefix, /*startIndex=*/0, /*endIndex=*/updates.size() - 1));
					}
					wait(tr->commit());

					// Now send the queued updates, since they have successfully been removed from the persistent queue
					while (!toSend.empty()) {
						self->results.send(toSend.front());
						toSend.pop_front();
					}
					self->lastUpdateIndex = startIndex + std::min(updates.size(), self->batchSize) - 1;
					break;
				} catch (Error& e) {
					TraceEvent("UpdateStreamGetUpdatesError").error(e).detail("LeaseId", self->leaseId);
					if (e.code() == error_code_commit_unknown_result) {
						CODE_PROBE(true,
						           "UpdateStream got commit_unknown_result while attempting to clear update queue");
						self->results.sendError(restart_update_stream());
						return Void();
					} else if (e.code() == error_code_restart_update_stream) {
						CODE_PROBE(true, "UpdateStream lost lease while consuming for update queue");
						self->results.sendError(restart_update_stream());
						return Void();
					}
					wait(tr->onError(e));
				}
			}
		}
	}

	ACTOR static Future<Void> update(Reference<ReadYourWritesTransaction> tr,
	                                 Key key,
	                                 Optional<Value> value,
	                                 Key metadataPrefix) {
		if (value.present()) {
			tr->set(key, value.get());
		} else {
			tr->clear(key);
		}
		Optional<uint64_t> previousUpdateIndex = wait(getLastUpdateIndex(tr, metadataPrefix));
		auto const currentUpdateIndex = getNextIndex(previousUpdateIndex);
		TraceEvent(SevDebug, "UpdateStreamUpdate")
		    .detail("Key", key)
		    .detail("Value", value)
		    .detail("CurrentUpdateIndex", currentUpdateIndex);
		tr->set(getUpdateKey(metadataPrefix, currentUpdateIndex), key);
		return Void();
	}
};

UpdateStream::UpdateStream(Key metadataPrefix, int batchSize, bool accessSystemKeys, bool immediatePriority)
  : metadataPrefix(metadataPrefix), batchSize(batchSize), leaseId(deterministicRandom()->randomUniqueID()),
    accessSystemKeys(accessSystemKeys), immediatePriority(immediatePriority) {}

Key UpdateStream::getLeaseKey() const {
	return ("lease"_sr).withPrefix(metadataPrefix);
}

Future<Optional<uint64_t>> UpdateStream::start(Database db) {
	return UpdateStreamImpl::start(this, db);
}

FutureStream<UpdateStream::Update> UpdateStream::getUpdates(Database db) {
	if (!updateStreamFuture.isValid()) {
		updateStreamFuture = UpdateStreamImpl::getUpdates(this, db);
	}
	return results.getFuture();
}

Future<Void> UpdateStream::update(Reference<ReadYourWritesTransaction> tr,
                                  Key const& key,
                                  Optional<Value> const& value,
                                  Key metadataPrefix) {
	return UpdateStreamImpl::update(tr, key, value, metadataPrefix);
}

UID UpdateStream::getLeaseId() const {
	return leaseId;
}

void UpdateStream::applyTransactionOptions(Reference<ReadYourWritesTransaction> tr) const {
	if (accessSystemKeys) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	}
	if (immediatePriority) {
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	}
}

UpdateStream::Update::Update(Key const& key, Optional<Value> const& value, uint64_t index)
  : key(key), value(value), index(index) {}
