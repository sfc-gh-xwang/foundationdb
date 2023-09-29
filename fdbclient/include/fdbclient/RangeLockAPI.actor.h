
/*
 * RangeLockAPI.actor.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_RANGE_LOCK_API_ACTOR_G_H)
#define FDBCLIENT_RANGE_LOCK_API_ACTOR_G_H
#include "fdbclient/RangeLockAPI.actor.g.h"
#elif !defined(FDBCLIENT_RANGE_LOCK_API_ACTOR_H)
#define FDBCLIENT_RANGE_LOCK_API_ACTOR_H

#include "fdbclient/SystemData.h"
#include "fdbclient/RangeLock.h"
#include "flow/Error.h"
#include "flow/actorcompiler.h"

#define RANGELOCKAPI_DEBUG 0
#if RANGELOCKAPI_DEBUG || !defined(NO_INTELLISENSE)
#define rl_debug fmt::print
#else
#define rl_debug(...)
#endif

namespace RangeLockAPI {

// return whether a range is unlocked
ACTOR template <class Transaction>
Future<bool> isRangeUnlockedTransaction(Transaction tr, KeyRef begin, KeyRef end) {
	state Reference<RangeLockMapSnapshot> overlap = wait(rangeLockMap().getSnapshot(tr, begin, end));
	state RangeLockMapSnapshot::Ranges ranges = overlap->ranges();
	state RangeLockMapSnapshot::RangeIter it = ranges.begin();
	for (; it != ranges.end(); ++it) {
		if (it.value().status != RangeLockInfo::UNLOCKED) {
			return false;
		}
	}
	return true;
}

ACTOR template <class Transaction>
Future<bool> isRangeLockedWithId(Transaction tr, KeyRef begin, KeyRef end, UID lockId) {
	state Reference<RangeLockMapSnapshot> overlap = wait(rangeLockMap().getSnapshot(tr, begin, end));
	state RangeLockMapSnapshot::Ranges ranges = overlap->ranges();
	state RangeLockMapSnapshot::RangeIter it = ranges.begin();
	for (; it != ranges.end(); ++it) {
		if (it.value().status == RangeLockInfo::UNLOCKED || it.value().lockId != lockId) {
			return false;
		}
	}
	return true;
}

// lock range [begin, end) with lockId
ACTOR template <class Transaction>
Future<Void> lockRangesTransaction(Transaction tr, KeyRef begin, KeyRef end, UID lockId) {
	rl_debug("------ Call lock range [{}, {}) ------\n", begin, end);
	// can't lock system keys
	// SOMEDAY: allow \xff/x02 in the future
	if (!normalKeys.contains(KeyRangeRef(begin, end)))
		throw illegal_range_lock_param();

	// can't lock overlapped range in the same transaction
	// Now only RYW can guarantee this property.
	if (!tr->rywEnabled()) {
		throw unsupported_operation();
	}

	state RangeLockMetadata metadata;
	state Optional<int> lockCount = wait(metadata.rangeLockCount().get(tr, Snapshot::True));
	if (lockCount.present() && lockCount.get() > CLIENT_KNOBS->RANGE_LOCK_LIMIT) {
		CODE_PROBE(true, "Too many range locks");
		throw range_lock_limit_exceeded();
	}

	// check existed range locks
	state Reference<RangeLockMapSnapshot> overlap = wait(rangeLockMap().getSnapshot(tr, begin, end));
	state RangeLockMapSnapshot::Ranges ranges = overlap->ranges();
	state RangeLockMapSnapshot::RangeIter it = ranges.begin();
	// Case1: All overlapped range is unlocked. Pass.
	// Case2: Exactly the same range is locked with the same id. Idempotent transaction.
	// Other cases: Fail.
	for (; it != ranges.end(); ++it) {
		rl_debug("lockRangesTransaction: {} {} {}\n",
		         it->range().begin.toStringView(),
		         it->range().end.toStringView(),
		         it->value().toString());

		if (it.value().status != RangeLockInfo::UNLOCKED) {
			// Case 2
			if (it->value().lockId == lockId && it->range().begin == begin && it->range().end == end) {
				CODE_PROBE(true, "lockRange is No-op");
				return Void();
			}

			// Other cases
			CODE_PROBE(true, "lockRange operation failed");
			throw illegal_range_lock_param();
		}
	}

	wait(rangeLockMap().updateRange(
	    tr, begin, end, RangeLockInfo(RangeLockInfo::LOCKED, lockId), true, CoalesceRange::False));
	// concurrent writes to the locked range will fail this lock attempt.
	tr->addReadConflictRange(KeyRangeRef(begin, end));
	metadata.rangeLockCount().atomicOp(tr, 1, MutationRef::AddValue);

	return Void();
}

// unlock [begin, end) with lockId.
ACTOR template <class Transaction>
Future<Void> unlockRangesTransaction(Transaction tr, KeyRef begin, KeyRef end, UID lockId) {
	rl_debug("------ Call unlock range [{}, {}) ------\n", begin, end);
	// won't have system key lock
	if (!normalKeys.contains(KeyRangeRef(begin, end)))
		throw illegal_range_lock_param();

	// can't unlock overlapped but different range in the same transaction.
	// Now only RYW can guarantee this property.
	if (!tr->rywEnabled()) {
		throw unsupported_operation();
	}

	// Case 1: the range is already unlocked. Do nothing.
	bool noop = wait(isRangeUnlockedTransaction(tr, begin, end));
	if (noop) {
		CODE_PROBE(true, "Unlock is a Noop");
		return Void();
	}

	// There must be a locked range contained by [begin, end). We just need to look at the first item and see whether
	// it's aligned with [begin, end) and has the same lock id.
	state Optional<RangeLockRangeValue> lockValue = wait(rangeLockMap().getRangeForKey(tr, begin));
	if (lockValue->range.begin != begin || lockValue->range.end != end || lockValue->value.lockId != lockId) {
		rl_debug("unlockRangesTransaction: {} {} {}\n",
		         lockValue->range.begin.toStringView(),
		         lockValue->range.end.toStringView(),
		         lockValue->value.lockId.toString());
		CODE_PROBE(true, "Unlock operation failed");
		throw illegal_range_lock_param();
	}

	state RangeLockMetadata metadata;
	metadata.rangeLockCount().atomicOp(tr, -1, MutationRef::AddValue);
	wait(rangeLockMap().updateRange(tr, begin, end, RangeLockInfo(RangeLockInfo::UNLOCKED, UID())));
	return Void();
}

// only for list api
struct RangeLockInfoWithKeys {
	Key begin, end;
	RangeLockInfo info;

	json_spirit::mObject toJson() const;
};

// return all locked ranges contained by [begin, end)
ACTOR template <class Transaction>
Future<std::vector<RangeLockInfoWithKeys>> listLockedRangesTransaction(Transaction tr, KeyRef begin, KeyRef end) {
	// check existed range locks
	state Reference<RangeLockMapSnapshot> overlap = wait(rangeLockMap().getSnapshot(tr, begin, end));
	state RangeLockMapSnapshot::Ranges ranges = overlap->ranges();
	state RangeLockMapSnapshot::RangeIter it = ranges.begin();
	state std::vector<RangeLockInfoWithKeys> result;
	for (; it != ranges.end(); ++it) {
		rl_debug("listLockedRangesTransaction: {} {} {}\n",
		         it->range().begin.toStringView(),
		         it->range().end.toStringView(),
		         it->value().toString());
		if (it->value().status != RangeLockInfo::UNLOCKED)
			result.emplace_back(RangeLockInfoWithKeys{ it->range().begin, it->range().end, it->value() });
	}

	return result;
}

ACTOR template <class DB>
Future<std::vector<RangeLockInfoWithKeys>> listLockedRanges(Reference<DB> db, KeyRef begin, KeyRef end) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			std::vector<RangeLockAPI::RangeLockInfoWithKeys> res =
			    wait(RangeLockAPI::listLockedRangesTransaction(tr, begin, end));
			return res;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

} // namespace RangeLockAPI

#include "flow/unactorcompiler.h"
#endif