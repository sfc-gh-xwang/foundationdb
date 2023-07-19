/*
 * BulkLoadMetadata.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/BulkLoadMetadata.h"
#include "flow/UnitTest.h"

namespace BulkLoadMetadata {

const KeyRangeRef bulkLoadStatusKeys("\xff\x02/bulkLoadStatus/"_sr, "\xff\x02/bulkLoadStatus0"_sr);
const KeyRangeRef bulkLoadInfoKeys("\xff\x02/bulkLoadInfo/"_sr, "\xff\x02/bulkLoadInfo0"_sr);
const StringRef bulkLoadChangeKey = "\xff\x02/bulkLoadChangeKey"_sr;
const KeyRangeRef bulkLoadVersionKeys("\xff\x02/bulkLoadVersion/"_sr, "\xff\x02/bulkLoadVersion0"_sr);
const KeyRangeRef bulkLoadRangeKeys("\xff\x02/bulkLoadRanges/"_sr, "\xff\x02/bulkLoadRanges0"_sr);

WatchableTrigger& onChange() {
	static WatchableTrigger instance(bulkLoadChangeKey);
	return instance;
}

StatusMap& statusMap() {
	static StatusMap instance(bulkLoadStatusKeys.begin, IncludeVersion());
	return instance;
}

InfoMap& infoMap() {
	static InfoMap instance(bulkLoadInfoKeys.begin, IncludeVersion(), onChange());
	return instance;
}

VersionMap& versionMap() {
	static VersionMap instance(bulkLoadVersionKeys.begin);
	return instance;
}

RangeMap& rangeMap() {
	static RangeMap instance(bulkLoadRangeKeys.begin, {}, ObjectCodec<BulkLoadID, _IncludeVersion>(IncludeVersion()));
	return instance;
}

}; // namespace BulkLoadMetadata

// throws error if state transition is invalid, otherwise returns a bool where true == state transition is allowed and
// false == state transition is already processed and unecessary
bool checkValidBulkLoadStateTransition(BulkLoadState current, BulkLoadState desired) {
	// handle idempotency
	if (current == desired) {
		return false;
	}
	if (current > desired && current != BulkLoadState::ABORTED && current != BulkLoadState::ABORTED_COMPLETE) {
		return false;
	}

	// cleanup can only be from done or aborted_complete
	if (desired == BulkLoadState::CLEANUP) {
		if (current != BulkLoadState::DONE && current != BulkLoadState::ABORTED_COMPLETE) {
			throw bulk_load_cleanup_failed();
		}
		return true;
	}

	// once bulk load is aborted_complete or done, no other state transitions can take place except idempotently or to
	// cleanup (handled above).
	if (current == BulkLoadState::ABORTED_COMPLETE) {
		if (desired == BulkLoadState::ABORTED_COMPLETE || desired == BulkLoadState::ABORTED) {
			return false;
		}
		throw bulk_load_aborted();
	}
	if (current == BulkLoadState::ABORTED) {
		if (desired == BulkLoadState::ABORTED_COMPLETE) {
			return true;
		}
		throw bulk_load_aborted();
	}
	if (desired == BulkLoadState::ABORTED_COMPLETE) {
		throw bulk_load_invalid_state_transition();
	}
	if (current == BulkLoadState::DONE) {
		if (desired == BulkLoadState::ABORTED) {
			throw bulk_load_abort_failed();
		}
		ASSERT(desired < current);
		// if bulk load done, any other state transition from earlier states is implicitly done
		return false;
	}

	if (desired == BulkLoadState::ABORTED) {
		// not cleanup or done - can go to aborted
		return true;
	}

	/*
	  outside of cleanup/done/abort, state must follow linear chain of:

	  STARTED
	  STAGING
	  STAGING_BLOB_DONE
	  STAGING_DDPREPARED
	  DONE

	  however, to handle idempotency/retries, if we are trying to set the state to X and the desired state is already >=
	  X, the state transition will be successful
	*/
	// TODO: REMOVE FIRST CASE. Only for testing for now.
	if (current == BulkLoadState::STAGING && desired == BulkLoadState::DONE) {
		return true;
	} else if (current == BulkLoadState::STARTED) {
		if (desired == BulkLoadState::STAGING)
			return true;
		throw bulk_load_invalid_state_transition();
	} else if (current == BulkLoadState::STAGING) {
		if (desired == BulkLoadState::STAGING_BLOB_DONE)
			return true;
		throw bulk_load_invalid_state_transition();
	} else if (current == BulkLoadState::STAGING_BLOB_DONE) {
		if (desired == BulkLoadState::STAGING_DDPREPARED)
			return true;
		throw bulk_load_invalid_state_transition();
	} else if (current == BulkLoadState::STAGING_DDPREPARED) {
		if (desired == BulkLoadState::DONE)
			return true;
		throw bulk_load_invalid_state_transition();
	} else {
		TraceEvent(SevWarn, "InvalidBulkLoadStateTransition")
		    .detail("Current", static_cast<int>(current))
		    .detail("Desired", static_cast<int>(desired));
		ASSERT(false);
	}
	return true;
}

enum StateTransitionResult { ALLOWED, DISALLOWED, ALREADY_DONE };

void testBulkLoadStateTransition(BulkLoadState current,
                                 BulkLoadState desired,
                                 StateTransitionResult expectedResult,
                                 Optional<Error> expectedError = {}) {
	fmt::print("Testing {0} -> {1}\n", static_cast<int>(current), static_cast<int>(desired));
	try {
		bool result = checkValidBulkLoadStateTransition(current, desired);
		ASSERT(expectedResult != StateTransitionResult::DISALLOWED);
		ASSERT(!expectedError.present());
		bool expectSuccess = expectedResult == StateTransitionResult::ALLOWED;
		ASSERT(result == expectSuccess);
	} catch (Error& e) {
		fmt::print("Error {0}\n", e.name());
		ASSERT(expectedResult == StateTransitionResult::DISALLOWED);
		ASSERT(expectedError.present());
		ASSERT(e.code() == expectedError.get().code());
	}
}

TEST_CASE("/fdbclient/bulkload/stateTransitions") {
	// STARTED -> X
	testBulkLoadStateTransition(BulkLoadState::STARTED, BulkLoadState::STARTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(BulkLoadState::STARTED, BulkLoadState::STAGING, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadState::STARTED,
	                            BulkLoadState::STAGING_BLOB_DONE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadState::STARTED,
	                            BulkLoadState::STAGING_DDPREPARED,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadState::STARTED,
	                            BulkLoadState::DONE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadState::STARTED, BulkLoadState::ABORTED, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadState::STARTED,
	                            BulkLoadState::ABORTED_COMPLETE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(
	    BulkLoadState::STARTED, BulkLoadState::CLEANUP, StateTransitionResult::DISALLOWED, bulk_load_cleanup_failed());

	// STAGING -> X
	testBulkLoadStateTransition(BulkLoadState::STAGING, BulkLoadState::STARTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(BulkLoadState::STAGING, BulkLoadState::STAGING, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING, BulkLoadState::STAGING_BLOB_DONE, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadState::STAGING,
	                            BulkLoadState::STAGING_DDPREPARED,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING, BulkLoadState::DONE, StateTransitionResult::ALLOWED); // TODO: only allowed for testing
	testBulkLoadStateTransition(BulkLoadState::STAGING, BulkLoadState::ABORTED, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadState::STAGING,
	                            BulkLoadState::ABORTED_COMPLETE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING, BulkLoadState::CLEANUP, StateTransitionResult::DISALLOWED, bulk_load_cleanup_failed());

	// STAGING_BLOB_DONE -> X
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING_BLOB_DONE, BulkLoadState::STARTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING_BLOB_DONE, BulkLoadState::STAGING, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING_BLOB_DONE, BulkLoadState::STAGING_BLOB_DONE, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING_BLOB_DONE, BulkLoadState::STAGING_DDPREPARED, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadState::STAGING_BLOB_DONE,
	                            BulkLoadState::DONE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING_BLOB_DONE, BulkLoadState::ABORTED, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadState::STAGING_BLOB_DONE,
	                            BulkLoadState::ABORTED_COMPLETE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadState::STAGING_BLOB_DONE,
	                            BulkLoadState::CLEANUP,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_cleanup_failed());

	// STAGING_DDPREPARED -> X
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING_DDPREPARED, BulkLoadState::STARTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING_DDPREPARED, BulkLoadState::STAGING, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING_DDPREPARED, BulkLoadState::STAGING_BLOB_DONE, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING_DDPREPARED, BulkLoadState::STAGING_DDPREPARED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(BulkLoadState::STAGING_DDPREPARED, BulkLoadState::DONE, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(
	    BulkLoadState::STAGING_DDPREPARED, BulkLoadState::ABORTED, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadState::STAGING_DDPREPARED,
	                            BulkLoadState::ABORTED_COMPLETE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadState::STAGING_DDPREPARED,
	                            BulkLoadState::CLEANUP,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_cleanup_failed());

	// DONE -> X
	testBulkLoadStateTransition(BulkLoadState::DONE, BulkLoadState::STARTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(BulkLoadState::DONE, BulkLoadState::STAGING, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::DONE, BulkLoadState::STAGING_BLOB_DONE, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::DONE, BulkLoadState::STAGING_DDPREPARED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(BulkLoadState::DONE, BulkLoadState::DONE, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::DONE, BulkLoadState::ABORTED, StateTransitionResult::DISALLOWED, bulk_load_abort_failed());
	testBulkLoadStateTransition(BulkLoadState::DONE,
	                            BulkLoadState::ABORTED_COMPLETE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadState::DONE, BulkLoadState::CLEANUP, StateTransitionResult::ALLOWED);

	// ABORTED -> X
	testBulkLoadStateTransition(
	    BulkLoadState::ABORTED, BulkLoadState::STARTED, StateTransitionResult::DISALLOWED, bulk_load_aborted());
	testBulkLoadStateTransition(
	    BulkLoadState::ABORTED, BulkLoadState::STAGING, StateTransitionResult::DISALLOWED, bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadState::ABORTED,
	                            BulkLoadState::STAGING_BLOB_DONE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadState::ABORTED,
	                            BulkLoadState::STAGING_DDPREPARED,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(
	    BulkLoadState::ABORTED, BulkLoadState::DONE, StateTransitionResult::DISALLOWED, bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadState::ABORTED, BulkLoadState::ABORTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::ABORTED, BulkLoadState::ABORTED_COMPLETE, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(
	    BulkLoadState::ABORTED, BulkLoadState::CLEANUP, StateTransitionResult::DISALLOWED, bulk_load_cleanup_failed());

	// ABORTED_COMPLETE -> X
	testBulkLoadStateTransition(BulkLoadState::ABORTED_COMPLETE,
	                            BulkLoadState::STARTED,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadState::ABORTED_COMPLETE,
	                            BulkLoadState::STAGING,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadState::ABORTED_COMPLETE,
	                            BulkLoadState::STAGING_BLOB_DONE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadState::ABORTED_COMPLETE,
	                            BulkLoadState::STAGING_DDPREPARED,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(
	    BulkLoadState::ABORTED_COMPLETE, BulkLoadState::DONE, StateTransitionResult::DISALLOWED, bulk_load_aborted());
	testBulkLoadStateTransition(
	    BulkLoadState::ABORTED_COMPLETE, BulkLoadState::ABORTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::ABORTED_COMPLETE, BulkLoadState::ABORTED_COMPLETE, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadState::ABORTED_COMPLETE, BulkLoadState::CLEANUP, StateTransitionResult::ALLOWED);

	return Void();
}
