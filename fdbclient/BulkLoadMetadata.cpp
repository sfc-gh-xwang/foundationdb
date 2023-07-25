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
bool checkValidBulkLoadStateTransition(BulkLoadStatus::State current, BulkLoadStatus::State desired) {
	// handle idempotency
	if (current == desired) {
		return false;
	}
	if (current > desired && current != BulkLoadStatus::State::ABORTED &&
	    current != BulkLoadStatus::State::ABORTED_COMPLETE) {
		return false;
	}

	// cleanup can only be from done or aborted_complete
	if (desired == BulkLoadStatus::State::CLEANUP) {
		if (current != BulkLoadStatus::State::DONE && current != BulkLoadStatus::State::ABORTED_COMPLETE) {
			throw bulk_load_cleanup_failed();
		}
		return true;
	}

	// once bulk load is aborted_complete or done, no other state transitions can take place except idempotently or to
	// cleanup (handled above).
	if (current == BulkLoadStatus::State::ABORTED_COMPLETE) {
		if (desired == BulkLoadStatus::State::ABORTED) {
			return false;
		}
		throw bulk_load_aborted();
	}
	if (current == BulkLoadStatus::State::ABORTED) {
		if (desired == BulkLoadStatus::State::ABORTED_COMPLETE) {
			return true;
		}
		throw bulk_load_aborted();
	}
	if (desired == BulkLoadStatus::State::ABORTED_COMPLETE) {
		throw bulk_load_invalid_state_transition();
	}
	if (current == BulkLoadStatus::State::DONE) {
		if (desired == BulkLoadStatus::State::ABORTED) {
			throw bulk_load_abort_failed();
		}
		ASSERT(desired < current);
		// if bulk load done, any other state transition from earlier states is implicitly done
		return false;
	}

	if (desired == BulkLoadStatus::State::ABORTED) {
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
	if (current == BulkLoadStatus::State::STAGING && desired == BulkLoadStatus::State::DONE) {
		return true;
	} else if (current == BulkLoadStatus::State::STARTED) {
		if (desired == BulkLoadStatus::State::STAGING)
			return true;
		throw bulk_load_invalid_state_transition();
	} else if (current == BulkLoadStatus::State::STAGING) {
		if (desired == BulkLoadStatus::State::STAGING_BLOB_DONE)
			return true;
		throw bulk_load_invalid_state_transition();
	} else if (current == BulkLoadStatus::State::STAGING_BLOB_DONE) {
		if (desired == BulkLoadStatus::State::STAGING_DDPREPARED)
			return true;
		throw bulk_load_invalid_state_transition();
	} else if (current == BulkLoadStatus::State::STAGING_DDPREPARED) {
		if (desired == BulkLoadStatus::State::DONE)
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

void testBulkLoadStateTransition(BulkLoadStatus::State current,
                                 BulkLoadStatus::State desired,
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
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STARTED, BulkLoadStatus::State::STARTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STARTED, BulkLoadStatus::State::STAGING, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadStatus::State::STARTED,
	                            BulkLoadStatus::State::STAGING_BLOB_DONE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadStatus::State::STARTED,
	                            BulkLoadStatus::State::STAGING_DDPREPARED,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadStatus::State::STARTED,
	                            BulkLoadStatus::State::DONE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STARTED, BulkLoadStatus::State::ABORTED, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadStatus::State::STARTED,
	                            BulkLoadStatus::State::ABORTED_COMPLETE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadStatus::State::STARTED,
	                            BulkLoadStatus::State::CLEANUP,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_cleanup_failed());

	// STAGING -> X
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STAGING, BulkLoadStatus::State::STARTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STAGING, BulkLoadStatus::State::STAGING, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STAGING, BulkLoadStatus::State::STAGING_BLOB_DONE, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING,
	                            BulkLoadStatus::State::STAGING_DDPREPARED,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING,
	                            BulkLoadStatus::State::DONE,
	                            StateTransitionResult::ALLOWED); // TODO: only allowed for testing
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STAGING, BulkLoadStatus::State::ABORTED, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING,
	                            BulkLoadStatus::State::ABORTED_COMPLETE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING,
	                            BulkLoadStatus::State::CLEANUP,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_cleanup_failed());

	// STAGING_BLOB_DONE -> X
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STAGING_BLOB_DONE, BulkLoadStatus::State::STARTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STAGING_BLOB_DONE, BulkLoadStatus::State::STAGING, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING_BLOB_DONE,
	                            BulkLoadStatus::State::STAGING_BLOB_DONE,
	                            StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING_BLOB_DONE,
	                            BulkLoadStatus::State::STAGING_DDPREPARED,
	                            StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING_BLOB_DONE,
	                            BulkLoadStatus::State::DONE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STAGING_BLOB_DONE, BulkLoadStatus::State::ABORTED, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING_BLOB_DONE,
	                            BulkLoadStatus::State::ABORTED_COMPLETE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING_BLOB_DONE,
	                            BulkLoadStatus::State::CLEANUP,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_cleanup_failed());

	// STAGING_DDPREPARED -> X
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STAGING_DDPREPARED, BulkLoadStatus::State::STARTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STAGING_DDPREPARED, BulkLoadStatus::State::STAGING, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING_DDPREPARED,
	                            BulkLoadStatus::State::STAGING_BLOB_DONE,
	                            StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING_DDPREPARED,
	                            BulkLoadStatus::State::STAGING_DDPREPARED,
	                            StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STAGING_DDPREPARED, BulkLoadStatus::State::DONE, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::STAGING_DDPREPARED, BulkLoadStatus::State::ABORTED, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING_DDPREPARED,
	                            BulkLoadStatus::State::ABORTED_COMPLETE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(BulkLoadStatus::State::STAGING_DDPREPARED,
	                            BulkLoadStatus::State::CLEANUP,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_cleanup_failed());

	// DONE -> X
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::DONE, BulkLoadStatus::State::STARTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::DONE, BulkLoadStatus::State::STAGING, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::DONE, BulkLoadStatus::State::STAGING_BLOB_DONE, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::DONE, BulkLoadStatus::State::STAGING_DDPREPARED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::DONE, BulkLoadStatus::State::DONE, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(BulkLoadStatus::State::DONE,
	                            BulkLoadStatus::State::ABORTED,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_abort_failed());
	testBulkLoadStateTransition(BulkLoadStatus::State::DONE,
	                            BulkLoadStatus::State::ABORTED_COMPLETE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_invalid_state_transition());
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::DONE, BulkLoadStatus::State::CLEANUP, StateTransitionResult::ALLOWED);

	// ABORTED -> X
	testBulkLoadStateTransition(BulkLoadStatus::State::ABORTED,
	                            BulkLoadStatus::State::STARTED,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadStatus::State::ABORTED,
	                            BulkLoadStatus::State::STAGING,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadStatus::State::ABORTED,
	                            BulkLoadStatus::State::STAGING_BLOB_DONE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadStatus::State::ABORTED,
	                            BulkLoadStatus::State::STAGING_DDPREPARED,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadStatus::State::ABORTED,
	                            BulkLoadStatus::State::DONE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::ABORTED, BulkLoadStatus::State::ABORTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::ABORTED, BulkLoadStatus::State::ABORTED_COMPLETE, StateTransitionResult::ALLOWED);
	testBulkLoadStateTransition(BulkLoadStatus::State::ABORTED,
	                            BulkLoadStatus::State::CLEANUP,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_cleanup_failed());

	// ABORTED_COMPLETE -> X
	testBulkLoadStateTransition(BulkLoadStatus::State::ABORTED_COMPLETE,
	                            BulkLoadStatus::State::STARTED,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadStatus::State::ABORTED_COMPLETE,
	                            BulkLoadStatus::State::STAGING,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadStatus::State::ABORTED_COMPLETE,
	                            BulkLoadStatus::State::STAGING_BLOB_DONE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadStatus::State::ABORTED_COMPLETE,
	                            BulkLoadStatus::State::STAGING_DDPREPARED,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(BulkLoadStatus::State::ABORTED_COMPLETE,
	                            BulkLoadStatus::State::DONE,
	                            StateTransitionResult::DISALLOWED,
	                            bulk_load_aborted());
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::ABORTED_COMPLETE, BulkLoadStatus::State::ABORTED, StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(BulkLoadStatus::State::ABORTED_COMPLETE,
	                            BulkLoadStatus::State::ABORTED_COMPLETE,
	                            StateTransitionResult::ALREADY_DONE);
	testBulkLoadStateTransition(
	    BulkLoadStatus::State::ABORTED_COMPLETE, BulkLoadStatus::State::CLEANUP, StateTransitionResult::ALLOWED);

	return Void();
}
