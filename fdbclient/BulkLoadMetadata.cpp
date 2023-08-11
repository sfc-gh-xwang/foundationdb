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
const KeyRangeRef bulkLoadReservationKeys("\xff\x02/bulkLoadReservations/"_sr, "\xff\x02/bulkLoadReservations0"_sr);
// put in the transaction state store in case we eventually want the commit proxies to be able to serve this directly
// without reading the db for changes
const KeyRangeRef bulkLoaderListKeys("\xff/bulkLoaderList/"_sr, "\xff/bulkLoaderList0"_sr);

const KeyRangeRef bulkLoadCreatedFilePointersKeys("\xff\x02/bulkLoadCreatedFiles/"_sr,
                                                  "\xff\x02/bulkLoadCreatedFiles0"_sr);
const KeyRangeRef bulkLoadWrittenFilePointersKeys("\xff\x02/bulkLoadWrittenFiles/"_sr,
                                                  "\xff\x02/bulkLoadWrittenFiles0"_sr);

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

ReservationMap& reservationMap() {
	static ReservationMap instance(bulkLoadReservationKeys.begin);
	return instance;
}

LoaderListMap& loaderListMap() {
	static LoaderListMap instance(bulkLoaderListKeys.begin, IncludeVersion());
	return instance;
}

CreatedFilePointersMap& createdFilePointersMap() {
	static CreatedFilePointersMap instance(bulkLoadCreatedFilePointersKeys.begin, IncludeVersion());
	return instance;
}

WrittenFilePointersMap& writtenFilePointersMap() {
	static WrittenFilePointersMap instance(bulkLoadWrittenFilePointersKeys.begin, IncludeVersion());
	return instance;
}

std::pair<FilePointerKey, FilePointerKey> allFilePointersForLoadRange(const BulkLoadID& id) {
	return { { id, ""_sr }, { strinc(id), ""_sr } };
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

// ----- Unit Tests + Helper Functions for testing checkValidBulkLoadStateTransition -----

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

// ensure sizes are valid, all ids are unique, all key ranges are within bulk load key range, and key ranges are all
// disjoint
// FIXME: for large number of files, maybe break up with yields?
void validateBulkLoadWrittenPointers(const BulkLoadID& id,
                                     const BulkLoadInfo& info,
                                     const VectorRef<BulkLoadWrittenFileRef>& files) {
	std::vector<KeyRangeRef> ranges;
	std::unordered_set<BulkLoadFileID> fileIds;
	ranges.reserve(files.size());
	fileIds.reserve(files.size());
	for (int i = 0; i < files.size(); i++) {
		auto& p = files[i];
		if (p.logicalSize < 0 || p.physicalSize < 0) {
			TraceEvent("BulkLoadInvalidPointerSize")
			    .detail("LoadID", id)
			    .detail("FileID", p.fileId)
			    .detail("LogicalSize", p.logicalSize)
			    .detail("PhysicalSize", p.physicalSize);
			throw bulk_load_invalid_file_metadata();
		}
		if (!info.keyRange.contains(p.keyRange)) {
			TraceEvent("BulkLoadInvalidPointerNotInsideLoadRange")
			    .detail("LoadID", id)
			    .detail("FileID", p.fileId)
			    .detail("LoadRange", info.keyRange)
			    .detail("PointerRange", p.keyRange);
			// file key range not inside bulk load key range
			throw bulk_load_invalid_file_metadata();
		}
		ranges.push_back(p.keyRange);
		if (!fileIds.insert(p.fileId).second) {
			// duplicate file ids
			TraceEvent("BulkLoadInvalidPointerDuplicateFileID").detail("LoadID", id).detail("FileID", p.fileId);
			throw bulk_load_invalid_file_metadata();
		}
	}
	std::sort(ranges.begin(), ranges.end(), KeyRangeRef::ArbitraryOrder());
	for (int i = 1; i < ranges.size(); i++) {
		// meta-check: validate sort worked
		ASSERT(ranges[i - 1].begin <= ranges[i].begin);
		if (ranges[i - 1].end > ranges[i].begin) {
			TraceEvent("BulkLoadInvalidPointerOverlappingKeyRanges")
			    .detail("LoadID", id)
			    .detail("FileRange", ranges[i])
			    .detail("OtherRange", ranges[i - 1]);
			// non-disjoint key ranges
			throw bulk_load_invalid_file_metadata();
		}
	}
}

// ----- Unit Tests + Helper Functions for testing validateBulkLoadWrittenPointers -----
// helper to convert std::vector to VectorRef
void validateBulkLoadWrittenPointers(const BulkLoadID& id,
                                     const BulkLoadInfo& info,
                                     const std::vector<BulkLoadWrittenFileRef>& files) {
	Standalone<VectorRef<BulkLoadWrittenFileRef>> vRef;
	vRef.reserve(vRef.arena(), files.size());
	for (auto& it : files) {
		vRef.push_back(vRef.arena(), it);
	}
	return validateBulkLoadWrittenPointers(id, info, vRef);
}

BulkLoadWrittenFileRef createPtr(const BulkLoadInfo& info, Arena& ar) {
	BulkLoadWrittenFileRef ptr;
	UID fileUID = deterministicRandom()->randomUniqueID();
	ptr.fileId = StringRef(ar, fileUID.toString());
	ptr.keyRange = info.keyRange;
	ptr.logicalSize = 1;
	ptr.physicalSize = 1;
	return ptr;
}

void checkKeyRanges(bool expectSuccess,
                    const BulkLoadID& id,
                    const BulkLoadInfo& info,
                    const std::vector<KeyRangeRef>& ranges) {
	Standalone<VectorRef<BulkLoadWrittenFileRef>> ptrs;
	ptrs.reserve(ptrs.arena(), ranges.size());
	for (auto& it : ranges) {
		BulkLoadWrittenFileRef ptr = createPtr(info, ptrs.arena());
		ptr.keyRange = it;
		ptrs.push_back(ptrs.arena(), ptr);
	}

	try {
		validateBulkLoadWrittenPointers(id, info, ptrs);
		ASSERT(expectSuccess);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_bulk_load_invalid_file_metadata);
		ASSERT(!expectSuccess);
	}
}

TEST_CASE("/fdbclient/bulkload/validateBulkLoadWrittenPointers") {
	Arena ar;
	Standalone<StringRef> keyA = "a"_sr;
	Standalone<StringRef> keyB = "b"_sr;
	Standalone<StringRef> keyC = "c"_sr;
	Standalone<StringRef> keyD = "d"_sr;
	Standalone<StringRef> keyZ = "z"_sr;
	BulkLoadID id = "bulkloadid"_sr;
	BulkLoadInfo info;
	info.keyRange = KeyRangeRef(keyA, keyD);

	// check default pointer is valid
	{
		BulkLoadWrittenFileRef ptr = createPtr(info, ar);
		validateBulkLoadWrittenPointers(id, info, { ptr });
		checkKeyRanges(true, id, info, { KeyRangeRef(keyA, keyB) });
	}

	// check invalid logical/physical size
	{
		try {
			BulkLoadWrittenFileRef ptr = createPtr(info, ar);
			ptr.logicalSize = -1;
			validateBulkLoadWrittenPointers(id, info, { ptr });
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_invalid_file_metadata);
		}
	}

	{
		try {
			BulkLoadWrittenFileRef ptr = createPtr(info, ar);
			ptr.physicalSize = -1;
			validateBulkLoadWrittenPointers(id, info, { ptr });
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_invalid_file_metadata);
		}
	}

	// check duplicate file ids
	{
		try {
			BulkLoadWrittenFileRef ptr = createPtr(info, ar);
			validateBulkLoadWrittenPointers(id, info, { ptr, ptr });
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_invalid_file_metadata);
		}
	}

	// check ranges outside of bulk load range
	{
		checkKeyRanges(false, id, info, { KeyRangeRef(""_sr, keyZ) });
		checkKeyRanges(false, id, info, { KeyRangeRef(""_sr, keyA) });
		checkKeyRanges(false, id, info, { KeyRangeRef(""_sr, keyB) });
		checkKeyRanges(false, id, info, { KeyRangeRef(""_sr, keyD) });
		checkKeyRanges(false, id, info, { KeyRangeRef(keyA, keyZ) });
		checkKeyRanges(false, id, info, { KeyRangeRef(keyB, keyZ) });
		checkKeyRanges(false, id, info, { KeyRangeRef(keyZ, "\xff"_sr) });
	}

	// check overlapping ranges within bulk load range
	{

		checkKeyRanges(false, id, info, { KeyRangeRef(keyA, keyC), KeyRangeRef(keyB, keyD) });
		checkKeyRanges(false, id, info, { KeyRangeRef(keyA, keyD), KeyRangeRef(keyB, keyC) });
		// reverse order
		checkKeyRanges(false, id, info, { KeyRangeRef(keyB, keyD), KeyRangeRef(keyA, keyC) });
		checkKeyRanges(false, id, info, { KeyRangeRef(keyB, keyC), KeyRangeRef(keyA, keyD) });
	}

	// check valid subsets of ranges
	{
		// one range or subranges
		checkKeyRanges(true, id, info, { KeyRangeRef(keyA, keyD) });
		checkKeyRanges(true, id, info, { KeyRangeRef(keyA, keyB) });
		checkKeyRanges(true, id, info, { KeyRangeRef(keyB, keyC) });
		checkKeyRanges(true, id, info, { KeyRangeRef(keyC, keyD) });
		// non-connecting but multiple
		checkKeyRanges(true, id, info, { KeyRangeRef(keyA, keyB), KeyRangeRef(keyC, keyD) });
		checkKeyRanges(true, id, info, { KeyRangeRef(keyC, keyD), KeyRangeRef(keyA, keyB) });
		// multiple connecting
		checkKeyRanges(true, id, info, { KeyRangeRef(keyA, keyB), KeyRangeRef(keyB, keyC), KeyRangeRef(keyC, keyD) });
		checkKeyRanges(true, id, info, { KeyRangeRef(keyB, keyC), KeyRangeRef(keyA, keyB), KeyRangeRef(keyC, keyD) });
		checkKeyRanges(true, id, info, { KeyRangeRef(keyC, keyD), KeyRangeRef(keyB, keyC), KeyRangeRef(keyA, keyB) });
	}

	return Void();
}