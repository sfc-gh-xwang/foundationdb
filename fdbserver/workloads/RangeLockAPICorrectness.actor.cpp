/*
 * RangeLockAPICorrectness.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbclient/RangeLockAPI.actor.h"
#include "fdbclient/RunTransaction.actor.h"
#include "flow/actorcompiler.h"

// TODO: shall we make sure this workload only run on simulation?
struct RangeLockAPICorrectnessWorkload : KVWorkload {

	static constexpr auto NAME = "RangeLockAPICorrectness";

	double testDuration;
	// for random generation convenience, we always lock key ranges [0, 1), [1,2), ...,[alwaysLockedCount-1,
	// alwaysLockedCount) during setup phase. And those range will never be unlocked successfully.
	int alwaysLockedCount = 10;

	// convert int to Key
	std::map<int, int> lockedRangeIndex;
	Version lastCommittedVersion = -1;

	struct OpParam {
		enum { UNKNOWN = -1, LockRange = 0, UnlockRange, Write, Clear, Read, MAX_OP_TYPE = Read } opType;
		int beginIndex;
		int endIndex;
		Key begin;
		Optional<Key> end; // if the operation is Read, the end value is the expected value
		UID lockId;

		// What error should expect if this Op is played
		Optional<Error> error;

		std::string toString() const {
			return fmt::format("op: {}, begin: {}, end: {}, expected error: {}, id {}",
			                   opType,
			                   beginIndex,
			                   endIndex,
			                   error.present() ? error->name() : "[]",
			                   lockId.toString());
		}
	};

	// whether the [begin, end) is locked
	bool isLockedIndexRange(int begin, int end) const {
		if (lockedRangeIndex.empty())
			return false;

		auto it = lockedRangeIndex.upper_bound(begin);
		// [begin, it->first, end]
		if (it != lockedRangeIndex.end()) {
			if (it->first < end)
				return true;
		}
		if (it != lockedRangeIndex.begin()) {
			--it;
			// [it->first, begin, it->second]
			if (it->second > begin)
				return true;
		}

		return false;
	}

	Optional<int> randomLockedIndex() const {
		if (alwaysLockedCount > 0) {
			return deterministicRandom()->randomInt(0, alwaysLockedCount);
		}
		if (!lockedRangeIndex.empty()) {
			int index = deterministicRandom()->randomInt(alwaysLockedCount, nodeCount);
			auto it = lockedRangeIndex.lower_bound(index);
			if (it == lockedRangeIndex.end()) {
				return lockedRangeIndex.rbegin()->first;
			}
			return it->first;
		}
		return {};
	}

	Optional<int> randomUnlockedIndex() const {
		for (int i = 0; i < 100; ++i) {
			int b = deterministicRandom()->randomInt(alwaysLockedCount, nodeCount);
			if (!isLockedIndexRange(b, b + 1)) {
				return b;
			}
		}
		return {};
	}

	// random choose a range from lockedRangeIndex
	Optional<OpParam> randomLockedRange() const {
		if (lockedRangeIndex.empty())
			return {};
		int index = deterministicRandom()->randomInt(alwaysLockedCount, nodeCount);

		auto it = lockedRangeIndex.lower_bound(index);
		if (it == lockedRangeIndex.end()) {
			auto rit = lockedRangeIndex.rbegin();
			return OpParam{ OpParam::UNKNOWN,
				            rit->first,
				            rit->second,
				            keyForIndex(rit->first),
				            keyForIndex(rit->second),
				            UID(rit->first, rit->second),
				            {} };
		}
		return OpParam{
			OpParam::UNKNOWN,           it->first, it->second, keyForIndex(it->first), keyForIndex(it->second),
			UID(it->first, it->second), {}
		};
	}

	Optional<OpParam> randomUnlockedRange() const {
		for (int i = 0; i < 100; ++i) {
			int b = deterministicRandom()->randomInt(alwaysLockedCount, nodeCount);
			int e = deterministicRandom()->randomInt(b + 1, nodeCount + 1);
			if (!isLockedIndexRange(b, e)) {
				return OpParam{ OpParam::UNKNOWN, b, e, keyForIndex(b), keyForIndex(e), UID(b, e), {} };
			}
		}
		return {};
	}

	Optional<OpParam> generateRandomParamForLock() const {
		if (alwaysLockedCount > 0 && deterministicRandom()->coinflip()) {
			// case 1: lock a locked range
			int b = deterministicRandom()->randomInt(0, alwaysLockedCount);
			int e = deterministicRandom()->randomInt(b + 2, nodeCount);
			CODE_PROBE(true, "Lock case 1");
			return OpParam{ OpParam::LockRange,        b, e, keyForIndex(b), keyForIndex(e), UID(b, e),
				            illegal_range_lock_param() };
		}
		// case 2: try best to generate an unlocked range
		auto param = randomUnlockedRange();
		if (param.present()) {
			CODE_PROBE(true, "Lock case 2");
			param.get().opType = OpParam::LockRange;
			if (lockedRangeIndex.size() + alwaysLockedCount > CLIENT_KNOBS->RANGE_LOCK_LIMIT) {
				CODE_PROBE(true, "Lock case 3");
				param.get().error = range_lock_limit_exceeded();
			}
		}
		return param;
	}

	Optional<OpParam> generateRandomParamForUnlock() const {
		int dice = deterministicRandom()->randomInt(0, 3);
		Optional<OpParam> result;
		if (alwaysLockedCount > 0 && dice == 0) {
			// case 1: unlock a range that is not exactly matched with a locked range
			CODE_PROBE(true, "Unlock case 1");
			int b = deterministicRandom()->randomInt(0, alwaysLockedCount);
			int e = deterministicRandom()->randomInt(b + 2, nodeCount);
			result = OpParam{ OpParam::UnlockRange,      b, e, keyForIndex(b), keyForIndex(e), UID(b, e),
				              illegal_range_lock_param() };
		} else if (dice == 1) {
			// case 2: try best to generate an unlocked range
			result = randomUnlockedRange();
			CODE_PROBE(result.present(), "Unlock case 2");
		} else if (dice == 2) {
			// case 3: random choose a locked range
			result = randomLockedRange();
			CODE_PROBE(result.present(), "Unlock case 3");
		}
		if (result.present()) {
			result.get().opType = OpParam::UnlockRange;
		}
		return result;
	}

	Optional<OpParam> generateRandomParamSingleKeyMutation() const {
		if (deterministicRandom()->coinflip()) {
			auto index = randomLockedIndex();
			if (index.present()) {
				CODE_PROBE(true, "SingleKey case 1");
				return OpParam{ OpParam::Write,
					            index.get(),
					            -1,
					            keyForIndex(index.get()),
					            ""_sr,
					            UID(),
					            illegal_locked_range_access() };
			}
		} else {
			auto index = randomUnlockedIndex();
			if (index.present()) {
				CODE_PROBE(true, "SingleKey case 2");
				return OpParam{ OpParam::Write, index.get(), -1, keyForIndex(index.get()), ""_sr, UID(), {} };
			}
		}
		return {};
	}

	std::vector<OpParam> generateTestOperation() const {
		std::vector<OpParam> result;
		int opType = deterministicRandom()->randomInt(0, OpParam::MAX_OP_TYPE);
		Optional<OpParam> param;
		if (opType == OpParam::LockRange) {
			param = generateRandomParamForLock();
		} else if (opType == OpParam::UnlockRange) {
			param = generateRandomParamForUnlock();
		} else if (opType == OpParam::Write) {
			param = generateRandomParamSingleKeyMutation();
		} else if (opType == OpParam::Read) {
			param = generateRandomParamSingleKeyMutation();
			// FIXME: now we don't have read lock. So temporarily, set the error as empty;
			if (param.present()) {
				param.get().opType = OpParam::Read;
				param.get().error.reset();
			}
		}

		if (param.present()) {
			result.emplace_back(std::move(param.get()));
		}
		return result;
	}

	explicit RangeLockAPICorrectnessWorkload(WorkloadContext const& wcx) : KVWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 120.0);
		nodeCount = std::max(nodeCount, (uint64_t)alwaysLockedCount + 1);
	}

	static void traceOperations(std::vector<OpParam> const& ops) {
		for (int i = 0; i < ops.size(); ++i) {
			TraceEvent(SevDebug, "ExecuteOp", ops[i].lockId).detail("I", i).detail("Op", ops.at(i).toString());
		}
	}

	// How to do parallel test?
	ACTOR static Future<Optional<Version>> executeOperations(RangeLockAPICorrectnessWorkload* self,
	                                                         std::vector<OpParam> params,
	                                                         Database cx,
	                                                         Version minVersion,
	                                                         bool useRYW = true) {
		state Optional<Error> expectedError;
		state Optional<Version> txnVersion;
		state bool isUnknownResult = false;
		state Version lastReadVersion = 0;

		loop {
			txnVersion.reset();
			auto db = SystemDB(cx.getReference(), true);
			state Reference<ReadYourWritesTransaction> tr = db->createTransaction();
			db->setOptions(tr);

			if (!useRYW) {
				tr->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
			}

			try {
				wait(store(lastReadVersion, tr->getReadVersion()));
				if (lastReadVersion < minVersion) {
					wait(delay(1.0));
					continue;
				}

				state int i = 0;
				for (; i < params.size(); ++i) {
					state OpParam* param = &params[i];
					if (param->error.present())
						expectedError = param->error;

					if (param->opType == OpParam::LockRange) {
						wait(RangeLockAPI::lockRangesTransaction(tr, param->begin, param->end.get(), param->lockId));
					} else if (param->opType == OpParam::UnlockRange) {
						wait(RangeLockAPI::unlockRangesTransaction(tr, param->begin, param->end.get(), param->lockId));
					} else if (param->opType == OpParam::Write) {
						tr->set(param->begin, param->end.get());
					} else if (param->opType == OpParam::Clear) {
						tr->clear(KeyRangeRef(param->begin, param->end.get()));
					} else if (param->opType == OpParam::Read) {
						ValueReadResult v = wait(tr->get(param->begin));
						ASSERT_EQ(v, param->end);
					}
				}
				wait(tr->commit());
				txnVersion = std::max(tr->getCommittedVersion(), lastReadVersion);

				if (!params.empty()) {
					rl_debug(
					    "Op {} minVersion {} Txn Version {}\n", params[0].toString(), minVersion, txnVersion.get());
				}
				break;
			} catch (Error& error) {
				if (RangeLockAPI::isRangeLockError(error)) {
					TraceEvent(SevDebug, "ExecuteOp_Error").error(error);
					break;
				}
				if (error.code() == error_code_commit_unknown_result) {
					isUnknownResult = true;
				}
				wait(tr->onError(error));
			}
		}
		// the transaction can't succeed if we expect an error
		if (expectedError.present() && txnVersion.present()) {
			TraceEvent(SevError, "RangeLockAPITestError1")
			    .detail("Reason", "txn expected to be failed")
			    .detail("Version", txnVersion)
			    .detail("Error", expectedError.get().name());
			traceOperations(params);
			ASSERT(false);
		} else if (!expectedError.present() && !txnVersion.present()) {
			// the transaction failed though we expect it should succeed
			if (!isUnknownResult) {
				TraceEvent(SevError, "RangeLockAPITestError2")
				    .detail("Reason", "txn expected to be successful")
				    .detail("ReadVersion", lastReadVersion);
				traceOperations(params);
				ASSERT(false);
			}
			txnVersion = lastReadVersion;
		}

		return txnVersion;
	}

	ACTOR static Future<std::vector<RangeLockAPI::RangeLockInfoWithKeys>> readRangeLocks(Database cx,
	                                                                                     Version minVersion,
	                                                                                     KeyRef begin,
	                                                                                     KeyRef end) {
		loop {
			state Transaction tr(cx);
			loop {
				try {
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
					state Version version = wait(tr.getReadVersion());
					if (version <= minVersion) {
						wait(delay(1.0));
						break;
					}
					std::vector<RangeLockAPI::RangeLockInfoWithKeys> res =
					    wait(RangeLockAPI::listLockedRangesTransaction(&tr, begin, end));
					rl_debug("Version {}, Read Version {}\n", minVersion, version);
					return res;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
	}

	ACTOR static Future<int> readRangeLockCount(Database cx, Version minVersion) {
		loop {
			state Transaction tr(cx);
			loop {
				try {
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
					state Version version = wait(tr.getReadVersion());
					if (version <= minVersion) {
						wait(delay(1.0));
						break;
					}
					Optional<int> count = wait(RangeLockAPI::rangeLockMetadata().rangeLockCount().get(&tr));
					return count.get();
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
	}

	ACTOR static Future<Void> _setup(RangeLockAPICorrectnessWorkload* self, Database cx) {
		if (self->clientId != 0)
			return Void();
		state std::vector<Future<Optional<Version>>> lockFutures;
		state double start = now();
		lockFutures.reserve(self->alwaysLockedCount);
		for (int i = 0; i < self->alwaysLockedCount; ++i) {
			lockFutures.emplace_back(executeOperations(
			    self,
			    { OpParam{
			        OpParam::LockRange, i, i + 1, self->keyForIndex(i), self->keyForIndex(i + 1), UID(i, i + 1), {} } },
			    cx,
			    0));
		}
		wait(waitForAll(lockFutures));
		for (auto& f : lockFutures) {
			self->lastCommittedVersion = std::max(self->lastCommittedVersion, f.get().get());
		}
		TraceEvent("RangeLockSetupFinish")
		    .detail("Version", self->lastCommittedVersion)
		    .detail("Duration", now() - start)
		    .detail("AlwaysLockedCount", self->alwaysLockedCount);
		return Void();
	}

	Future<Void> setup(Database const& cx) override { return _setup(this, cx); }

	void updateWorkloadState(std::vector<OpParam> const& params, Optional<Version> const& version) {
		if (!version.present())
			return;
		lastCommittedVersion = std::max(lastCommittedVersion, version.get());
		for (auto& p : params) {
			if (p.opType == OpParam::LockRange) {
				lockedRangeIndex.emplace(p.beginIndex, p.endIndex);
			} else if (p.opType == OpParam::UnlockRange) {
				lockedRangeIndex.erase(p.beginIndex);
			}
		}
	}

	ACTOR static Future<Void> unitSimpleFailures(RangeLockAPICorrectnessWorkload* self, Database cx) {
		fmt::print("unitSimpleFailures Start\n");
		// disable ryw: unsupported operation
		{
			wait(success(executeOperations(self,
			                               { OpParam{ OpParam::UnlockRange,
			                                          0,
			                                          1,
			                                          self->keyForIndex(0),
			                                          self->keyForIndex(1),
			                                          UID(0, 1),
			                                          unsupported_operation() } },
			                               cx,
			                               self->lastCommittedVersion,
			                               /*useRYW=*/false)));
		}

		// lock system keys
		{
			wait(success(executeOperations(
			    self,
			    { OpParam{ OpParam::LockRange, 0, 1, "z"_sr, "\xff\x02"_sr, UID(0, 1), illegal_range_lock_param() } },
			    cx,
			    self->lastCommittedVersion)));
		}

		// normal key change with range lock
		int ib = self->alwaysLockedCount, ie = ib + 5;
		state std::vector<OpParam> params;
		{
			params = { { OpParam::Write, ib, ib, self->keyForIndex(ib), ""_sr, UID(ib, ib), {} },
				       { OpParam::LockRange,
				         ib,
				         ie,
				         self->keyForIndex(ib),
				         self->keyForIndex(ie),
				         UID(ib, ie),
				         illegal_range_lock_operation() } };
			wait(success(executeOperations(self, params, cx, self->lastCommittedVersion)));
		}

		// unlock use wrong lock Id
		{
			wait(success(executeOperations(self,
			                               { OpParam{ OpParam::UnlockRange,
			                                          0,
			                                          1,
			                                          self->keyForIndex(0),
			                                          self->keyForIndex(1),
			                                          UID(88, 99),
			                                          illegal_range_lock_param() } },
			                               cx,
			                               self->lastCommittedVersion)));
		}

		// read locked key with txn state change
		state Key stateKey = testOnlyTxnStateStorePrefixRange.begin.withSuffix("bad"_sr);
		{
			params = { { OpParam::Write, 1, -1, self->keyForIndex(1), ""_sr, UID(1, 1), {} },
				       { OpParam::Write, -1, -1, stateKey, "bad"_sr, UID(1, 1), illegal_locked_range_access() } };
			wait(success(executeOperations(self, params, cx, self->lastCommittedVersion)));
			// expect an empty result
			wait(success(executeOperations(self,
			                               { { OpParam::Read, -1, -1, stateKey, Optional<Key>(), UID(1, 1), {} } },
			                               cx,
			                               self->lastCommittedVersion)));
		}

		fmt::print("unitSimpleFailures Pass\n");
		return Void();
	}

	ACTOR static Future<Void> unitRangeMapCoalesceTest(RangeLockAPICorrectnessWorkload* self, Database cx) {
		fmt::print("unitRangeMapCoalesceTest Start\n");
		state std::vector<Future<Optional<Version>>> futures;
		// Unlock [1,2) [2,3),...[6,7) in parallel
		// each txn unlock [i, i+1), unlock [7-i, 8-i)
		{
			for (int i = 1; i <= 3; ++i) {
				futures.push_back(executeOperations(self,
				                                    { OpParam{ OpParam::UnlockRange,
				                                               i,
				                                               i + 1,
				                                               self->keyForIndex(i),
				                                               self->keyForIndex(i + 1),
				                                               UID(i, i + 1),
				                                               {} },
				                                      OpParam{ OpParam::UnlockRange,
				                                               7 - i,
				                                               8 - i,
				                                               self->keyForIndex(7 - i),
				                                               self->keyForIndex(8 - i),
				                                               UID(7 - i, 8 - i),
				                                               {} } },
				                                    cx,
				                                    self->lastCommittedVersion));
			}
			wait(waitForAll(futures));
			for (int i = 1; i <= 3; ++i) {
				self->lastCommittedVersion = std::max(self->lastCommittedVersion, futures[i - 1].get().get());
			}
			int count = wait(readRangeLockCount(cx, self->lastCommittedVersion));
			ASSERT_EQ(count, self->alwaysLockedCount - 6);
		}

		// Lock [2, 5), [5, 6) using same UID
		{
			Optional<Version> v1 = wait(executeOperations(
			    self,
			    { OpParam{ OpParam::LockRange, 2, 5, self->keyForIndex(2), self->keyForIndex(5), UID(2, 5), {} },
			      OpParam{ OpParam::LockRange, 5, 6, self->keyForIndex(5), self->keyForIndex(6), UID(2, 5), {} } },
			    cx,
			    self->lastCommittedVersion));
			self->lastCommittedVersion = std::max(self->lastCommittedVersion, v1.get());
			int count = wait(readRangeLockCount(cx, self->lastCommittedVersion));
			ASSERT_EQ(count, self->alwaysLockedCount - 4);
		}

		// should return [0, 1) [2, 5), [5, 6) [7, 8)
		state Key begin = self->keyForIndex(0);
		state Key end = self->keyForIndex(8);
		{
			std::vector<RangeLockAPI::RangeLockInfoWithKeys> locks =
			    wait(readRangeLocks(cx, self->lastCommittedVersion, begin, end));
			ASSERT_EQ(locks.size(), 4);
			ASSERT_EQ(self->indexForKey(locks[0].begin), 0);
			ASSERT_EQ(self->indexForKey(locks[0].end), 1);
			ASSERT_EQ(self->indexForKey(locks[1].begin), 2);
			ASSERT_EQ(self->indexForKey(locks[1].end), 5);
			ASSERT_EQ(self->indexForKey(locks[3].begin), 7);
			ASSERT_EQ(self->indexForKey(locks[3].end), 8);
		}

		// should return [2, 5)
		begin = self->keyForIndex(3);
		end = self->keyForIndex(4);
		{
			std::vector<RangeLockAPI::RangeLockInfoWithKeys> locks =
			    wait(readRangeLocks(cx, self->lastCommittedVersion, begin, end));
			ASSERT_EQ(locks.size(), 1);
			ASSERT_EQ(self->indexForKey(locks[0].begin), 2);
			ASSERT_EQ(self->indexForKey(locks[0].end), 5);
		}

		// lock [2, 4) using the same ID as lock[2, 5) should fail
		begin = self->keyForIndex(2);
		{
			wait(success(executeOperations(
			    self,
			    { OpParam{ OpParam::LockRange, 2, 4, begin, end, UID(2, 5), illegal_range_lock_param() } },
			    cx,
			    self->lastCommittedVersion)));
		}

		// tx1 (success): unlock [2, 6), lock [2, 3)
		// tx2 (fail): lock [3, 5), lock [4, 5) same id
		futures.clear();
		{
			futures.push_back(executeOperations(
			    self,
			    { OpParam{ OpParam::UnlockRange, 2, 5, self->keyForIndex(2), self->keyForIndex(5), UID(2, 5), {} },
			      OpParam{ OpParam::UnlockRange, 5, 6, self->keyForIndex(5), self->keyForIndex(6), UID(2, 5), {} },
			      OpParam{ OpParam::LockRange, 2, 3, self->keyForIndex(2), self->keyForIndex(3), UID(2, 3), {} } },
			    cx,
			    self->lastCommittedVersion));

			futures.push_back(executeOperations(self,
			                                    { OpParam{ OpParam::LockRange,
			                                               3,
			                                               5,
			                                               self->keyForIndex(3),
			                                               self->keyForIndex(5),
			                                               UID(3, 5),
			                                               illegal_range_lock_param() },
			                                      OpParam{ OpParam::LockRange,
			                                               4,
			                                               5,
			                                               self->keyForIndex(4),
			                                               self->keyForIndex(5),
			                                               UID(3, 5),
			                                               illegal_range_lock_param() } },
			                                    cx,
			                                    self->lastCommittedVersion));

			wait(waitForAll(futures));
			self->lastCommittedVersion = std::max(self->lastCommittedVersion, futures[0].get().get());
			int count = wait(readRangeLockCount(cx, self->lastCommittedVersion));
			ASSERT_EQ(count, self->alwaysLockedCount - 5);
		}

		// lock [1,2), unlock[2,3), lock [2,3) [3,4)...[6,7) in the same transaction
		state std::vector<OpParam> params;
		{
			for (int i = 1; i <= 6; ++i) {
				if (i == 2) {
					params.emplace_back(OpParam{ OpParam::UnlockRange,
					                             i,
					                             i + 1,
					                             self->keyForIndex(i),
					                             self->keyForIndex(i + 1),
					                             UID(i, i + 1),
					                             {} });
				}
				params.emplace_back(OpParam{
				    OpParam::LockRange, i, i + 1, self->keyForIndex(i), self->keyForIndex(i + 1), UID(i, i + 1), {} });
			}
			Optional<Version> v1 = wait(executeOperations(self, params, cx, self->lastCommittedVersion));
			int count = wait(readRangeLockCount(cx, v1.get()));
			ASSERT_EQ(count, self->alwaysLockedCount);
		}

		fmt::print("unitRangeMapCoalesceTest Pass\n");
		return Void();
	}

	ACTOR static Future<Void> unitWithTxnStateStoreChange(RangeLockAPICorrectnessWorkload* self, Database cx) {
		fmt::print("unitWithTxnStateStoreChange Start\n");
		state Key begin = self->keyForIndex(self->nodeCount);
		state Key end = self->keyForIndex(self->nodeCount + 1);
		state UID lockId(self->nodeCount, self->nodeCount + 1);
		state Key stateKey = testOnlyTxnStateStorePrefixRange.begin.withSuffix("unitWithTxnStateStoreChange"_sr);
		// txn A: set(begin, end), clear stateKey
		state Future<Void> work1 = runTransactionVoid(cx.getReference(), [=](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->set(begin, end);
			tr->clear(stateKey);
			return Future<Void>(Void());
		});

		// txn B: read range [begin, end), write stateKey, lockRange [begin, end)
		state Optional<Version> lastVersion;
		state Version readVersion = 0;
		loop {
			auto db = SystemDB(cx.getReference(), true);
			state Reference<ReadYourWritesTransaction> tr = db->createTransaction();
			db->setOptions(tr);
			try {
				tr->set(stateKey, stateKey);
				RangeReadResult readResult = wait(tr->getRange(KeyRangeRef(begin, end), 1));
				if (!readResult.empty()) {
					break;
				}
				wait(store(readVersion, tr->getReadVersion()));
				wait(RangeLockAPI::lockRangesTransaction(tr, begin, end, lockId));
				wait(tr->commit());
				lastVersion = std::max(readVersion, tr->getCommittedVersion());
				break;
			} catch (Error& error) {
				if (RangeLockAPI::isRangeLockError(error))
					break;
				wait(tr->onError(error));
			}
		}

		// txn A and txn B can't success together (but can fail together)
		state ErrorOr<Void> txnA = wait(errorOr(work1));
		if (lastVersion.present()) {
			CODE_PROBE(true, "Txn B lock success");
			ASSERT(txnA.isError());
			// read stateKey
			wait(success(executeOperations(self,
			                               { OpParam{ OpParam::Read, -1, -1, stateKey, stateKey, /*UID=*/{}, {} },
			                                 OpParam{ OpParam::UnlockRange, -1, -1, begin, end, lockId, {} } },
			                               cx,
			                               lastVersion.get())));
		}

		if (!txnA.isError()) {
			CODE_PROBE(true, "Txn A write success");
			ASSERT(!lastVersion.present());
		}
		fmt::print("unitWithTxnStateStoreChange Pass\n");

		return Void();
	}

	ACTOR static Future<Void> executeUnitTestCases(RangeLockAPICorrectnessWorkload* self, Database cx) {
		wait(unitSimpleFailures(self, cx));

		wait(unitRangeMapCoalesceTest(self, cx));

		wait(unitWithTxnStateStoreChange(self, cx));
		return Void();
	}

	ACTOR static Future<Void> worker(RangeLockAPICorrectnessWorkload* self, Database cx) {
		state double start = now();
		loop {
			state std::vector<OpParam> ops = self->generateTestOperation();
			state Optional<Version> version = wait(executeOperations(self, ops, cx, self->lastCommittedVersion));

			self->updateWorkloadState(ops, version);

			if (version.present()) {
				std::vector<RangeLockAPI::RangeLockInfoWithKeys> locks =
				    wait(readRangeLocks(cx, self->lastCommittedVersion, ""_sr, "\xff"_sr));

				if (locks.size() != self->alwaysLockedCount + self->lockedRangeIndex.size()) {
					TraceEvent(SevError, "ReadRangeLocksMismatch")
					    .detail("Version", self->lastCommittedVersion)
					    .detail("Size", locks.size())
					    .detail("ExpectSize", self->alwaysLockedCount + self->lockedRangeIndex.size());
					ASSERT(false);
				}
			}

			if (now() - start > self->testDuration)
				return Void();

			wait(delayJittered(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
		}
	}

	ACTOR static Future<Void> _start(RangeLockAPICorrectnessWorkload* self, Database cx) {
		wait(executeUnitTestCases(self, cx));

		Future<Void> workerFuture = worker(self, cx);
		wait(workerFuture);
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();

		return _start(this, cx);
	}

	ACTOR static Future<Void> clearAllRangeLocks(RangeLockAPICorrectnessWorkload* self, Database cx) {
		state std::vector<Future<Optional<Version>>> futures;
		for (int i = 0; i < self->alwaysLockedCount; ++i) {
			futures.push_back(executeOperations(self,
			                                    { OpParam{ OpParam::UnlockRange,
			                                               i,
			                                               i + 1,
			                                               self->keyForIndex(i),
			                                               self->keyForIndex(i + 1),
			                                               UID(i, i + 1),
			                                               {} } },
			                                    cx,
			                                    self->lastCommittedVersion));
		}

		std::vector<RangeLockAPI::RangeLockInfoWithKeys> locks =
		    wait(readRangeLocks(cx, self->lastCommittedVersion, ""_sr, "\xff"_sr));

		for (auto& lock : locks) {
			futures.push_back(executeOperations(self,
			                                    { OpParam{ OpParam::UnlockRange,
			                                               (int)lock.info.lockId.first(),
			                                               (int)lock.info.lockId.second(),
			                                               lock.begin,
			                                               lock.end,
			                                               lock.info.lockId,
			                                               {} } },
			                                    cx,
			                                    self->lastCommittedVersion));
		}

		wait(waitForAll(futures));
		return Void();
	}

	ACTOR static Future<bool> _check(RangeLockAPICorrectnessWorkload* self, Database cx) {
		wait(clearAllRangeLocks(self, cx));
		return true;
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0)
			return true;
		return _check(this, cx);
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<RangeLockAPICorrectnessWorkload> RangeLockAPICorrectnessWorkloadFactory;