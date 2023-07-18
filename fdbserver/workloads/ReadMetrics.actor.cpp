/*
 * ReadMetrics.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/FDBOptions.g.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct ReadMetricsWorkload : KVWorkload {
	static constexpr auto NAME = "ReadMetrics";

	double testDuration;
	int actorCount;
	int numDistinctOperations;

	int totalOps = 0;
	int successfulOps = 0;

	enum class OperationType { GET_VALUE, GET_RANGE, GET_KEY, END };

	struct Operation {
		OperationType type;
		KeySelector begin;
		KeySelector end;
		int limit = 0;
		bool reverse = false;
		bool snapshot = false;
		bool disableRyw = false;
		double expectedAccuracy = 0.0;
	};

	Operation randomOperation(bool isPrimaryOperation) const {
		Operation op;
		op.type = (OperationType)deterministicRandom()->randomInt(0, (int)OperationType::END);
		op.disableRyw = deterministicRandom()->coinflip();

		if (op.type == OperationType::GET_VALUE) {
			op.begin = firstGreaterOrEqual(getRandomKey());
			op.expectedAccuracy = 0.6;
		} else if (op.type == OperationType::GET_RANGE) {
			Arena arena;
			bool largeOffsets = deterministicRandom()->coinflip();
			op.begin = op.disableRyw ? firstGreaterOrEqual(getRandomKey()) : randomKeySelector(largeOffsets);
			op.end = op.disableRyw ? firstGreaterOrEqual(getRandomKey()) : randomKeySelector(largeOffsets);

			KeySelectorRef normalizedBegin = op.begin;
			KeySelectorRef normalizedEnd = op.end;

			normalizedBegin.removeOrEqual(arena);
			normalizedEnd.removeOrEqual(arena);

			// If the range is known empty, then it won't send a request to the SS
			if (normalizedBegin.getKey() == normalizedEnd.getKey() && normalizedBegin.offset == normalizedEnd.offset) {
				op.end = KeySelectorRef(keyAfter(op.end.getKey()), op.end.orEqual, op.end.offset);
			} else if (normalizedBegin.getKey() >= normalizedEnd.getKey() &&
			           normalizedBegin.offset >= normalizedEnd.offset) {
				std::swap(op.begin, op.end);
			}

			op.limit = deterministicRandom()->randomInt(1, largeOffsets && isPrimaryOperation ? 1000 : 10);
			op.reverse = deterministicRandom()->coinflip();
			op.snapshot = deterministicRandom()->coinflip();
			op.expectedAccuracy = largeOffsets ? 0.333 : 0.5;
		} else if (op.type == OperationType::GET_KEY) {
			bool largeOffsets = deterministicRandom()->coinflip();
			op.begin = randomKeySelector(largeOffsets);
			op.expectedAccuracy = 0.6;
		} else {
			ASSERT(false);
		}

		return op;
	}

	Operation repeatedOperation;
	bool highFrequency;

	ReadMetricsWorkload(WorkloadContext const& wcx) : KVWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 300.0);
		actorCount = getOption(options, "actorCount"_sr, 20);
		numDistinctOperations = getOption(options, "numDistinctOperations"_sr, 10);

		repeatedOperation = randomOperation(true);
		highFrequency = deterministicRandom()->coinflip();

		if (clientId == 0) {
			TraceEvent("ReadMetricsRepeatedOperation")
			    .detail("OperationType", repeatedOperation.type)
			    .detail("Begin", repeatedOperation.begin)
			    .detail("End", repeatedOperation.end)
			    .detail("DisableRyw", repeatedOperation.disableRyw)
			    .detail("Limit", repeatedOperation.limit)
			    .detail("Reverse", repeatedOperation.reverse)
			    .detail("Snapshot", repeatedOperation.snapshot)
			    .detail("ExpectedAccuracy", repeatedOperation.expectedAccuracy);
		}

		// We don't start checking the results until we've had enough time to aggregate metrics, and the test needs
		// to be long enough that we start doing these checks
		ASSERT_GE(testDuration, 2 * SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL);
	}

	KeySelector randomKeySelector(bool largeOffsets) const {
		int maxOffset = largeOffsets ? 100 : 10;
		int offset = deterministicRandom()->randomInt(-maxOffset, maxOffset + 1);
		return KeySelectorRef(getRandomKey(), deterministicRandom()->coinflip(), offset);
	}

	ACTOR static Future<Void> _setup(Database cx, ReadMetricsWorkload* self) {
		wait(bulkSetup(cx, self, self->nodeCount, Promise<double>()));

		// Balance the data and then disable data distribution to avoid interference from data movement reads
		if (self->clientId == 0) {
			fmt::print("Read metrics waiting for quiet database\n");
			wait(quietDatabase(cx, self->dbInfo, "ReadMetricsStart"));
			fmt::print("Waiting for quiet database done\n");
			wait(success(setDDMode(cx, 0)));
			fmt::print("Data distribution disabled\n");
			wait(delay(60.0));
			fmt::print("Pre-test wait complete\n");
		}

		return Void();
	}

	Future<Void> setup(const Database& cx) override { return _setup(cx, this); }

	ACTOR static Future<Void> readActor(Database cx, ReadMetricsWorkload* self) {
		state double start = now();
		state double selectionProbability = 0.90;
		while (self->clientId == 0 && now() - start < self->testDuration) {
			state Operation op;
			state ReadMetrics readMetrics;
			state bool useRepeatedOperation = deterministicRandom()->random01() < selectionProbability;

			if (useRepeatedOperation) {
				op = self->repeatedOperation;
			} else {
				op = self->randomOperation(false);
			}

			if (op.type == OperationType::GET_VALUE) {
				fmt::print("Running op get value: {} {} {} {}\n",
				           now(),
				           useRepeatedOperation,
				           op.disableRyw,
				           printable(op.begin.getKey()));
				Optional<ValueReadResult> read = wait(timeout(runRYWTransaction(cx, [op = op](Reference<ReadYourWritesTransaction> tr) {
					if (op.disableRyw) {
						tr->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
					}
					return tr->get(op.begin.getKey());
				}), 15));

				if (!read.present()) {
					continue;
				}

				readMetrics = read.get().getReadMetrics();
			} else if (op.type == OperationType::GET_RANGE) {
				fmt::print("Running op get range: {} {} {} {} {} {} {} {}\n",
				           now(),
				           useRepeatedOperation,
				           op.disableRyw,
				           op.begin.toString(),
				           op.end.toString(),
				           op.reverse,
				           op.limit,
				           op.snapshot);
				Optional<RangeReadResult> read = wait(timeout(runRYWTransaction(cx, [op = op](Reference<ReadYourWritesTransaction> tr) {
					if (op.disableRyw) {
						tr->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
					}
					return tr->getRange(op.begin, op.end, op.limit, Snapshot(op.snapshot), Reverse(op.reverse));
				}), 15));

				if (!read.present()) {
					continue;
				}

				readMetrics = read.get().getReadMetrics();
			} else {
				fmt::print("Running op get key: {} {} {} {}\n",
				           now(),
				           useRepeatedOperation,
				           op.disableRyw,
				           op.begin.toString());
				Optional<KeyReadResult> read = wait(timeout(runRYWTransaction(cx, [op = op](Reference<ReadYourWritesTransaction> tr) {
					if (op.disableRyw) {
						tr->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
					}
					return tr->getKey(op.begin);
				}), 15));

				if (!read.present()) {
					continue;
				}

				readMetrics = read.get().getReadMetrics();
			}

			if (useRepeatedOperation && now() - start > SERVER_KNOBS->STORAGE_METRICS_AVERAGE_INTERVAL) {
				++self->totalOps;
				double expectedBusyness = op.expectedAccuracy * selectionProbability;
				if (readMetrics.getRangeBusyness() > expectedBusyness) {
					++self->successfulOps;
				} else {
					ASSERT_GT(readMetrics.getRangeBusyness(), 0);
					TraceEvent("OperationWithLowReadBusyness")
					    .detail("ExpectedBusyness", expectedBusyness)
					    .detail("ActualBusyness", readMetrics.getRangeBusyness());
				}
			}
		}

		ASSERT_GE(self->successfulOps, self->totalOps * 0.9);
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		std::vector<Future<Void>> readActors;
		if (clientId == 0) {
			for (int i = 0; i < 20; ++i) {
				readActors.push_back(readActor(cx, this));
			}
		}

		return waitForAll(readActors);
	}

	KeyValue operator()(uint64_t n) {
		return KeyValueRef(keyForIndex(n), deterministicRandom()->randomAlphaNumeric(maxValueBytes));
	}

	ACTOR static Future<bool> _check(Database cx) {
		// Reenable data distribution for the post-test consistency check
		wait(success(setDDMode(cx, 1)));
		fmt::print("Data distribution enabled\n");
		return true;
	}
	Future<bool> check(Database const& cx) override {
		if (clientId == 0) {
			return _check(cx);
		} else {
			return true;
		}
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ReadMetricsWorkload> ReadMetricsWorkloadFactory;
