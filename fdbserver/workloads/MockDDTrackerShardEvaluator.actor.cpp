/*
 * MockDDTrackerShardEvaluator.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/workloads/MockDDTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class MockDDTrackerShardEvaluatorWorkload : public MockDDTestWorkload {
public:
	static constexpr auto NAME = "MockDDTrackerShardEvaluator";
	DDSharedContext ddcx;
	Reference<DDMockTxnProcessor> mock;

	PromiseStream<RelocateShard> output;
	PromiseStream<GetMetricsRequest> getShardMetrics;
	PromiseStream<GetTopKMetricsRequest> getTopKMetrics;
	PromiseStream<GetMetricsListRequest> getShardMetricsList;
	PromiseStream<Promise<int64_t>> getAverageShardBytes;

	KeyRangeMap<ShardTrackedData> shards;

	ActorCollection actors;

	std::map<RelocateReason, int> rsReasonCounts;
	StorageMetrics totalMetrics, thresholdMetrics;

	Reference<DataDistributionTracker> shardTracker;

	// --- test configs ---

	double shardMetricsDelay = 0;
	// check threshold
	int checkMinShardCount = 1;
	int checkMinSizeSplit = 0;
	int checkMinWriteSplit = 0;

	MockDDTrackerShardEvaluatorWorkload(WorkloadContext const& wcx)
	  : MockDDTestWorkload(wcx),
	    ddcx(deterministicRandom()->randomUniqueID()) {
		checkMinShardCount = getOption(options, "checkMinShardCount"_sr, checkMinShardCount);
		checkMinSizeSplit = getOption(options, "checkMinSizeSplit"_sr, checkMinSizeSplit);
		checkMinWriteSplit = getOption(options, "checkMinWriteSplit"_sr, checkMinWriteSplit);
		shardMetricsDelay = getOption(options, "shardMetricsDelay"_sr, shardMetricsDelay);

		thresholdMetrics = ShardSizeBounds::shardSizeBoundsBeforeTrack().min;
		thresholdMetrics.opsReadPerKSecond = getOption(options, "MinSampledReadOps"_sr, getPerSecond);
	}

	Future<Void> setup(Database const& cx) override {
		if (!enabled)
			return Void();
		MockDDTestWorkload::setup(cx);
		// populate sharedMgs before run tracker
		populateMgs();
		mock = makeReference<DDMockTxnProcessor>(sharedMgs);
		return Void();
	}

	ACTOR static Future<Void> traceStorageMetrics(MockDDTrackerShardEvaluatorWorkload* self,
	                                              double delaySec,
	                                              KeyRange keys,
	                                              bool addToTotal = false) {
		wait(delay(delaySec));
		GetMetricsRequest req(keys);
		self->getShardMetrics.send(req);
		StorageMetrics metrics = wait(req.reply.getFuture());
		DisabledTraceEvent("MockDDTracker_ShardMetrics")
		    .detail("DelayAfter", delaySec)
		    .detail("Keys", keys)
		    .detail("StorageMetrics", metrics.toString());

		if (addToTotal) {
			self->totalMetrics += metrics;
		}

		return Void();
	}

	ACTOR static Future<Void> relocateShardReporter(MockDDTrackerShardEvaluatorWorkload* self,
	                                                FutureStream<RelocateShard> input) {
		loop {
			try {
				choose {
					when(RelocateShard rs = waitNext(input)) {
						++self->rsReasonCounts[rs.reason];
						self->actors.add(traceStorageMetrics(self, 0, rs.keys));
						if (self->shardMetricsDelay > 0) {
							self->actors.add(traceStorageMetrics(self, self->shardMetricsDelay, rs.keys, true));
						}
					}
				}
			} catch (Error& e) {
				if (e.code() != error_code_wrong_shard_server)
					throw e;
			}
			wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY));
		}
	}

	Future<Void> start(Database const& cx) override {
		if (!enabled)
			return Void();

		// start mock servers
		actors.add(waitForAll(sharedMgs->runAllMockServers()));

		// start tracker
		Reference<InitialDataDistribution> initData =
		    mock->getInitialDataDistribution(ddcx.id(), ddcx.lock, {}, ddcx.ddEnabledState.get(), SkipDDModeCheck::True)
		        .get();
		Reference<PhysicalShardCollection> physicalShardCollection = makeReference<PhysicalShardCollection>();
		Reference<AsyncVar<bool>> zeroHealthyTeams = makeReference<AsyncVar<bool>>(false);

		shardTracker = makeReference<DataDistributionTracker>(
		    DataDistributionTrackerInitParams{ .db = mock,
		                                       .distributorId = ddcx.id(),
		                                       .readyToStart = Promise<Void>(),
		                                       .output = output,
		                                       .shardsAffectedByTeamFailure = ddcx.shardsAffectedByTeamFailure,
		                                       .physicalShardCollection = physicalShardCollection,
		                                       .anyZeroHealthyTeams = zeroHealthyTeams,
		                                       .shards = &shards,
		                                       .trackerCancelled = &ddcx.trackerCancelled,
		                                       .ddTenantCache = {} });
		actors.add(DataDistributionTracker::run(shardTracker,
		                                        initData,
		                                        getShardMetrics.getFuture(),
		                                        getTopKMetrics.getFuture(),
		                                        getShardMetricsList.getFuture(),
		                                        getAverageShardBytes.getFuture()));
		actors.add(relocateShardReporter(this, output.getFuture()));

		if (getPerSecond > 0) {
			actors.add(randomGetClient(1.0 / getPerSecond, 0, keySpaceCount));
		}

		return timeout(reportErrors(actors.getResult(), "MockDDTrackerShardEvaluatorWorkload"), testDuration, Void());
	}

	Future<bool> check(Database const& cx) override {
		if (!enabled)
			return true;

		fmt::print("Check phase shards count: {}\n", shards.size());
		ASSERT_GE(shards.size(), checkMinShardCount);
		for (auto& [r, c] : rsReasonCounts) {
			fmt::print("{}: {}\n", r.toString(), c);
		}
		ASSERT_GE(rsReasonCounts[RelocateReason::SIZE_SPLIT], checkMinSizeSplit);
		ASSERT_GE(rsReasonCounts[RelocateReason::WRITE_SPLIT], checkMinWriteSplit);

		fmt::print("Check Total Storage Metrics: {}\n", totalMetrics.toString());
		if (!thresholdMetrics.allLessOrEqual(totalMetrics)) {
			TraceEvent(SevError, "MockDDTracker_Failed")
			    .detail("Reason", "Wrong Storage Metrics")
			    .detail("Threshold", thresholdMetrics.toString())
			    .detail("Actual", totalMetrics.toString());
		}

		actors.clear(true);
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		if (!enabled)
			return;

		for (const auto& [reason, count] : rsReasonCounts) {
			m.push_back(PerfMetric(RelocateReason(reason).toString(), count, Averaged::False));
		}
		m.emplace_back(
		    totalReadsMetric->metricName.toString(), totalReadsMetric.getValue() / testDuration, Averaged::True);
	}
};

WorkloadFactory<MockDDTrackerShardEvaluatorWorkload> MockDDTrackerShardEvaluatorWorkload;