/*
 * BlobGranuleBigTuplesWorkload.actor.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BGCorrectnessUtils.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/Util.h"
#include <cstring>
#include <limits>

#include "flow/actorcompiler.h" // This must be the last #include.

/*
 * Makes tuples of length > 2 to verify that we don't double-align and thus truncate tuples anywhere
 */
struct BlobGranuleBigTuplesWorkload : TestWorkload {
	static constexpr auto NAME = "BlobGranuleBigTuples";
	int targetGranules;
	bool initAfter;
	int nodeCount;
	int targetValueLen;
	int bucketSize;

	Optional<TenantName> tenantName;
	Optional<Reference<Tenant>> tenant;

	BlobGranuleBigTuplesWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		int granuleVariance = std::max(SERVER_KNOBS->BG_MAX_SPLIT_FANOUT, 6);
		targetGranules = 3 + sharedRandomNumber % granuleVariance;
		sharedRandomNumber /= granuleVariance;
		sharedRandomNumber /= 6;
		initAfter = (sharedRandomNumber % 4) == 0;
		sharedRandomNumber /= 4;
		targetValueLen = 100 * (1 + sharedRandomNumber % 10);
		sharedRandomNumber /= 10;

		int64_t targetBytes = targetGranules * SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES;
		targetBytes = std::max<int64_t>(
		    1000000, targetBytes); // write at least 1 MB to avoid very small granule/byte sample issues
		nodeCount = (int)(targetBytes / targetValueLen);

		bucketSize = deterministicRandom()->randomSkewedUInt32(1, nodeCount);

		tenantName = "bgBigTuplesTenant"_sr;

		// FIXME: maybe enable for completeness at some point?
		initAfter = false;

		TraceEvent("BlobGranuleBigTuplesWorkloadInit")
		    .detail("TargetGranules", targetGranules)
		    .detail("InitAfter", initAfter)
		    .detail("TargetValSize", targetValueLen)
		    .detail("TargetBytes", targetBytes)
		    .detail("GranuleSize", SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES)
		    .detail("NodeCount", nodeCount);
	}

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	ACTOR Future<Void> setUpBlobRange(Database cx, BlobGranuleBigTuplesWorkload* self) {
		bool success = wait(cx->blobbifyRange(normalKeys, self->tenant));
		ASSERT(success);
		return Void();
	}

	// Functions required by `bulkSetup()`
	// key is always a 3-tuple with the same first element, and 2 ints for the last element, that share common prefixes
	Key keyForIndex(int n) { return Tuple::makeTuple(11, n / bucketSize, n % bucketSize).pack(); }
	Value value(int n) {
		int valLen = deterministicRandom()->randomInt(1, 2 * targetValueLen);
		valLen = std::max(10, valLen);
		return genVal(n, valLen);
	}
	Standalone<KeyValueRef> operator()(int n) { return KeyValueRef(keyForIndex(n), value(n)); }

	ACTOR Future<Void> _setup(Database cx, BlobGranuleBigTuplesWorkload* self) {
		if (self->clientId != 0) {
			return Void();
		}
		TraceEvent("BlobGranuleBigTuplesInit")
		    .detail("TargetGranules", self->targetGranules)
		    .detail("InitAfter", self->initAfter)
		    .detail("BucketSize", self->bucketSize);

		// set up blob granules
		wait(success(ManagementAPI::changeConfig(cx.getReference(), "blob_granules_enabled=1", true)));

		Optional<TenantMapEntry> entry = wait(TenantAPI::createTenant(cx.getReference(), self->tenantName.get()));
		ASSERT(entry.present());
		self->tenant = makeReference<Tenant>(cx, self->tenantName.get());

		if (!self->initAfter) {
			wait(self->setUpBlobRange(cx, self));

			TraceEvent("BlobGranuleBigTuplesSetupVerifying");
			loop {
				Version checkVersion = wait(cx->verifyBlobRange(normalKeys, latestVersion, self->tenant));
				if (checkVersion != -1) {
					break;
				}

				TraceEvent("BlobGranuleBigTuplesSetupVerifyRetrying");

				wait(delay(1.0));
			}
		}

		TraceEvent("BlobGranuleBigTuplesLoading");

		// we only have one client and bulk setup divides the writes amongst them, so multiply node count by client
		// count
		wait(bulkSetup(cx,
		               self,
		               self->nodeCount * self->clientCount,
		               Promise<double>(),
		               true,
		               0.0,
		               1e12,
		               std::vector<uint64_t>(),
		               Promise<std::vector<std::pair<uint64_t, double>>>(),
		               0,
		               0.1,
		               0,
		               0,
		               { self->tenant.get() }));

		TraceEvent("BlobGranuleBigTuplesLoadingComplete");

		if (self->initAfter) {
			wait(self->setUpBlobRange(cx, self));
		}

		TraceEvent("BlobGranuleBigTuplesSetupComplete");
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		// no test phase
		return Void();
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }

	ACTOR Future<bool> _check(Database cx, BlobGranuleBigTuplesWorkload* self) {
		if (self->clientId != 0) {
			return true;
		}
		state Key tuplePrefix = Tuple::makeTuple(11).pack();
		// FIXME: checking normalKeys finds another empty granule, that's metadata overhead we should fix at some point
		state KeyRange tupleRange(KeyRangeRef(tuplePrefix, strinc(tuplePrefix)));
		TraceEvent("BlobGranuleBigTuplesCheckStart").detail("Range", tupleRange);
		loop {
			Version checkVersion = wait(cx->verifyBlobRange(tupleRange, latestVersion, self->tenant));
			if (checkVersion != -1) {
				TraceEvent("BlobGranuleBigTuplesCheckRead").detail("CheckVersion", checkVersion);
				break;
			}

			TraceEvent("BlobGranuleBigTuplesCheckRetrying");

			wait(delay(1.0));
		}

		// check that all of the granule boundaries are 2-tuples that start with 11
		state Transaction tr = Transaction::fromTenantOrDefault(self->tenant, cx);
		loop {
			try {
				Standalone<VectorRef<KeyRangeRef>> granules = wait(tr.getBlobGranuleRanges(tupleRange, 1000000));
				TraceEvent("BlobGranuleBigTuplesCheckGranules").detail("GranuleCount", granules.size());
				for (int i = 0; i < granules.size() - 1; i++) {
					ASSERT(granules[i].end == granules[i + 1].begin);
					Tuple t = Tuple::unpackUserType(granules[i].end, true);
					if (SERVER_KNOBS->BG_ENABLE_SPLIT_TRUNCATED) {
						ASSERT(2 == t.size() || 3 == t.size());
					} else {
						ASSERT_EQ(2, t.size());
					}

					TraceEvent("BlobGranuleBigTuplesBoundary")
					    .detail("Idx", i)
					    .detail("T0", t.getInt(0))
					    .detail("T1", t.getInt(1));
					ASSERT(t.getInt(0) == 11);
					int v1 = t.getInt(1);
					ASSERT(v1 >= 0 && v1 <= self->nodeCount / self->bucketSize);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<BlobGranuleBigTuplesWorkload> BlobGranuleBigTuplesWorkloadFactory;
