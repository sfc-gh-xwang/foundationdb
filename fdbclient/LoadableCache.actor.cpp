/*
 * LoadableCache.cpp
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

#include "fdbclient/LoadableCache.actor.h"

#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"
#include "flow/flow.h"

#include <unordered_map>
#include <unordered_set>

#define DEBUG_LOADABLE_CACHE_WORKLOAD 0

namespace {
struct Int64Hash {
	std::size_t operator()(int64_t key) const { return static_cast<std::size_t>(key); }
};

struct Int64Equal {
	bool operator()(int64_t lhs, int64_t rhs) const { return lhs == rhs; }
};

// Workload generator for validation LoadableCache functionality; salient poitns
// 1. Implement in-memory caching using 'LoadableCache' for KeyType = 'int64_t' & ValueType = 'int64_t'
// 2. Test cases covered includes:
//  2.1. Invalid key lookup.
//  2.2. Single valid key lookup.
//  2.3. Vector (valid) key lookup.
//  2.4. Vector (valid + invalid) key lookup.
//  2.5. Spin multiple actors each randomly accessing keyspace, further, BUGGIFY to include "invalid keys" lookup too

class LoadableCacheWorkload : public ReferenceCounted<LoadableCacheWorkload>, NonCopyable {
	using KVMap = std::unordered_map<int64_t, int64_t, Int64Hash, Int64Equal>;

public:
	int numIterations;
	int itemCount;
	int nWorkers;
	Reference<LoadableCacheOpStats> wStats;
	std::vector<int64_t> sourceVec;
	std::unordered_map<int64_t, int64_t> sourceCache;
	Reference<LoadableCache<int64_t, int64_t, Int64Hash, Int64Equal>> cache;
	bool failKeyLoads = false;

	LoadableCacheWorkload() { init(); }

	ACTOR Future<Void> run(Reference<LoadableCacheWorkload> self) {
		TraceEvent("LoadableCacheWorkloadStart");

		wait(testInvalidKey(self));
		wait(testLoadOnMiss(self, 0, 1.0));
		wait(delay(1.001));
		wait(testLoadExpiredKey(self, 0));
		wait(testCacheHits(self, 0));
		wait(testLoadKeys(self, 1, 5));
		wait(testLoadKeysWithInvalidKeys(self, 1, 5));
		wait(testParallelLookups(self));

		self->cache->trim();

		TraceEvent("LoadableCacheWorkloadEnd")
		    .detail("NumHits", self->wStats->nHits)
		    .detail("NumDeups", self->wStats->nDedups)
		    .detail("NumFetches", self->wStats->nFetched);

		// ensure cache key populate adheres to `almost once` semantics; account for expiredKey test
		ASSERT_EQ(self->wStats->nFetched, self->cache->size() + 1);

		return Void();
	}

private:
	void init() {
		numIterations = deterministicRandom()->randomInt(512, 786);
		itemCount = deterministicRandom()->randomInt(32768, 65536);
		nWorkers = deterministicRandom()->randomInt(5, 12);
		std::function<Future<Void>(void* data, const std::vector<int64_t>&, KVMap*)> loaderFunc =
		    &LoadableCacheWorkload::loadKeys;
		cache = makeReference<LoadableCache<int64_t, int64_t, Int64Hash, Int64Equal>>(loaderFunc);

		for (int i = 0; i < itemCount; i++) {
			sourceCache[i] = i + 1024;
			sourceVec.push_back(i);
		}
		wStats = makeReference<LoadableCacheOpStats>();
		TraceEvent("LoadableCacheWorkload").detail("ItemCount", itemCount);
	}

	static void validate(Reference<LoadableCacheWorkload> self, const KVMap& toValidate) {
		for (const auto entry : toValidate) {
			auto it = self->sourceCache.find(entry.first);
			if (it != self->sourceCache.end()) {
				ASSERT_EQ(it->second, entry.second);
			} else {
				ASSERT_GE(entry.first, self->itemCount);
			}
		}
	}

	static void updateWorkloadStats(Reference<LoadableCacheWorkload> self, Reference<LoadableCacheOpStats> stats) {
		self->wStats->nHits += stats->nHits;
		self->wStats->nDedups += stats->nDedups;
		self->wStats->nFetched += stats->nFetched;
	}

	ACTOR static Future<Void> loadKeysImpl(LoadableCacheWorkload* self, std::vector<int64_t> keys, KVMap* result) {
		if (self->failKeyLoads && deterministicRandom()->randomInt(0, 100) < 5) {
			if (DEBUG_LOADABLE_CACHE_WORKLOAD) {
				TraceEvent("LoadableCacheInjectError");
			}
			throw operation_failed();
		}

		for (const auto key : keys) {
			auto it = self->sourceCache.find(key);
			if (it != self->sourceCache.end()) {
				result->emplace(it->first, it->second);
				if (DEBUG_LOADABLE_CACHE_WORKLOAD) {
					TraceEvent("LoadKeyImpl").detail("Key", key);
				}
			} else if (DEBUG_LOADABLE_CACHE_WORKLOAD) {
				TraceEvent(SevDebug, "LoadKeysNotFound").detail("Key", key);
			}
		}

		state double loadTimeMS = (deterministicRandom()->randomInt(1, 3) * 1.0) / 1000;
		wait(delay(loadTimeMS));
		return Void();
	}

	static Future<Void> loadKeys(void* data, const std::vector<int64_t>& keys, KVMap* result) {
		ASSERT(data != nullptr);
		return loadKeysImpl(static_cast<LoadableCacheWorkload*>(data), keys, result);
	}

	static std::vector<int64_t> getLookupVec(Reference<LoadableCacheWorkload> self, bool* invalidKeyAdded) {
		int nLookups = std::max(deterministicRandom()->randomInt(1, self->itemCount / 4), 1);
		std::unordered_set<int64_t> s;

		*invalidKeyAdded = false;

		while (nLookups) {
			int idx = deterministicRandom()->randomInt(0, self->itemCount);
			if (s.count(idx) == 0) {
				s.insert(idx);
				nLookups--;
			}
		}

		if (BUGGIFY && deterministicRandom()->randomInt(0, 100) < 2) {
			// insert one or more "invalid" keys as part of lookup
			int nKeys = deterministicRandom()->randomInt(1, 10);
			while (nKeys--) {
				s.insert(self->itemCount + nKeys);
			}
			*invalidKeyAdded = true;
		}

		return std::vector<int64_t>(s.begin(), s.end());
	}

	ACTOR static Future<Void> worker(Reference<LoadableCacheWorkload> self, int wId) {
		state int i;
		state Reference<LoadableCacheOpStats> stats = makeReference<LoadableCacheOpStats>();
		state bool missesExpected;

		ASSERT_EQ(self->sourceCache.size(), self->itemCount);

		TraceEvent("TestParallelLookupStart").detail("WId", wId);
		for (i = 0; i < self->numIterations; i++) {
			state std::vector<int64_t> lookupVec = getLookupVec(self, &missesExpected);

			try {
				state KVMap result = wait(self->cache->get(lookupVec, self.getPtr(), Optional<double>(), stats));
				ASSERT(result.size() == lookupVec.size() || missesExpected);
				validate(self, result);
				updateWorkloadStats(self, stats);
			} catch (Error& e) {
				// expected error
				if (e.code() == error_code_broken_promise) {
					TraceEvent("WorkloadError").error(e);
				}
				ASSERT_EQ(e.code(), error_code_operation_failed);
			}
		}
		TraceEvent("TestParallelLookupEnd").detail("WId", wId);

		return Void();
	}

	ACTOR static Future<Void> testParallelLookups(Reference<LoadableCacheWorkload> self) {
		state std::vector<Future<Void>> futures;

		TraceEvent("TestParallelLookupsStart");
		self->failKeyLoads = true;
		for (int i = 0; i < self->nWorkers; i++) {
			futures.push_back(worker(self, i));
		}
		wait(waitForAll(futures));
		self->failKeyLoads = false;
		TraceEvent("TestParallelLookupsEnd");

		return Void();
	}

	ACTOR static Future<Void> testInvalidKey(Reference<LoadableCacheWorkload> self) {
		state Reference<LoadableCacheOpStats> stats = makeReference<LoadableCacheOpStats>();

		TraceEvent("TestInvalidKey");
		Optional<int64_t> value =
		    wait(self->cache->get(self->itemCount + 100, self.getPtr(), Optional<double>(), stats));
		ASSERT(!value.present());
		ASSERT_EQ(stats->nHits, 0);
		ASSERT_EQ(stats->nDedups, 0);
		ASSERT_EQ(stats->nFetched, 0);
		updateWorkloadStats(self, stats);
		TraceEvent("TestInvalidKey");

		return Void();
	}

	ACTOR static Future<Void> testLoadOnMiss(Reference<LoadableCacheWorkload> self,
	                                         int64_t key,
	                                         double expDurationSec) {
		state Reference<LoadableCacheOpStats> stats = makeReference<LoadableCacheOpStats>();

		TraceEvent("TestLoadOnMissStart");
		Optional<int64_t> value = wait(self->cache->get(key, self.getPtr(), expDurationSec, stats));
		ASSERT(value.present());
		validate(self, { { key, value.get() } });
		ASSERT_EQ(stats->nHits, 0);
		ASSERT_EQ(stats->nDedups, 0);
		ASSERT_EQ(stats->nFetched, 1);
		updateWorkloadStats(self, stats);
		TraceEvent("TestLoadOnMissEnd");

		return Void();
	}

	ACTOR static Future<Void> testLoadExpiredKey(Reference<LoadableCacheWorkload> self, int64_t key) {
		state Reference<LoadableCacheOpStats> stats = makeReference<LoadableCacheOpStats>();

		TraceEvent("TestLoadExpiredKeyStart");
		Optional<int64_t> value = wait(self->cache->get(key, self.getPtr(), Optional<double>(), stats));
		ASSERT(value.present());
		validate(self, { { key, value.get() } });
		ASSERT_EQ(stats->nHits, 0);
		ASSERT_EQ(stats->nDedups, 0);
		ASSERT_EQ(stats->nFetched, 1);
		updateWorkloadStats(self, stats);
		TraceEvent("TestLoadExpiredKeyEnd");

		return Void();
	}

	ACTOR static Future<Void> testCacheHits(Reference<LoadableCacheWorkload> self, int64_t key) {
		state int i;
		state Reference<LoadableCacheOpStats> stats = makeReference<LoadableCacheOpStats>();
		state int count = deterministicRandom()->randomInt(10, 20);

		TraceEvent("TestCacheHitsStart");
		// ensure repeated access to cached keys result in cache-hits
		for (i = 0; i < count; i++) {
			Optional<int64_t> value = wait(self->cache->get(key, self.getPtr(), Optional<double>(), stats));
			ASSERT(value.present());
			validate(self, { { key, value.get() } });
			ASSERT_EQ(stats->nHits, 1);
			ASSERT_EQ(stats->nDedups, 0);
			ASSERT_EQ(stats->nFetched, 0);
			updateWorkloadStats(self, stats);
		}
		TraceEvent("TestCacheHitsEnd");

		return Void();
	}

	ACTOR static Future<Void> testLoadKeys(Reference<LoadableCacheWorkload> self, int start, int count) {
		state std::vector<int64_t> ids;
		state Reference<LoadableCacheOpStats> stats = makeReference<LoadableCacheOpStats>();

		TraceEvent("TestLoadKeysStart");
		for (int i = 0; i < count && i < self->itemCount; i++) {
			ids.push_back(i + start);
		}

		KVMap values = wait(self->cache->get(ids, self.getPtr(), Optional<double>(), stats));
		ASSERT_EQ(values.size(), ids.size());
		validate(self, values);
		ASSERT_EQ(stats->nHits, 0);
		ASSERT_EQ(stats->nDedups, 0);
		ASSERT_EQ(stats->nFetched, ids.size());
		updateWorkloadStats(self, stats);
		TraceEvent("TestLoadKeysEnd");

		return Void();
	}

	ACTOR static Future<Void> testLoadKeysWithInvalidKeys(Reference<LoadableCacheWorkload> self, int start, int count) {
		state std::vector<int64_t> ids;
		state Reference<LoadableCacheOpStats> stats = makeReference<LoadableCacheOpStats>();

		TraceEvent("TestLoadKeysWithInvalidStart");
		for (int i = 0; i < count && i < self->itemCount; i++) {
			ids.push_back(i + start);
			// append invalid keys as well
			ids.push_back(self->itemCount + 1 + i);
		}

		std::unordered_map<int64_t, int64_t, Int64Hash, Int64Equal> values =
		    wait(self->cache->get(ids, self.getPtr(), Optional<double>(), stats));
		ASSERT_EQ(values.size(), ids.size() / 2);
		validate(self, values);
		ASSERT_EQ(stats->nHits, count);
		ASSERT_EQ(stats->nDedups, 0);
		ASSERT_EQ(stats->nFetched, 0);
		updateWorkloadStats(self, stats);
		TraceEvent("TestLoadKeysWithInvalidEnd");

		return Void();
	}
};

} // namespace

void forceLinkLoadableCacheTests() {}

TEST_CASE("/loadableCache") {
	Reference<LoadableCacheWorkload> workload = makeReference<LoadableCacheWorkload>();
	wait(workload->run(workload));
	return Void();
}
