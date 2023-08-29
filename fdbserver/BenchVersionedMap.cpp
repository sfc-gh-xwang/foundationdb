#include "benchmark/benchmark.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/VersionedMap.h"
#include "fdbclient/RandomKeyValueUtils.h"
#include "fdbserver/Knobs.h"
#include "flow/FastAlloc.h"
#include "flow/IRandom.h"
#include <vector>

static void bench_ptree_allocation(benchmark::State& benchState) {
	for (auto _ : benchState) {
		auto* p = new PTreeImpl::PTree<int>(1, 1);
		benchmark::DoNotOptimize(p);
		delete p;
	}
	benchState.counters["allocated"] = static_cast<double>(TrackedFastAllocated<PTreeImpl::PTree<int>>::allocated);

	ASSERT_EQ(TrackedFastAllocated<PTreeImpl::PTree<int>>::allocated,
	          TrackedFastAllocated<PTreeImpl::PTree<int>>::released);
}
BENCHMARK(bench_ptree_allocation);

// StorageServer::VersionedData
typedef VersionedMap<KeyRef, ValueOrClearToRef> VersionedData;

static void bench_vm_insert(benchmark::State& benchState) {
	int64_t numVersions = benchState.range(0) * SERVER_KNOBS->VERSIONS_PER_SECOND;
	uint64_t numKeys = benchState.range(1);

	ValueOrClearToRef value = ValueOrClearToRef::value("fixed_value"_sr);

	RandomKeySetGenerator keyGen(numKeys, "1..5/a..z");
	for (auto _ : benchState) {
		VersionedData vm;

		for (auto v = 0; v < numVersions; v++) {
			vm.createNewVersion(v);
			for (const auto& k : keyGen.keys) {
				vm.insert(k, value);
			}
		}
		benchState.counters["size"] =
		    static_cast<double>(VersionedData::PTreeSize::allocated - VersionedData::PTreeSize::released);
	}

	ASSERT_EQ(VersionedData::PTreeSize::allocated, VersionedData::PTreeSize::released);

	benchState.SetItemsProcessed(numVersions * numKeys * benchState.iterations());
}

BENCHMARK(bench_vm_insert)
    ->ArgsProduct({ benchmark::CreateDenseRange(1, 5, 1), benchmark::CreateRange(1, 64, 4) })
    ->ArgNames({ "seconds", "numKeys" });
static void bench_vm_bound(benchmark::State& benchState) {
	bool isLowerBound = benchState.range(0);
	int64_t dataSize = benchState.range(1);

	VersionedData data;
	ValueOrClearToRef value = ValueOrClearToRef::value("fixed_value"_sr);
	// only insert half of the keys, so when we bench search (lower_bound/upper_bound), it could test the cases that
	// the key exist and non-exist
	RandomKeySetGenerator keyGen(dataSize * 2, "10..20/a..z");
	int64_t i = 0;
	for (auto& key : keyGen.keys) {
		if (i++ % 2) {
			data.insert(key, value);
		}
	}

	for (auto _ : benchState) {
		auto it = isLowerBound ? data.at(data.getLatestVersion()).lower_bound(keyGen.next())
		                       : data.at(data.getLatestVersion()).upper_bound(keyGen.next());
		benchmark::DoNotOptimize(it);
	}

	benchState.SetItemsProcessed(benchState.iterations());
}

static void versioned_map_bound_arguments(benchmark::internal::Benchmark* b) {
	for (bool isLowerBound : { false, true }) {
		for (int64_t dataSize : { 1e3, 1e5 }) {
			b->Args({ isLowerBound, dataSize });
		}
	}
	b->ArgNames({ "isLowerBound", "dataSize" });
}

BENCHMARK(bench_vm_bound)->Iterations(1e6)->Apply(versioned_map_bound_arguments);
