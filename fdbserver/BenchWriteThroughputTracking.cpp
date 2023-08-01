#include "benchmark/benchmark.h"
#include "fdbserver/IRKMetricsTracker.h"
#include "fdbserver/Knobs.h"
#include "fdbclient/Knobs.h"
#include "fdbserver/RatekeeperInterface.h"
#include "flow/IRandom.h"

static void bench_refreshCommitCost(benchmark::State& benchState) {
	const int numTags = benchState.range(0);

	VectorRef<StringRef> tags;
	Arena arena;
	for (int i = 0; i < numTags; ++i) {
		tags.push_back(arena, StringRef(arena, deterministicRandom()->randomAlphaNumeric(10)));
	}

	auto ssInfo = StorageQueueInfo::createTestInfo(
	    UID{}, /*storageQueueBytes=*/SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES);

	for (auto _ : benchState) {
		benchState.PauseTiming();
		for (const auto& tag : tags) {
			ssInfo.addCommitCost(tag,
			                     TransactionCommitCostEstimation{
			                         .opsSum = 1,
			                         .costSum = static_cast<uint64_t>(SERVER_KNOBS->MIN_TAG_WRITE_PAGES_RATE *
			                                                          CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE) });
		}
		benchState.ResumeTiming();

		benchmark::DoNotOptimize(ssInfo.refreshCommitCost(1));
	}
	benchState.SetItemsProcessed(static_cast<long>(benchState.iterations()) * numTags);
}

BENCHMARK(bench_refreshCommitCost)->Range(1, 131072);
