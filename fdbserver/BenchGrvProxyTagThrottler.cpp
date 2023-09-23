#include "benchmark/benchmark.h"

#include "fdbserver/GrvProxyTagThrottler.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"

static ThrottlingId getThrottlingId(int index) {
	return ThrottlingIdRef::fromTag(BinaryWriter::toValue(index, Unversioned()));
}

static std::vector<GetReadVersionRequest> getRequests(int numReqs, int numDistinctTags) {
	std::vector<GetReadVersionRequest> result;
	for (int i = 0; i < numReqs; ++i) {
		auto& req = result.emplace_back();
		req.priority = deterministicRandom()->coinflip() ? TransactionPriority::DEFAULT : TransactionPriority::LOW;
		req.transactionCount = 1;
		req.expectedTotalCost = 1;
		req.throttlingId = getThrottlingId(i % numDistinctTags);
	}
	deterministicRandom()->randomShuffle(result);
	return result;
}

static void setRates(GrvProxyTagThrottler& throttler, int numDistinctTags) {
	ThrottlingIdMap<double> rates;
	for (int i = 0; i < numDistinctTags; ++i) {
		// Exact rate doesn't matter here:
		rates[getThrottlingId(i)] = 10.0;
	}
	throttler.updateRates(rates);
}

static void clearRates(GrvProxyTagThrottler& throttler) {
	throttler.updateRates(ThrottlingIdMap<double>{});
}

static void bench_grvProxyTagThrottler(benchmark::State& state) {
	int const numReqs = state.range(0);
	int const numDistinctTags = state.range(1);
	GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	auto const reqs = getRequests(numReqs, numDistinctTags);

	for (auto _ : state) {
		state.PauseTiming();
		// Set rates so that requests are queued:
		setRates(throttler, numDistinctTags);
		for (const auto& req : reqs) {
			ASSERT(throttler.tryAddRequest(req));
		}
		// Clear rates so that all requests are released in the first release window:
		clearRates(throttler);

		Deque<GetReadVersionRequest> outBatchPriority;
		Deque<GetReadVersionRequest> outDefaultPriority;

		state.ResumeTiming();

		// Elapsed time doesn't matter here, since all requests will be released:
		throttler.releaseTransactions(/*elapsed=*/0.01, outBatchPriority, outDefaultPriority);
	}

	state.SetItemsProcessed(numReqs * static_cast<long>(state.iterations()));
}

BENCHMARK(bench_grvProxyTagThrottler)
    ->ArgsProduct({ benchmark::CreateRange(1, 100000, 10), benchmark::CreateRange(1, 100, 10) });
