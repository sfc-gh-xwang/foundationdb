#include "benchmark/benchmark.h"

#include "fdbserver/GrvProxyTagThrottler.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"

static void bench_grvProxyTagThrottler(benchmark::State& state) {
	GrvProxyTagThrottler throttler(/*maxThrottleDuration=*/5.0, /*starvationToleranceRatio=*/0.5);
	int numReqs = state.range(0);

	std::vector<GetReadVersionRequest> reqs;
	Arena arena;

	for (int i = 0; i < numReqs; ++i) {
		auto& req = reqs.emplace_back();

		req.priority = deterministicRandom()->coinflip() ? TransactionPriority::DEFAULT : TransactionPriority::LOW;
		req.transactionCount = 1;
		req.throttlingId = ThrottlingIdRef::fromTag(deterministicRandom()->randomAlphaNumeric(10));
	}

	for (auto _ : state) {
		state.PauseTiming();
		for (const auto& req : reqs) {
			// FIXME: For now this does nothing
			throttler.tryAddRequest(req);
		}

		Deque<GetReadVersionRequest> outBatchPriority;
		Deque<GetReadVersionRequest> outDefaultPriority;

		state.ResumeTiming();

		throttler.releaseTransactions(/*elapsed=*/0.01, outBatchPriority, outDefaultPriority);
	}

	state.SetItemsProcessed(numReqs * static_cast<long>(state.iterations()));
}

BENCHMARK(bench_grvProxyTagThrottler)->RangeMultiplier(10)->Range(1, 100000);
