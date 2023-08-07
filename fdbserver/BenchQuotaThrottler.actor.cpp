/**
 * BenchQuotaThrottler.actor.cpp
 */

#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbclient/Knobs.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/QuotaThrottler.h"
#include "benchmark/benchmark.h"
#include "flow/actorcompiler.h" // must be last include

static uint64_t getThroughput() {
	return SERVER_KNOBS->MIN_TAG_READ_PAGES_RATE * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
}

static ThrottlingId getThrottlingId(int index) {
	return ThrottlingIdRef::fromTag(BinaryWriter::toValue(index, Unversioned()).withPrefix("tag/"_sr));
}

static MockRKMetricsTracker getMetricsTracker(int numStorageServers, int busyThrottlingIdsPerSS) {
	MockRKMetricsTracker result;
	for (int i = 0; i < numStorageServers; ++i) {
		auto ssInfo = StorageQueueInfo::createTestInfo(
		    UID(i, i), /*storageQueueBytes=*/SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES);
		for (int j = 0; j < busyThrottlingIdsPerSS; ++j) {
			ssInfo.busiestReaders.emplace_back(getThrottlingId(j), getThroughput());
			ssInfo.busiestWriters.emplace_back(getThrottlingId(j), getThroughput());
		}
		result.updateStorageQueueInfo(ssInfo);
	}
	return result;
}

ACTOR static Future<Void> simulateWorkload(QuotaThrottler* quotaThrottler,
                                           RKServerThroughputTracker* serverThroughputTracker,
                                           MockRKMetricsTracker const* metricsTracker,
                                           int numThrottlingIds) {
	state int i = 0;
	while (i < 10) {
		for (int j = 0; j < numThrottlingIds; ++j) {
			quotaThrottler->addRequests(getThrottlingId(j), 1000);
		}
		serverThroughputTracker->update(metricsTracker->getStorageQueueInfo());
		wait(delay(0.1));
		++i;
	}
	return Void();
}

static MockRKThroughputQuotaCache getQuotaCache(int numThrottlingIds) {
	MockRKThroughputQuotaCache result;
	for (int i = 0; i < numThrottlingIds; ++i) {
		result.setQuota(getThrottlingId(i), /*totalQuota=*/CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, /*reservedQuota=*/0);
	}
	return result;
}

ACTOR static Future<Void> benchQuotaThrottlerActor(benchmark::State* benchState) {
	state int const numStorageServers = benchState->range(0);
	state int const numThrottlingIds = benchState->range(1);
	state int const busyThrottlingIdsPerSS = benchState->range(2);
	state MockRKMetricsTracker const metricsTracker = getMetricsTracker(numStorageServers, busyThrottlingIdsPerSS);
	state RKServerThroughputTracker serverThroughputTracker;
	state QuotaThrottler quotaThrottler(
	    UID{}, SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND, SERVER_KNOBS->GLOBAL_TAG_THROTTLING_LIMITING_THRESHOLD);
	wait(simulateWorkload(&quotaThrottler, &serverThroughputTracker, &metricsTracker, numThrottlingIds));
	MockRKThroughputQuotaCache quotaCache = getQuotaCache(numThrottlingIds);
	while (benchState->KeepRunning()) {
		auto const rates = quotaThrottler.getTargetRates(
		    /*numProxies=*/1, quotaCache, metricsTracker, serverThroughputTracker, serverThroughputTracker);
		ASSERT_GT(rates.size(), 0);
	}
	benchState->SetItemsProcessed(static_cast<long>(benchState->iterations()));
	return Void();
}

static void bench_quota_throttler(benchmark::State& benchState) {
	onMainThread([&benchState] { return benchQuotaThrottlerActor(&benchState); }).blockUntilReady();
}

static void bench_server_throughput_tracker(benchmark::State& benchState) {
	int const numStorageServers = benchState.range(0);
	int const busyThrottlingIdsPerSS = benchState.range(1);
	auto const metricsTracker = getMetricsTracker(numStorageServers, busyThrottlingIdsPerSS);
	RKServerThroughputTracker serverThroughputTracker;
	for (auto _ : benchState) {
		serverThroughputTracker.update(metricsTracker.getStorageQueueInfo());
		ASSERT_GT(serverThroughputTracker.storageServersTracked(), 0);
	}
	benchState.SetItemsProcessed(static_cast<long>(benchState.iterations()));
}

BENCHMARK(bench_quota_throttler)->Ranges({ { 1, 1000 }, { 1, 100 }, { 1, 5 } })->ReportAggregatesOnly(true);

BENCHMARK(bench_server_throughput_tracker)->Ranges({ { 1, 1000 }, { 1, 10 } })->ReportAggregatesOnly(true);
