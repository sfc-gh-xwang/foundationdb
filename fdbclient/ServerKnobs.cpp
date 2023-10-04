/*
 * ServerKnobs.cpp
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

#include "fdbclient/ServerKnobs.h"
#include "flow/CompressionUtils.h"
#include "flow/IRandom.h"
#include "flow/flow.h"

#define init(...) KNOB_FN(__VA_ARGS__, INIT_ATOMIC_KNOB, INIT_KNOB)(__VA_ARGS__)

ServerKnobs::ServerKnobs(Randomize randomize, ClientKnobs* clientKnobs, IsSimulated isSimulated) {
	initialize(randomize, clientKnobs, isSimulated);
}

void ServerKnobs::initialize(Randomize randomize, ClientKnobs* clientKnobs, IsSimulated isSimulated) {
	// clang-format off
	init( ALLOW_DANGEROUS_KNOBS,                               isSimulated );
	// Versions
	init( VERSIONS_PER_SECOND,                                   1e6 );
	init( MAX_VERSIONS_IN_FLIGHT,                100 * VERSIONS_PER_SECOND );
	init( MAX_VERSIONS_IN_FLIGHT_FORCED,         6e5 * VERSIONS_PER_SECOND ); //one week of versions
	init( ENABLE_VERSION_VECTOR,                               false );
	init( ENABLE_VERSION_VECTOR_TLOG_UNICAST,                  false );

	bool buggifyShortReadWindow = randomize && BUGGIFY && !ENABLE_VERSION_VECTOR;
	init( MAX_READ_TRANSACTION_LIFE_VERSIONS,      5 * VERSIONS_PER_SECOND ); if (BUGGIFY_KNOB(MAX_READ_TRANSACTION_LIFE_VERSIONS, randomize && BUGGIFY, VERSIONS_PER_SECOND)) {} else if (BUGGIFY_KNOB(MAX_READ_TRANSACTION_LIFE_VERSIONS, buggifyShortReadWindow, std::max<int>(1, 0.1 * VERSIONS_PER_SECOND))) {} else BUGGIFY_KNOB(MAX_READ_TRANSACTION_LIFE_VERSIONS, randomize && BUGGIFY, 10 * VERSIONS_PER_SECOND);
	init( MAX_WRITE_TRANSACTION_LIFE_VERSIONS,     5 * VERSIONS_PER_SECOND ); BUGGIFY_KNOB(MAX_WRITE_TRANSACTION_LIFE_VERSIONS, randomize && BUGGIFY, std::max<int>(1, 1 * VERSIONS_PER_SECOND));
	init( MAX_COMMIT_BATCH_INTERVAL,                             2.0 ); BUGGIFY_KNOB(MAX_COMMIT_BATCH_INTERVAL, randomize && BUGGIFY, 0.5); // Each commit proxy generates a CommitTransactionBatchRequest at least this often, so that versions always advance smoothly
	MAX_COMMIT_BATCH_INTERVAL = std::min(MAX_COMMIT_BATCH_INTERVAL, MAX_READ_TRANSACTION_LIFE_VERSIONS/double(2*VERSIONS_PER_SECOND)); // Ensure that the proxy commits 2 times every MAX_READ_TRANSACTION_LIFE_VERSIONS, otherwise the master will not give out versions fast enough
	MAX_COMMIT_BATCH_INTERVAL = std::min(MAX_COMMIT_BATCH_INTERVAL, MAX_WRITE_TRANSACTION_LIFE_VERSIONS/double(2*VERSIONS_PER_SECOND)); // Ensure that the proxy commits 2 times every MAX_WRITE_TRANSACTION_LIFE_VERSIONS, otherwise the master will not give out versions fast enough
	init( MAX_VERSION_RATE_MODIFIER,                             0.1 );
	init( MAX_VERSION_RATE_OFFSET,               VERSIONS_PER_SECOND ); // If the calculated version is more than this amount away from the expected version, it will be clamped to this value. This prevents huge version jumps.
	init( ENABLE_VERSION_VECTOR_HA_OPTIMIZATION,               false );
	init( MAX_RECOVERY_VERSION_EPOCH_CORRECTION_SECONDS,       86400 );

	// TLogs
	init( TLOG_TIMEOUT,                                          0.4 ); //cannot buggify because of availability
	init( TLOG_SLOW_REJOIN_WARN_TIMEOUT_SECS,                     60 ); BUGGIFY_KNOB(TLOG_SLOW_REJOIN_WARN_TIMEOUT_SECS, randomize && BUGGIFY, deterministicRandom()->randomInt(5,10));
	init( TLOG_STORAGE_MIN_UPDATE_INTERVAL,                      0.5 );
	init( BUGGIFY_TLOG_STORAGE_MIN_UPDATE_INTERVAL,               30 );
	init( DESIRED_TOTAL_BYTES,                                150000 ); BUGGIFY_KNOB(DESIRED_TOTAL_BYTES, randomize && BUGGIFY, 10000);
	init( DESIRED_UPDATE_BYTES,                2*DESIRED_TOTAL_BYTES );
	init( UPDATE_DELAY,                                        0.001 );
	init( MAXIMUM_PEEK_BYTES,                                   10e6 );
	init( APPLY_MUTATION_BYTES,                                  1e6 );
	init( BUGGIFY_RECOVER_MEMORY_LIMIT,                          1e6 );
	init( BUGGIFY_WORKER_REMOVED_MAX_LAG,                         30 );
	init( UPDATE_STORAGE_BYTE_LIMIT,                             1e6 );
	init( TLOG_PEEK_DELAY,                                    0.0005 );
	init( LEGACY_TLOG_UPGRADE_ENTRIES_PER_VERSION,               100 );
	init( VERSION_MESSAGES_OVERHEAD_FACTOR_1024THS,             1072 ); // Based on a naive interpretation of the gcc version of std::deque, we would expect this to be 16 bytes overhead per 512 bytes data. In practice, it seems to be 24 bytes overhead per 512.
	init( VERSION_MESSAGES_ENTRY_BYTES_WITH_OVERHEAD, std::ceil(16.0 * VERSION_MESSAGES_OVERHEAD_FACTOR_1024THS / 1024) );
	init( LOG_SYSTEM_PUSHED_DATA_BLOCK_SIZE,                     1e5 );
	init( MAX_MESSAGE_SIZE,            std::max<int>(LOG_SYSTEM_PUSHED_DATA_BLOCK_SIZE, 1e5 + 2e4 + 1) + 8 ); // VALUE_SIZE_LIMIT + SYSTEM_KEY_SIZE_LIMIT + 9 bytes (4 bytes for length, 4 bytes for sequence number, and 1 byte for mutation type)
	init( TLOG_MESSAGE_BLOCK_BYTES,                             10e6 );
	init( TLOG_MESSAGE_BLOCK_OVERHEAD_FACTOR,      double(TLOG_MESSAGE_BLOCK_BYTES) / (TLOG_MESSAGE_BLOCK_BYTES - MAX_MESSAGE_SIZE) ); //1.0121466709838096006362758832473
	init( PEEK_TRACKER_EXPIRATION_TIME,                          600 ); BUGGIFY_KNOB(PEEK_TRACKER_EXPIRATION_TIME, randomize && BUGGIFY, 120); // Cannot be buggified lower without changing the following assert in LogSystemPeekCursor.actor.cpp: ASSERT_WE_THINK(e.code() == error_code_operation_obsolete || SERVER_KNOBS->PEEK_TRACKER_EXPIRATION_TIME < 10));
	init( PEEK_USING_STREAMING,                                true ); BUGGIFY_KNOB(PEEK_USING_STREAMING, randomize && isSimulated && BUGGIFY, false);
	init( PARALLEL_GET_MORE_REQUESTS,                             32 ); BUGGIFY_KNOB(PARALLEL_GET_MORE_REQUESTS, randomize && BUGGIFY, 2);
	init( MULTI_CURSOR_PRE_FETCH_LIMIT,                           10 );
	init( MAX_QUEUE_COMMIT_BYTES,                               15e6 ); BUGGIFY_KNOB(MAX_QUEUE_COMMIT_BYTES, randomize && BUGGIFY, 5000);
	init( DESIRED_OUTSTANDING_MESSAGES,                         5000 ); BUGGIFY_KNOB(DESIRED_OUTSTANDING_MESSAGES, randomize && BUGGIFY, deterministicRandom()->randomInt(0,100));
	init( DESIRED_GET_MORE_DELAY,                              0.005 );
	init( CONCURRENT_LOG_ROUTER_READS,                             5 ); BUGGIFY_KNOB(CONCURRENT_LOG_ROUTER_READS, randomize && BUGGIFY, 1);
	init( LOG_ROUTER_PEEK_FROM_SATELLITES_PREFERRED,               1 ); BUGGIFY_KNOB(LOG_ROUTER_PEEK_FROM_SATELLITES_PREFERRED, randomize && BUGGIFY, 0);
	init( LOG_ROUTER_PEEK_SWITCH_DC_TIME,                       60.0 );
	init( DISK_QUEUE_ADAPTER_MIN_SWITCH_TIME,                    1.0 );
	init( DISK_QUEUE_ADAPTER_MAX_SWITCH_TIME,                    5.0 );
	init( TLOG_SPILL_REFERENCE_MAX_PEEK_MEMORY_BYTES,            2e9 ); BUGGIFY_KNOB(TLOG_SPILL_REFERENCE_MAX_PEEK_MEMORY_BYTES, randomize && BUGGIFY, 2e6);
	init( TLOG_SPILL_REFERENCE_MAX_BATCHES_PER_PEEK,             100 ); BUGGIFY_KNOB(TLOG_SPILL_REFERENCE_MAX_BATCHES_PER_PEEK, randomize && BUGGIFY, 1);
	init( TLOG_SPILL_REFERENCE_MAX_BYTES_PER_BATCH,           16<<10 ); BUGGIFY_KNOB(TLOG_SPILL_REFERENCE_MAX_BYTES_PER_BATCH, randomize && BUGGIFY, 500);
	init( DISK_QUEUE_FILE_EXTENSION_BYTES,                    10<<20 ); // BUGGIFYd per file within the DiskQueue
	init( DISK_QUEUE_FILE_SHRINK_BYTES,                      100<<20 ); // BUGGIFYd per file within the DiskQueue
	init( DISK_QUEUE_MAX_TRUNCATE_BYTES,                     2LL<<30 ); BUGGIFY_KNOB(DISK_QUEUE_MAX_TRUNCATE_BYTES, randomize && BUGGIFY, 0);
	init( TLOG_DEGRADED_DURATION,                                5.0 );
	init( MAX_CACHE_VERSIONS,                                   10e6 );
	init( TLOG_IGNORE_POP_AUTO_ENABLE_DELAY,                   300.0 );
	init( TXS_POPPED_MAX_DELAY,                                  1.0 ); BUGGIFY_KNOB(TXS_POPPED_MAX_DELAY, randomize && BUGGIFY, deterministicRandom()->random01());
	// In some rare simulation tests, particularly with log_spill:=1 configured, the 10 second limit is exceeded, causing SevError trace events
	// and simulation test failure. Increasing the knob value to 15.0 in simulation is a workaround to avoid these failures.
	init( TLOG_MAX_CREATE_DURATION,                             10.0 ); if (isSimulated) TLOG_MAX_CREATE_DURATION = 15.0;
	init( PEEK_LOGGING_AMOUNT,                                     5 );
	init( PEEK_LOGGING_DELAY,                                    5.0 );
	init( PEEK_RESET_INTERVAL,                                 300.0 ); BUGGIFY_KNOB(PEEK_RESET_INTERVAL, randomize && BUGGIFY, 20.0);
	init( PEEK_MAX_LATENCY,                                      0.5 ); BUGGIFY_KNOB(PEEK_MAX_LATENCY, randomize && BUGGIFY, 0.0);
	init( PEEK_COUNT_SMALL_MESSAGES,                           false ); BUGGIFY_KNOB(PEEK_COUNT_SMALL_MESSAGES, randomize && BUGGIFY, true);
	init( PEEK_STATS_INTERVAL,                                  10.0 );
	init( PEEK_STATS_SLOW_AMOUNT,                                  2 );
	init( PEEK_STATS_SLOW_RATIO,                                 0.5 );
	// Buggified value must be larger than the amount of simulated time taken by snapshots, to prevent repeatedly failing
	// snapshots due to closed commit proxy connections
	init( PUSH_RESET_INTERVAL,                                 300.0 ); BUGGIFY_KNOB(PUSH_RESET_INTERVAL, randomize && BUGGIFY, 40.0);
	init( PUSH_MAX_LATENCY,                                      0.5 ); BUGGIFY_KNOB(PUSH_MAX_LATENCY, randomize && BUGGIFY, 0.0);
	init( PUSH_STATS_INTERVAL,                                  10.0 );
	init( PUSH_STATS_SLOW_AMOUNT,                                  2 );
	init( PUSH_STATS_SLOW_RATIO,                                 0.5 );
	init( TLOG_POP_BATCH_SIZE,                                  1000 ); BUGGIFY_KNOB(TLOG_POP_BATCH_SIZE, randomize && BUGGIFY, 10);
	init( TLOG_POPPED_VER_LAG_THRESHOLD_FOR_TLOGPOP_TRACE,     250e6 );
	init( BLOCKING_PEEK_TIMEOUT,                                 0.4 );
	init( ENABLE_DETAILED_TLOG_POP_TRACE,                      false ); BUGGIFY_KNOB(ENABLE_DETAILED_TLOG_POP_TRACE, randomize && BUGGIFY, true);
	init( POP_FROM_LOG_DELAY,                                      1 ); BUGGIFY_KNOB(POP_FROM_LOG_DELAY, randomize && BUGGIFY, 0);
	init( TLOG_PULL_ASYNC_DATA_WARNING_TIMEOUT_SECS,             120 );

	// Dynamic Empty Version Wait
	init( PEEK_BATCHING_EMPTY_MSG,                             false ); BUGGIFY_KNOB(PEEK_BATCHING_EMPTY_MSG, randomize && BUGGIFY, true);
	init( PEEK_BATCHING_EMPTY_MSG_INTERVAL,                    0.001 ); BUGGIFY_KNOB(PEEK_BATCHING_EMPTY_MSG_INTERVAL, randomize && BUGGIFY, 0.01);
	init( DYNAMIC_EMPTY_VERSION_WAIT,    EMPTY_VERSION_WAIT_DISABLED ); BUGGIFY_KNOB(DYNAMIC_EMPTY_VERSION_WAIT, randomize && BUGGIFY, EMPTY_VERSION_WAIT);
	// Dynamic Empty Version Wait: Sampling
	init( DYNAMIC_EMPTY_VERSION_WAIT_SAMPLE_PROB,                  1 ); 
	init( DYNAMIC_EMPTY_VERSION_WAIT_QUEUE_MAX_LENGTH,         10000 ); 
	init( DYNAMIC_EMPTY_VERSION_WAIT_LESS_SKEW_STAT,            true ); 
	// Dynamic Empty Version Wait: Decision
	init( DYNAMIC_EMPTY_VERSION_WAIT_SACRIFICE_PERCENTILE,         0 );
	init( DYNAMIC_EMPTY_VERSION_WAIT_MUTATION_THRESHOLD,         0.1 );
	init( DYNAMIC_EMPTY_VERSION_WAIT_MUTATION_SMOOTHING_FACTOR, 0.95 );
	init( DYNAMIC_EMPTY_VERSION_WAIT_CAP,                      0.020 );
	// Dynamic Empty Version Wait: Syncing
	init( DYNAMIC_EMPTY_VERSION_WAIT_EMERGENCY_DEVIATION,        0.2 );
	init( DYNAMIC_EMPTY_VERSION_WAIT_SYNC_INTERVAL,                1 );
	init( DYNAMIC_EMPTY_VERSION_WAIT_SYNC_SAMPLE_REQUIREMENT,   2000 );

	// disk snapshot max timeout, to be put in TLog, storage and coordinator nodes
	init( MAX_FORKED_PROCESS_OUTPUT,                            1024 );
	init( SNAP_CREATE_MAX_TIMEOUT,        isSimulated ? 70.0 : 300.0 );
	init( SNAP_MINIMUM_TIME_GAP,                                 5.0 );
	init( SNAP_NETWORK_FAILURE_RETRY_LIMIT,                       10 );
	init( MAX_STORAGE_SNAPSHOT_FAULT_TOLERANCE,                    1 );
	init( MAX_COORDINATOR_SNAPSHOT_FAULT_TOLERANCE,                1 );
	init( SNAPSHOT_ALL_STATEFUL_PROCESSES,                     false ); BUGGIFY_KNOB(SNAPSHOT_ALL_STATEFUL_PROCESSES, randomize && BUGGIFY, true);

	// Data distribution queue
	init( HEALTH_POLL_TIME,                                      1.0 );
	init( BEST_TEAM_STUCK_DELAY,                                 1.0 );
	init( DEST_OVERLOADED_DELAY,                                 0.2 );
	init( BG_REBALANCE_POLLING_INTERVAL,                        10.0 );
	init( BG_REBALANCE_SWITCH_CHECK_INTERVAL,                    5.0 ); BUGGIFY_KNOB(BG_REBALANCE_SWITCH_CHECK_INTERVAL, randomize && BUGGIFY, 1.0);
	init( DD_QUEUE_LOGGING_INTERVAL,                             5.0 );
	init( DD_QUEUE_COUNTER_REFRESH_INTERVAL,                    60.0 );
	// 100 / 60 < 2 trace/sec ~ 2 * 200 = 400b/sec
	init( DD_QUEUE_COUNTER_MAX_LOG,                              100 ); BUGGIFY_KNOB(DD_QUEUE_COUNTER_MAX_LOG, randomize && BUGGIFY, 1);
	init( DD_QUEUE_COUNTER_SUMMARIZE,                           true );
	init( RELOCATION_PARALLELISM_PER_SOURCE_SERVER,                2 ); BUGGIFY_KNOB(RELOCATION_PARALLELISM_PER_SOURCE_SERVER, randomize && BUGGIFY, 1);
	init( RELOCATION_PARALLELISM_PER_DEST_SERVER,                 10 ); BUGGIFY_KNOB(RELOCATION_PARALLELISM_PER_DEST_SERVER, randomize && BUGGIFY, 1); // Note: if this is smaller than FETCH_KEYS_PARALLELISM, this will artificially reduce performance. The current default of 10 is probably too high but is set conservatively for now.
	init( DD_QUEUE_MAX_KEY_SERVERS,                              100 ); // Do not buggify
	init( DD_REBALANCE_PARALLELISM,                               50 );
	init( DD_REBALANCE_RESET_AMOUNT,                              30 );
	init( INFLIGHT_PENALTY_HEALTHY,                              1.0 );
	init( INFLIGHT_PENALTY_UNHEALTHY,                          500.0 );
	init( INFLIGHT_PENALTY_ONE_LEFT,                          1000.0 );
	init( USE_OLD_NEEDED_SERVERS,                              false );

	init( PRIORITY_RECOVER_MOVE,                                 110 );
	init( PRIORITY_REBALANCE_UNDERUTILIZED_TEAM,                 120 );
	init( PRIORITY_REBALANCE_READ_UNDERUTIL_TEAM,                121 );
	init( PRIORITY_REBALANCE_OVERUTILIZED_TEAM,                  122 );
	init( PRIORITY_REBALANCE_READ_OVERUTIL_TEAM,                 123 );
	init( PRIORITY_PERPETUAL_STORAGE_WIGGLE,                     139 );
	init( PRIORITY_TEAM_HEALTHY,                                 140 );
	init( PRIORITY_TEAM_CONTAINS_UNDESIRED_SERVER,               150 );
	init( PRIORITY_TEAM_REDUNDANT,                               200 );
	init( PRIORITY_MERGE_SHARD,                                  340 );
	init( PRIORITY_POPULATE_REGION,                              600 );
	init( PRIORITY_TEAM_UNHEALTHY,                               700 );
	init( PRIORITY_TEAM_2_LEFT,                                  709 );
	init( PRIORITY_TEAM_1_LEFT,                                  800 );
	init( PRIORITY_TEAM_FAILED,                                  805 );
	init( PRIORITY_TEAM_0_LEFT,                                  809 );
	init( PRIORITY_SPLIT_SHARD,                                  950 ); BUGGIFY_KNOB(PRIORITY_SPLIT_SHARD, randomize && BUGGIFY, 350);
	init( PRIORITY_ENFORCE_MOVE_OUT_OF_PHYSICAL_SHARD,           960 ); BUGGIFY_KNOB(PRIORITY_ENFORCE_MOVE_OUT_OF_PHYSICAL_SHARD, randomize && BUGGIFY, 360); // Set as the lowest priority

	// Data distribution
	init( LOAD_BYTES_PIVOT_RATIO,                        	     0.5 );
	init( CPU_PIVOT_RATIO,                                       0.9 );
	// In order to make sure GetTeam has enough eligible destination team:
	ASSERT_GT(LOAD_BYTES_PIVOT_RATIO + CPU_PIVOT_RATIO, 1.0 );
	// For dd re-evaluatioin
	bool buggifyDdLoadBytesPivotRatio = randomize && BUGGIFY;
	init( DD_LOWER_LOAD_BYTES_PIVOT_RATIO,                      0.4 ); BUGGIFY_KNOB(DD_LOWER_LOAD_BYTES_PIVOT_RATIO, buggifyDdLoadBytesPivotRatio, deterministicRandom()->random01() * 0.4 + 0.3);
	init( DD_UPPER_LOAD_BYTES_PIVOT_RATIO,                      0.6 ); BUGGIFY_KNOB(DD_UPPER_LOAD_BYTES_PIVOT_RATIO, buggifyDdLoadBytesPivotRatio, DD_LOWER_LOAD_BYTES_PIVOT_RATIO + (0.9 - DD_LOWER_LOAD_BYTES_PIVOT_RATIO) * deterministicRandom()->random01());
	ASSERT_LE(DD_LOWER_LOAD_BYTES_PIVOT_RATIO, DD_UPPER_LOAD_BYTES_PIVOT_RATIO);

	bool buggifyDdCpuPivotRatio = randomize && BUGGIFY;
	init( DD_LOWER_CPU_PIVOT_RATIO,                             0.7 ); BUGGIFY_KNOB(DD_LOWER_CPU_PIVOT_RATIO, buggifyDdCpuPivotRatio, deterministicRandom()->random01() * 0.4 + 0.5);
	init( DD_UPPER_CPU_PIVOT_RATIO,                             0.9 ); BUGGIFY_KNOB(DD_UPPER_CPU_PIVOT_RATIO, buggifyDdCpuPivotRatio, DD_LOWER_CPU_PIVOT_RATIO + (0.999 - DD_LOWER_CPU_PIVOT_RATIO) * deterministicRandom()->random01());
	ASSERT_LE(DD_LOWER_CPU_PIVOT_RATIO, DD_UPPER_CPU_PIVOT_RATIO);

	bool buggifyDdWritePivotRatio = randomize && BUGGIFY;
	init( DD_LOWER_WRITE_PIVOT_RATIO,                           0.7 ); BUGGIFY_KNOB(DD_LOWER_WRITE_PIVOT_RATIO, buggifyDdWritePivotRatio, deterministicRandom()->random01() * 0.4 + 0.5);
	init( DD_UPPER_WRITE_PIVOT_RATIO,                           0.9 ); BUGGIFY_KNOB(DD_UPPER_WRITE_PIVOT_RATIO, buggifyDdWritePivotRatio, DD_LOWER_WRITE_PIVOT_RATIO + (0.999 - DD_LOWER_WRITE_PIVOT_RATIO) * deterministicRandom()->random01());
	ASSERT_LE(DD_LOWER_WRITE_PIVOT_RATIO, DD_UPPER_WRITE_PIVOT_RATIO);
	init( DD_REEVALUATION_ENABLED,                            false ); BUGGIFY_KNOB(DD_REEVALUATION_ENABLED, isSimulated, deterministicRandom()->coinflip());

	// In simulation, the CPU percent of every storage server is hard-coded as 100.0%. It is difficult to test pivot CPU in normal simulation. TODO: add mock DD Test case for it.
	// TODO: choose a meaning value for real cluster
	init( MAX_DEST_CPU_PERCENT,                                 100.0);
	init( DD_TEAM_PIVOT_UPDATE_DELAY,                            5.0 );

	init( SHARD_ENCODE_LOCATION_METADATA,                      false );
	init( ENABLE_DD_PHYSICAL_SHARD,                            false ); // EXPERIMENTAL; If true, SHARD_ENCODE_LOCATION_METADATA must be true; When true, optimization of data move between DCs is disabled
	init( ENABLE_DD_PHYSICAL_SHARD_MOVE,                       false );
	init( MAX_PHYSICAL_SHARD_BYTES,                         10000000 ); // 10 MB; for ENABLE_DD_PHYSICAL_SHARD; smaller leads to larger number of physicalShard per storage server
 	init( PHYSICAL_SHARD_METRICS_DELAY,                        300.0 ); // 300 seconds; for ENABLE_DD_PHYSICAL_SHARD
	init( ANONYMOUS_PHYSICAL_SHARD_TRANSITION_TIME,            600.0 ); BUGGIFY_KNOB(ANONYMOUS_PHYSICAL_SHARD_TRANSITION_TIME, randomize && BUGGIFY, 0.0); // 600 seconds for ENABLE_DD_PHYSICAL_SHARD
	init( PHYSICAL_SHARD_MOVE_VERBOSE_TRACKING,                false );
	init( READ_REBALANCE_CPU_THRESHOLD,                         15.0 );
	init( READ_REBALANCE_SRC_PARALLELISM,                         20 );
	init( READ_REBALANCE_SHARD_TOPK, READ_REBALANCE_SRC_PARALLELISM * 2 );
	init( READ_REBALANCE_DIFF_FRAC,                               0.3);
	init( READ_REBALANCE_MAX_SHARD_FRAC,                          0.2); // FIXME: add buggify here when we have DD test, seems DD is pretty sensitive to this parameter

	// TODO: now we set it to a large number  so that the shard average traffic can guard this change. Consider change it to a lower value in the future.
	init( READ_REBALANCE_MIN_READ_BYTES_KS, std::numeric_limits<double>::max() );
	init( RETRY_RELOCATESHARD_DELAY,                             0.1 );
	init( DATA_DISTRIBUTION_FAILURE_REACTION_TIME,              60.0 ); BUGGIFY_KNOB(DATA_DISTRIBUTION_FAILURE_REACTION_TIME, randomize && BUGGIFY, 1.0);
	bool buggifySmallShards = randomize && BUGGIFY;
	bool simulationMediumShards = !buggifySmallShards && isSimulated && randomize && deterministicRandom()->random01() < 0.8; // prefer smaller shards in simulation
	// FIXME: increase this even more eventually
	init( MIN_SHARD_BYTES,                                  10000000 ); BUGGIFY_KNOB(MIN_SHARD_BYTES, buggifySmallShards, 40000); BUGGIFY_KNOB(MIN_SHARD_BYTES, simulationMediumShards, 200000); //FIXME: data distribution tracker (specifically StorageMetrics) relies on this number being larger than the maximum size of a key value pair
	init( SHARD_BYTES_RATIO,                                       4 );
	init( SHARD_BYTES_PER_SQRT_BYTES,                             45 ); BUGGIFY_KNOB(SHARD_BYTES_PER_SQRT_BYTES, buggifySmallShards, 0);//Approximately 10000 bytes per shard
	init( MAX_SHARD_BYTES,                                 500000000 );
	init( KEY_SERVER_SHARD_BYTES,                          500000000 );

	init( SHARD_MAX_READ_OPS_PER_KSEC,                  45000 * 1000 );
    init( SHARD_READ_OPS_CHANGE_THRESHOLD, SHARD_MAX_READ_OPS_PER_KSEC / 4); BUGGIFY_KNOB(SHARD_READ_OPS_CHANGE_THRESHOLD, randomize && BUGGIFY, 2000);
 	/*
 	 * The assumption is when the read ops reach to 45k/s the Storage Server instance will be CPU-saturated.
 	 */
	init( SHARD_MAX_READ_DENSITY_RATIO,                           8.0); BUGGIFY_KNOB(SHARD_MAX_READ_DENSITY_RATIO, randomize && BUGGIFY, 2.0);
	/*
		The bytesRead/byteSize radio. Will be declared as read hot when larger than this. 8.0 was chosen to avoid reporting table scan as read hot.
	*/
	init ( SHARD_READ_HOT_BANDWIDTH_MIN_PER_KSECONDS,      1666667 * 1000);
	/*
		The read bandwidth of a given shard needs to be larger than this value in order to be evaluated if it's read hot. The roughly 1.67MB per second is calculated as following:
			- Heuristic data suggests that each storage process can do max 500K read operations per second
			- Each read has a minimum cost of EMPTY_READ_PENALTY, which is 20 bytes
			- Thus that gives a minimum 10MB per second
			- But to be conservative, set that number to be 1/6 of 10MB, which is roughly 1,666,667 bytes per second
		Shard with a read bandwidth smaller than this value will never be too busy to handle the reads.
	*/
	init( SHARD_MAX_BYTES_READ_PER_KSEC_JITTER,     0.1 );
	bool buggifySmallBandwidthSplit = randomize && BUGGIFY;
	init( SHARD_MAX_BYTES_PER_KSEC,                 1LL*1000000*1000 ); BUGGIFY_KNOB(SHARD_MAX_BYTES_PER_KSEC, buggifySmallBandwidthSplit, (int64_t)10LL*1000*1000);
	/* 1*1MB/sec * 1000sec/ksec
		Shards with more than this bandwidth will be split immediately.
		For a large shard (100MB), it will be split into multiple shards with sizes < SHARD_SPLIT_BYTES_PER_KSEC;
		all but one split shard will be moved; so splitting may cost ~100MB of work or about 10MB/sec over a 10 sec sampling window.
		If the sampling window is too much longer, the MVCC window will fill up while we wait.
		If SHARD_MAX_BYTES_PER_KSEC is too much lower, we could do a lot of data movement work in response to a small impulse of bandwidth.
		If SHARD_MAX_BYTES_PER_KSEC is too high relative to the I/O bandwidth of a given server, a workload can remain concentrated on a single
		team indefinitely, limiting performance.
		*/

	init( SHARD_MIN_BYTES_PER_KSEC,                100 * 1000 * 1000 ); BUGGIFY_KNOB(SHARD_MIN_BYTES_PER_KSEC, buggifySmallBandwidthSplit, 200*1*1000);
	/* 100*1KB/sec * 1000sec/ksec
		Shards with more than this bandwidth will not be merged.
		Obviously this needs to be significantly less than SHARD_MAX_BYTES_PER_KSEC, else we will repeatedly merge and split.
		It should probably be significantly less than SHARD_SPLIT_BYTES_PER_KSEC, else we will merge right after splitting.

		The number of extra shards in the database because of bandwidth splitting can't be more than about W/SHARD_MIN_BYTES_PER_KSEC, where
		W is the maximum bandwidth of the entire database in bytes/ksec.  For 250MB/sec write bandwidth, (250MB/sec)/(200KB/sec) = 1250 extra
		shards.

		The bandwidth sample maintained by the storage server needs to be accurate enough to reliably measure this minimum bandwidth.  See
		BYTES_WRITTEN_UNITS_PER_SAMPLE.  If this number is too low, the storage server needs to spend more memory and time on sampling.
		*/

	init( SHARD_SPLIT_BYTES_PER_KSEC,              250 * 1000 * 1000 ); BUGGIFY_KNOB(SHARD_SPLIT_BYTES_PER_KSEC, buggifySmallBandwidthSplit, 50 * 1000 * 1000);
	/* 250*1KB/sec * 1000sec/ksec
		When splitting a shard, it is split into pieces with less than this bandwidth.
		Obviously this should be less than half of SHARD_MAX_BYTES_PER_KSEC.

		Smaller values mean that high bandwidth shards are split into more pieces, more quickly utilizing large numbers of servers to handle the
		bandwidth.

		Too many pieces (too small a value) may stress data movement mechanisms (see e.g. RELOCATION_PARALLELISM_PER_SOURCE_SERVER).

		If this value is too small relative to SHARD_MIN_BYTES_PER_KSEC immediate merging work will be generated.
		*/

	init( STORAGE_METRIC_TIMEOUT,         isSimulated ? 60.0 : 600.0 ); BUGGIFY_KNOB(STORAGE_METRIC_TIMEOUT, randomize && BUGGIFY, deterministicRandom()->coinflip() ? 10.0 : 30.0);
	init( METRIC_DELAY,                                          0.1 ); BUGGIFY_KNOB(METRIC_DELAY, randomize && BUGGIFY, 1.0);
	init( ALL_DATA_REMOVED_DELAY,                                1.0 );
	init( INITIAL_FAILURE_REACTION_DELAY,                       30.0 ); BUGGIFY_KNOB(INITIAL_FAILURE_REACTION_DELAY, randomize && BUGGIFY, 0.0);
	init( CHECK_TEAM_DELAY,                                     30.0 );
	// This is a safety knob to avoid busy spinning and the case a small cluster don't have enough space when excluding and including too fast. The basic idea is let PW wait for the re-included storage to take on data before wiggling the next one.
	// This knob's ideal value would vary by cluster based on its size and disk type. In the meanwhile, the wiggle will also wait until the storage load is almost (85%) balanced.
	init( PERPETUAL_WIGGLE_DELAY,                isSimulated? 10: 60 );
	init( PERPETUAL_WIGGLE_SMALL_LOAD_RATIO,                      10 );
	init( PERPETUAL_WIGGLE_MIN_BYTES_BALANCE_RATIO,             0.85 );
	init( PERPETUAL_WIGGLE_DISABLE_REMOVER,                     true );
	init( LOG_ON_COMPLETION_DELAY,         DD_QUEUE_LOGGING_INTERVAL );
	init( BEST_TEAM_MAX_TEAM_TRIES,                               10 );
	init( BEST_TEAM_OPTION_COUNT,                                  4 );
	init( BEST_OF_AMT,                                             4 );
	init( SERVER_LIST_DELAY,                                     1.0 );
	init( RECRUITMENT_IDLE_DELAY,                                1.0 );
	init( STORAGE_RECRUITMENT_DELAY,                            10.0 );
	init( TSS_HACK_IDENTITY_MAPPING,                           false ); // THIS SHOULD NEVER BE SET IN PROD. Only for performance testing
	init( TSS_RECRUITMENT_TIMEOUT,       3*STORAGE_RECRUITMENT_DELAY ); BUGGIFY_KNOB(TSS_RECRUITMENT_TIMEOUT, randomize && BUGGIFY, 1.0); // Super low timeout should cause tss recruitments to fail
	init( TSS_DD_CHECK_INTERVAL,                                60.0 ); BUGGIFY_KNOB(TSS_DD_CHECK_INTERVAL, randomize && BUGGIFY, 1.0);    // May kill all TSS quickly
	init( DATA_DISTRIBUTION_LOGGING_INTERVAL,                    5.0 );
	init( DD_ENABLED_CHECK_DELAY,                                1.0 );
	init( DD_STALL_CHECK_DELAY,                                  0.4 ); //Must be larger than 2*MAX_BUGGIFIED_DELAY
	init( DD_LOW_BANDWIDTH_DELAY,         isSimulated ? 15.0 : 240.0 ); BUGGIFY_KNOB(DD_LOW_BANDWIDTH_DELAY, randomize && BUGGIFY, 0); //Because of delayJitter, this should be less than 0.9 * DD_MERGE_COALESCE_DELAY
	init( DD_MERGE_COALESCE_DELAY,       isSimulated ?  30.0 : 300.0 ); BUGGIFY_KNOB(DD_MERGE_COALESCE_DELAY, randomize && BUGGIFY, 0.001);
	init( STORAGE_METRICS_POLLING_DELAY,                         2.0 ); BUGGIFY_KNOB(STORAGE_METRICS_POLLING_DELAY, randomize && BUGGIFY, 15.0);
	init( STORAGE_METRICS_RANDOM_DELAY,                          0.2 );
	init( AVAILABLE_SPACE_RATIO_CUTOFF,                         0.05 );
	init( DESIRED_TEAMS_PER_SERVER,                                5 ); BUGGIFY_KNOB(DESIRED_TEAMS_PER_SERVER, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 10));
	init( MAX_TEAMS_PER_SERVER,           5*DESIRED_TEAMS_PER_SERVER );
	init( DD_SHARD_SIZE_GRANULARITY,                         5000000 );
	init( DD_SHARD_SIZE_GRANULARITY_SIM,                      500000 ); BUGGIFY_KNOB(DD_SHARD_SIZE_GRANULARITY_SIM, randomize && BUGGIFY, 0);
	init( DD_MOVE_KEYS_PARALLELISM,                               15 ); BUGGIFY_KNOB(DD_MOVE_KEYS_PARALLELISM, randomize && BUGGIFY, 1);
	init( DD_FETCH_SOURCE_PARALLELISM,                          1000 ); BUGGIFY_KNOB(DD_FETCH_SOURCE_PARALLELISM, randomize && BUGGIFY, 1);
	init( DD_MERGE_LIMIT,                                       2000 ); BUGGIFY_KNOB(DD_MERGE_LIMIT, randomize && BUGGIFY, 2);
	init( DD_SHARD_METRICS_TIMEOUT,                             60.0 );
	init( DD_LOCATION_CACHE_SIZE,                            2000000 ); BUGGIFY_KNOB(DD_LOCATION_CACHE_SIZE, randomize && BUGGIFY, 3);
	init( MOVEKEYS_LOCK_POLLING_DELAY,                           5.0 );
	init( DEBOUNCE_RECRUITING_DELAY,                             5.0 );
	init( DD_FAILURE_TIME,                                       1.0 ); BUGGIFY_KNOB(DD_FAILURE_TIME, randomize && BUGGIFY, 10.0);
	init( DD_ZERO_HEALTHY_TEAM_DELAY,                            1.0 );
	init( REMOTE_KV_STORE,                                     false );
	init( REBOOT_KV_STORE_DELAY,                                 0.1 );
	init( REMOTE_KV_STORE_MAX_INIT_DURATION,                    10.0 );
	init( REBALANCE_MAX_RETRIES,                                 100 );
	init( DD_OVERLAP_PENALTY,                                  10000 );
	init( DD_EXCLUDE_MIN_REPLICAS,                                 1 );
	init( DD_VALIDATE_LOCALITY,                                 true ); BUGGIFY_KNOB(DD_VALIDATE_LOCALITY, randomize && BUGGIFY, false);
	init( DD_CHECK_INVALID_LOCALITY_DELAY,                       60  ); BUGGIFY_KNOB(DD_CHECK_INVALID_LOCALITY_DELAY, randomize && BUGGIFY, 1 + deterministicRandom()->random01() * 600);
	init( DD_ENABLE_VERBOSE_TRACING,                           false ); BUGGIFY_KNOB(DD_ENABLE_VERBOSE_TRACING, randomize && BUGGIFY, true);
	bool buggifyDdSsVersionLag = randomize && BUGGIFY;
	init( DD_SS_FAILURE_VERSIONLAG,                        250000000 ); BUGGIFY_KNOB(DD_SS_FAILURE_VERSIONLAG, buggifyDdSsVersionLag, deterministicRandom()->randomInt(15000000, 500000000));
	init( DD_SS_ALLOWED_VERSIONLAG,                        200000000 ); BUGGIFY_KNOB(DD_SS_ALLOWED_VERSIONLAG, buggifyDdSsVersionLag, 0.75 * DD_SS_FAILURE_VERSIONLAG);
	init( DD_SS_STUCK_TIME_LIMIT,                              300.0 ); BUGGIFY_KNOB(DD_SS_STUCK_TIME_LIMIT, randomize && BUGGIFY, 200.0 + deterministicRandom()->random01() * 100.0);
	init( DD_TEAMS_INFO_PRINT_INTERVAL,                           60 ); BUGGIFY_KNOB(DD_TEAMS_INFO_PRINT_INTERVAL, randomize && BUGGIFY, 10);
	init( DD_TEAMS_INFO_PRINT_YIELD_COUNT,                       100 ); BUGGIFY_KNOB(DD_TEAMS_INFO_PRINT_YIELD_COUNT, randomize && BUGGIFY, deterministicRandom()->random01() * 1000 + 1);
	init( DD_TEAM_ZERO_SERVER_LEFT_LOG_DELAY,                    120 ); BUGGIFY_KNOB(DD_TEAM_ZERO_SERVER_LEFT_LOG_DELAY, randomize && BUGGIFY, 5);
	init( DD_STORAGE_WIGGLE_PAUSE_THRESHOLD,                      10 ); BUGGIFY_KNOB(DD_STORAGE_WIGGLE_PAUSE_THRESHOLD, randomize && BUGGIFY, 1000);
	init( DD_STORAGE_WIGGLE_STUCK_THRESHOLD,                      20 );
	init( DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC,   isSimulated ? 2 : 21 * 60 * 60 * 24 ); BUGGIFY_KNOB(DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC, randomize && BUGGIFY, isSimulated ? 0: 120);
	init( DD_TENANT_AWARENESS_ENABLED,                         false );
	init( DD_BUILD_EXTRA_TEAMS_OVERRIDE,                          10 ); BUGGIFY_KNOB(DD_BUILD_EXTRA_TEAMS_OVERRIDE, randomize && BUGGIFY, 2);

	// Knobs for Storage Quota feature
	init( STORAGE_QUOTA_ENABLED,                                true ); BUGGIFY_KNOB(STORAGE_QUOTA_ENABLED, isSimulated, deterministicRandom()->coinflip());
	init( TENANT_CACHE_LIST_REFRESH_INTERVAL,                     30 ); if(isSimulated) TENANT_CACHE_LIST_REFRESH_INTERVAL = 5; BUGGIFY_KNOB(TENANT_CACHE_LIST_REFRESH_INTERVAL, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 10));
	init( TENANT_CACHE_STORAGE_USAGE_REFRESH_INTERVAL,           180 ); if(isSimulated) TENANT_CACHE_STORAGE_USAGE_REFRESH_INTERVAL = 10; BUGGIFY_KNOB(TENANT_CACHE_STORAGE_USAGE_REFRESH_INTERVAL, randomize && BUGGIFY, deterministicRandom()->randomInt(5, 15));
	init( TENANT_CACHE_STORAGE_QUOTA_REFRESH_INTERVAL,            30 ); if(isSimulated) TENANT_CACHE_STORAGE_QUOTA_REFRESH_INTERVAL = 5; BUGGIFY_KNOB(TENANT_CACHE_STORAGE_QUOTA_REFRESH_INTERVAL, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 10));
	init( TENANT_CACHE_STORAGE_USAGE_TRACE_INTERVAL,             300 );
	init( TC_STORAGE_USAGE_VARIABLE_POLLING_LAG_BYTES, 1000000000 ); if(isSimulated) TC_STORAGE_USAGE_VARIABLE_POLLING_LAG_BYTES = deterministicRandom()->randomInt(0, 10) * 1000;
	init( TC_STORAGE_USAGE_VARIABLE_POLLING_LAG_SECONDS,       0 ); if(isSimulated) TC_STORAGE_USAGE_VARIABLE_POLLING_LAG_SECONDS = deterministicRandom()->randomInt(0, 5);
	init( CP_FETCH_TENANTS_OVER_STORAGE_QUOTA_INTERVAL,            5 ); BUGGIFY_KNOB(CP_FETCH_TENANTS_OVER_STORAGE_QUOTA_INTERVAL, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 10));

	// Large teams are disabled when SHARD_ENCODE_LOCATION_METADATA is enabled
	init( DD_MAX_SHARDS_ON_LARGE_TEAMS,                          100 ); BUGGIFY_KNOB(DD_MAX_SHARDS_ON_LARGE_TEAMS, randomize && BUGGIFY, deterministicRandom()->randomInt(0, 3));
	init( DD_MAXIMUM_LARGE_TEAM_CLEANUP,                       10000 ); BUGGIFY_KNOB(DD_MAXIMUM_LARGE_TEAM_CLEANUP, randomize && BUGGIFY, 10);
	init( DD_LARGE_TEAM_DELAY,                                  60.0 );
	init( DD_FIX_WRONG_REPLICAS_DELAY,                          60.0 );

	// TeamRemover
	init( TR_FLAG_DISABLE_MACHINE_TEAM_REMOVER,                false ); BUGGIFY_KNOB(TR_FLAG_DISABLE_MACHINE_TEAM_REMOVER, randomize && BUGGIFY, deterministicRandom()->random01() < 0.1 ? true : false); // false by default. disable the consistency check when it's true
	init( TR_REMOVE_MACHINE_TEAM_DELAY,                         60.0 ); BUGGIFY_KNOB(TR_REMOVE_MACHINE_TEAM_DELAY, randomize && BUGGIFY,  deterministicRandom()->random01() * 60.0);
	init( TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS,                    true ); BUGGIFY_KNOB(TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS, randomize && BUGGIFY, deterministicRandom()->random01() < 0.1 ? true : false);
	init( TR_FLAG_DISABLE_SERVER_TEAM_REMOVER,                 false ); BUGGIFY_KNOB(TR_FLAG_DISABLE_SERVER_TEAM_REMOVER, randomize && BUGGIFY, deterministicRandom()->random01() < 0.1 ? true : false); // false by default. disable the consistency check when it's true
	init( TR_REMOVE_SERVER_TEAM_DELAY,                          60.0 ); BUGGIFY_KNOB(TR_REMOVE_SERVER_TEAM_DELAY, randomize && BUGGIFY,  std::max(0.1, deterministicRandom()->random01() * 60.0));
	init( TR_REMOVE_SERVER_TEAM_EXTRA_DELAY,                     5.0 ); BUGGIFY_KNOB(TR_REMOVE_SERVER_TEAM_EXTRA_DELAY, randomize && BUGGIFY,  deterministicRandom()->random01() * 10.0);

	init( DD_REMOVE_STORE_ENGINE_DELAY,                         60.0 ); BUGGIFY_KNOB(DD_REMOVE_STORE_ENGINE_DELAY, randomize && BUGGIFY,  deterministicRandom()->random01() * 60.0);

	// KeyValueStore SQLITE
	init( CLEAR_BUFFER_SIZE,                                   20000 );
	init( READ_VALUE_TIME_ESTIMATE,                           .00005 );
	init( READ_RANGE_TIME_ESTIMATE,                           .00005 );
	init( SET_TIME_ESTIMATE,                                  .00005 );
	init( CLEAR_TIME_ESTIMATE,                                .00005 );
	init( COMMIT_TIME_ESTIMATE,                                 .005 );
	init( CHECK_FREE_PAGE_AMOUNT,                                100 ); BUGGIFY_KNOB(CHECK_FREE_PAGE_AMOUNT, randomize && BUGGIFY, 5);
	init( SOFT_HEAP_LIMIT,                                     300e6 );

	bool buggifyWriteWindow = randomize && BUGGIFY;
	init( SQLITE_PAGE_SCAN_ERROR_LIMIT,                        10000 );
	init( SQLITE_BTREE_PAGE_USABLE,                          4096 - 8);  // pageSize - reserveSize for page checksum
	init( SQLITE_CHUNK_SIZE_PAGES,                             25600 );  // 100MB
	init( SQLITE_CHUNK_SIZE_PAGES_SIM,                          1024 );  // 4MB
	init( SQLITE_READER_THREADS,                                  64 );  // number of read threads
	init( SQLITE_WRITE_WINDOW_SECONDS,                            -1 ); BUGGIFY_KNOB(SQLITE_WRITE_WINDOW_SECONDS, buggifyWriteWindow, 0.01 + deterministicRandom()->random01());
	init( SQLITE_CURSOR_MAX_LIFETIME_BYTES,                      1e6 ); BUGGIFY_KNOB(SQLITE_CURSOR_MAX_LIFETIME_BYTES, buggifySmallShards || simulationMediumShards, MIN_SHARD_BYTES); BUGGIFY_KNOB(SQLITE_CURSOR_MAX_LIFETIME_BYTES, randomize && BUGGIFY, 0);
	init( SQLITE_WRITE_WINDOW_LIMIT,                              -1 ); BUGGIFY_KNOB(SQLITE_WRITE_WINDOW_LIMIT, buggifyWriteWindow, SQLITE_WRITE_WINDOW_SECONDS * deterministicRandom()->randomInt(1000, 5000)); // Set window limit to opsPerSecond scaled down to window size

	// Maximum and minimum cell payload bytes allowed on primary page as calculated in SQLite.
	// These formulas are copied from SQLite, using its hardcoded constants, so if you are
	// changing this you should also be changing SQLite.
	init( SQLITE_BTREE_CELL_MAX_LOCAL, (SQLITE_BTREE_PAGE_USABLE - 12) * 64/255 - 23 );
	init( SQLITE_BTREE_CELL_MIN_LOCAL, (SQLITE_BTREE_PAGE_USABLE - 12) * 32/255 - 23 );

	// Maximum FDB fragment key and value bytes that can fit in a primary btree page
	init( SQLITE_FRAGMENT_PRIMARY_PAGE_USABLE,
					SQLITE_BTREE_CELL_MAX_LOCAL
					 - 1 // vdbeRecord header length size
					 - 2 // max key length size
					 - 4 // max index length size
					 - 2 // max value fragment length size
	);

	// Maximum FDB fragment value bytes in an overflow page
	init( SQLITE_FRAGMENT_OVERFLOW_PAGE_USABLE,
					SQLITE_BTREE_PAGE_USABLE
					 - 4 // next pageNumber size
	);
	init( SQLITE_FRAGMENT_MIN_SAVINGS,                          0.20 );

	// KeyValueStoreSqlite spring cleaning
	init( SPRING_CLEANING_NO_ACTION_INTERVAL,                    1.0 ); BUGGIFY_KNOB(SPRING_CLEANING_NO_ACTION_INTERVAL, randomize && BUGGIFY, deterministicRandom()->coinflip() ? 0.1 : deterministicRandom()->random01() * 5);
	init( SPRING_CLEANING_LAZY_DELETE_INTERVAL,                  0.1 ); BUGGIFY_KNOB(SPRING_CLEANING_LAZY_DELETE_INTERVAL, randomize && BUGGIFY, deterministicRandom()->coinflip() ? 1.0 : deterministicRandom()->random01() * 5);
	init( SPRING_CLEANING_VACUUM_INTERVAL,                       1.0 ); BUGGIFY_KNOB(SPRING_CLEANING_VACUUM_INTERVAL, randomize && BUGGIFY, deterministicRandom()->coinflip() ? 0.1 : deterministicRandom()->random01() * 5);
	init( SPRING_CLEANING_LAZY_DELETE_TIME_ESTIMATE,            .010 ); BUGGIFY_KNOB(SPRING_CLEANING_LAZY_DELETE_TIME_ESTIMATE, randomize && BUGGIFY, deterministicRandom()->random01() * 5);
	init( SPRING_CLEANING_VACUUM_TIME_ESTIMATE,                 .010 ); BUGGIFY_KNOB(SPRING_CLEANING_VACUUM_TIME_ESTIMATE, randomize && BUGGIFY, deterministicRandom()->random01() * 5);
	init( SPRING_CLEANING_VACUUMS_PER_LAZY_DELETE_PAGE,          0.0 ); BUGGIFY_KNOB(SPRING_CLEANING_VACUUMS_PER_LAZY_DELETE_PAGE, randomize && BUGGIFY, deterministicRandom()->coinflip() ? 1e9 : deterministicRandom()->random01() * 5);
	init( SPRING_CLEANING_MIN_LAZY_DELETE_PAGES,                   0 ); BUGGIFY_KNOB(SPRING_CLEANING_MIN_LAZY_DELETE_PAGES, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 100));
	init( SPRING_CLEANING_MAX_LAZY_DELETE_PAGES,                 1e9 ); BUGGIFY_KNOB(SPRING_CLEANING_MAX_LAZY_DELETE_PAGES, randomize && BUGGIFY, deterministicRandom()->coinflip() ? 0 : deterministicRandom()->randomInt(1, 1e4));
	init( SPRING_CLEANING_LAZY_DELETE_BATCH_SIZE,                100 ); BUGGIFY_KNOB(SPRING_CLEANING_LAZY_DELETE_BATCH_SIZE, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 1000));
	init( SPRING_CLEANING_MIN_VACUUM_PAGES,                        1 ); BUGGIFY_KNOB(SPRING_CLEANING_MIN_VACUUM_PAGES, randomize && BUGGIFY, deterministicRandom()->randomInt(0, 100));
	init( SPRING_CLEANING_MAX_VACUUM_PAGES,                      1e9 ); BUGGIFY_KNOB(SPRING_CLEANING_MAX_VACUUM_PAGES, randomize && BUGGIFY, deterministicRandom()->coinflip() ? 0 : deterministicRandom()->randomInt(1, 1e4));

	// KeyValueStoreMemory
	init( REPLACE_CONTENTS_BYTES,                                1e5 );

	// KeyValueStoreRocksDB
	init( ROCKSDB_SET_READ_TIMEOUT,         		    !isSimulated );
	init( ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES,         true ); BUGGIFY_KNOB( ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES, randomize && BUGGIFY, false);
	init( ROCKSDB_SUGGEST_COMPACT_CLEAR_RANGE,                  true ); BUGGIFY_KNOB( ROCKSDB_SUGGEST_COMPACT_CLEAR_RANGE, randomize && BUGGIFY, false);
	init( ROCKSDB_READ_RANGE_ROW_LIMIT,                        65535 ); BUGGIFY_KNOB( ROCKSDB_READ_RANGE_ROW_LIMIT, randomize && BUGGIFY, deterministicRandom()->randomInt(2, 10));
	init( ROCKSDB_READER_THREAD_PRIORITY,                          0 );
	init( ROCKSDB_WRITER_THREAD_PRIORITY,                          0 );
	init( ROCKSDB_BACKGROUND_PARALLELISM,                          4 );
	init( ROCKSDB_READ_PARALLELISM,                                4 );
	init( ROCKSDB_CHECKPOINT_READER_PARALLELISM,                   4 );
	// If true, do not process and store RocksDB logs
	init( ROCKSDB_MUTE_LOGS,                                    true );
	// Use a smaller memtable in simulation to avoid OOMs.
	int64_t memtableBytes = isSimulated ? 1024 * 1024 : 512 * 1024 * 1024;
	init( ROCKSDB_MEMTABLE_BYTES,                      memtableBytes );
	init( ROCKSDB_LEVEL_STYLE_COMPACTION,                       true );
	init( ROCKSDB_UNSAFE_AUTO_FSYNC,                           false );
	init( ROCKSDB_PERIODIC_COMPACTION_SECONDS,                     0 );
	init( ROCKSDB_PREFIX_LEN,                                      0 );
	// If rocksdb block cache size is 0, the default 8MB is used.
	int64_t blockCacheSize = isSimulated ? 16 * 1024 * 1024 : 1024 * 1024 * 1024 /* 1GB */;
	init( ROCKSDB_BLOCK_CACHE_SIZE,                   blockCacheSize );
	init( ROCKSDB_METRICS_DELAY,                                60.0 );
	// ROCKSDB_READ_VALUE_TIMEOUT, ROCKSDB_READ_VALUE_PREFIX_TIMEOUT, ROCKSDB_READ_RANGE_TIMEOUT knobs:
	// In simulation, increasing the read operation timeouts to 5 minutes, as some of the tests have
	// very high load and single read thread cannot process all the load within the timeouts.
	init( ROCKSDB_READ_VALUE_TIMEOUT,                            5.0 ); if (isSimulated) ROCKSDB_READ_VALUE_TIMEOUT = 5 * 60;
	init( ROCKSDB_READ_VALUE_PREFIX_TIMEOUT,                     5.0 ); if (isSimulated) ROCKSDB_READ_VALUE_PREFIX_TIMEOUT = 5 * 60;
	init( ROCKSDB_READ_RANGE_TIMEOUT,                            5.0 ); if (isSimulated) ROCKSDB_READ_RANGE_TIMEOUT = 5 * 60;
	init( ROCKSDB_READ_CHECKPOINT_TIMEOUT,                      60.0 ); if (isSimulated) ROCKSDB_READ_CHECKPOINT_TIMEOUT = 5 * 60;
	init( ROCKSDB_CHECKPOINT_READ_AHEAD_SIZE,           2 << 20 ); // 2M
	init( ROCKSDB_READ_QUEUE_WAIT,                               1.0 );
	init( ROCKSDB_READ_QUEUE_HARD_MAX,                          1000 );
	init( ROCKSDB_READ_QUEUE_SOFT_MAX,                           500 );
	init( ROCKSDB_FETCH_QUEUE_HARD_MAX,                          100 );
	init( ROCKSDB_FETCH_QUEUE_SOFT_MAX,                           50 );
	init( ROCKSDB_HISTOGRAMS_SAMPLE_RATE,                      0.001 ); BUGGIFY_KNOB(ROCKSDB_HISTOGRAMS_SAMPLE_RATE, randomize && BUGGIFY, 0);
	init( ROCKSDB_READ_RANGE_ITERATOR_REFRESH_TIME,             30.0 ); BUGGIFY_KNOB(ROCKSDB_READ_RANGE_ITERATOR_REFRESH_TIME, randomize && BUGGIFY, 0.1);
	init( ROCKSDB_READ_RANGE_REUSE_ITERATORS,                   true ); BUGGIFY_KNOB(ROCKSDB_READ_RANGE_REUSE_ITERATORS, randomize && BUGGIFY, deterministicRandom()->coinflip());
	init( ROCKSDB_READ_RANGE_REUSE_BOUNDED_ITERATORS,          false ); BUGGIFY_KNOB(ROCKSDB_READ_RANGE_REUSE_BOUNDED_ITERATORS, randomize && BUGGIFY, deterministicRandom()->coinflip());
	init( ROCKSDB_READ_RANGE_BOUNDED_ITERATORS_MAX_LIMIT,        200 );
	// Set to 0 to disable rocksdb write rate limiting. Rate limiter unit: bytes per second.
	init( ROCKSDB_WRITE_RATE_LIMITER_BYTES_PER_SEC,                0 );
	// If true, enables dynamic adjustment of ROCKSDB_WRITE_RATE_LIMITER_BYTES according to the recent demand of background IO.
	init( ROCKSDB_WRITE_RATE_LIMITER_AUTO_TUNE,                 true );
	init( DEFAULT_FDB_ROCKSDB_COLUMN_FAMILY,                    "fdb");
	init( ROCKSDB_DISABLE_AUTO_COMPACTIONS,                    false ); // RocksDB default

	init( ROCKSDB_PERFCONTEXT_ENABLE,                          false ); BUGGIFY_KNOB(ROCKSDB_PERFCONTEXT_ENABLE, randomize && BUGGIFY, deterministicRandom()->coinflip());
	init( ROCKSDB_PERFCONTEXT_SAMPLE_RATE,                    0.0001 );
	init( ROCKSDB_METRICS_SAMPLE_INTERVAL,						  0.0);
	init( ROCKSDB_MAX_SUBCOMPACTIONS,                              2 );
	init( ROCKSDB_SOFT_PENDING_COMPACT_BYTES_LIMIT,      64000000000 ); // 64GB, Rocksdb option, Writes will slow down.
	init( ROCKSDB_HARD_PENDING_COMPACT_BYTES_LIMIT,     100000000000 ); // 100GB, Rocksdb option, Writes will stall.
	init( ROCKSDB_CAN_COMMIT_COMPACT_BYTES_LIMIT,        50000000000 ); // 50GB, Commit waits.
	// Enable this knob only for experminatal purpose, never enable this in production.
	// If enabled, all the committed in-memory memtable writes are lost on a crash.
	init( ROCKSDB_DISABLE_WAL_EXPERIMENTAL,                    false );
	// If ROCKSDB_SINGLEKEY_DELETES_ON_CLEARRANGE is enabled, disable ROCKSDB_ENABLE_CLEAR_RANGE_EAGER_READS knob.
	// These knobs have contrary functionality.
	init( ROCKSDB_SINGLEKEY_DELETES_ON_CLEARRANGE,             false ); BUGGIFY_KNOB(ROCKSDB_SINGLEKEY_DELETES_ON_CLEARRANGE, randomize && BUGGIFY, deterministicRandom()->coinflip());
	init( ROCKSDB_SINGLEKEY_DELETES_BYTES_LIMIT,              200000 ); // 200KB
	init( ROCKSDB_ENABLE_CLEAR_RANGE_EAGER_READS,               true ); BUGGIFY_KNOB(ROCKSDB_ENABLE_CLEAR_RANGE_EAGER_READS, randomize && BUGGIFY, deterministicRandom()->coinflip());
	// ROCKSDB_STATS_LEVEL=1 indicates rocksdb::StatsLevel::kExceptHistogramOrTimers
	// Refer StatsLevel: https://github.com/facebook/rocksdb/blob/main/include/rocksdb/statistics.h#L594
	init( ROCKSDB_STATS_LEVEL,                                     1 ); BUGGIFY_KNOB(ROCKSDB_STATS_LEVEL, randomize && BUGGIFY, deterministicRandom()->randomInt(0, 6));
	init( ROCKSDB_ENABLE_COMPACT_ON_DELETION,                   true );
	// CDCF: CompactOnDeletionCollectorFactory. The below 3 are parameters of the CompactOnDeletionCollectorFactory
	// which controls the compaction on deleted data.
	init( ROCKSDB_CDCF_SLIDING_WINDOW_SIZE,                      128 );
	init( ROCKSDB_CDCF_DELETION_TRIGGER,                           1 );
	init( ROCKSDB_CDCF_DELETION_RATIO,                             0 );
	// Can commit will delay ROCKSDB_CAN_COMMIT_DELAY_ON_OVERLOAD seconds for
	// ROCKSDB_CAN_COMMIT_DELAY_TIMES_ON_OVERLOAD times, if rocksdb overloaded.
	// Set ROCKSDB_CAN_COMMIT_DELAY_TIMES_ON_OVERLOAD to 0, to disable
	init( ROCKSDB_CAN_COMMIT_DELAY_ON_OVERLOAD,                    1 );
	init( ROCKSDB_CAN_COMMIT_DELAY_TIMES_ON_OVERLOAD,              5 );
	init( ROCKSDB_COMPACTION_READAHEAD_SIZE,                   32768 ); // 32 KB, performs bigger reads when doing compaction.
	init( ROCKSDB_BLOCK_SIZE,                                  32768 ); // 32 KB, size of the block in rocksdb cache.
 	init( ENABLE_SHARDED_ROCKSDB,                              false );
	init( ROCKSDB_WRITE_BUFFER_SIZE,                         1 << 30 ); // 1G
	init( ROCKSDB_CF_WRITE_BUFFER_SIZE,                     64 << 20 ); // 64M, RocksDB default.
	init( ROCKSDB_MAX_TOTAL_WAL_SIZE,                              0 ); // RocksDB default.
	init( ROCKSDB_MAX_BACKGROUND_JOBS,                             2 ); // RocksDB default.
	init( ROCKSDB_DELETE_OBSOLETE_FILE_PERIOD,                 21600 ); // 6h, RocksDB default.
	init( ROCKSDB_PHYSICAL_SHARD_CLEAN_UP_DELAY, isSimulated ? 10.0 : 300.0 ); // Delays shard clean up, must be larger than ROCKSDB_READ_VALUE_TIMEOUT to prevent reading deleted shard.
	init( ROCKSDB_EMPTY_RANGE_CHECK,              isSimulated ? true : false);
	init( ROCKSDB_CREATE_BYTES_SAMPLE_FILE_RETRY_MAX,             50 );

	// Leader election
	bool longLeaderElection = randomize && BUGGIFY;
	init( MAX_NOTIFICATIONS,                                  100000 );
	init( MIN_NOTIFICATIONS,                                     100 );
	init( NOTIFICATION_FULL_CLEAR_TIME,                      10000.0 );
	init( CANDIDATE_MIN_DELAY,                                  0.05 );
	init( CANDIDATE_MAX_DELAY,                                   1.0 );
	init( CANDIDATE_GROWTH_RATE,                                 1.2 );
	init( POLLING_FREQUENCY,                                     2.0 ); BUGGIFY_KNOB(POLLING_FREQUENCY, longLeaderElection, 8.0);
	init( HEARTBEAT_FREQUENCY,                                   0.5 ); BUGGIFY_KNOB(HEARTBEAT_FREQUENCY, longLeaderElection, 1.0);

	// Commit Proxy and GRV Proxy
	init( START_TRANSACTION_BATCH_INTERVAL_MIN,                 1e-6 );
	init( START_TRANSACTION_BATCH_INTERVAL_MAX,                0.010 );
	init( START_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION,     0.5 );
	init( START_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA,       0.1 );
	init( START_TRANSACTION_BATCH_QUEUE_CHECK_INTERVAL,        0.001 );
	init( START_TRANSACTION_MAX_TRANSACTIONS_TO_START,        100000 );
	init( START_TRANSACTION_MAX_BYTES_TO_START,            409600000 );
	init( START_TRANSACTION_MAX_REQUESTS_TO_START,             10000 );
	init( START_TRANSACTION_RATE_WINDOW,                         2.0 );
	init( TAG_THROTTLE_RATE_WINDOW,                              2.0 );
	init( START_TRANSACTION_MAX_EMPTY_QUEUE_BUDGET,             10.0 );
	init( TAG_THROTTLE_MAX_EMPTY_QUEUE_BUDGET,                  10.0 );
	init( TAG_THROTTLE_MAX_EMPTY_QUEUE_BYTES_BUDGET,           40960 );
	init( START_TRANSACTION_MAX_QUEUE_SIZE,                      1e6 ); BUGGIFY_KNOB(START_TRANSACTION_MAX_QUEUE_SIZE, randomize && BUGGIFY, 1000);
	init( KEY_LOCATION_MAX_QUEUE_SIZE,                           1e6 );
	init( TENANT_ID_REQUEST_MAX_QUEUE_SIZE,                      1e6 );
	init( BLOB_GRANULE_LOCATION_MAX_QUEUE_SIZE,                  1e5 ); BUGGIFY_KNOB(BLOB_GRANULE_LOCATION_MAX_QUEUE_SIZE, randomize && BUGGIFY, 100);
	init( BULK_LOADERS_MAX_QUEUE_SIZE,                           1e4 ); BUGGIFY_KNOB(BULK_LOADERS_MAX_QUEUE_SIZE, randomize && BUGGIFY, 100);
	init( COMMIT_PROXY_LIVENESS_TIMEOUT,                        20.0 );
	init( COMMIT_PROXY_MAX_LIVENESS_TIMEOUT,                   600.0 ); BUGGIFY_KNOB(COMMIT_PROXY_MAX_LIVENESS_TIMEOUT, randomize && BUGGIFY, 20.0);
	// Delete this after the release of the 8.0.1 client. This is a temporary server-side solution to ensure the tag throttler functions properly with the unpatched 8.0 client.
    init( TAG_THROTTLE_CONVERT_TENANT_GROUP_TO_TAG,             true ); BUGGIFY_KNOB(TAG_THROTTLE_CONVERT_TENANT_GROUP_TO_TAG, randomize && BUGGIFY, false);

	init( COMMIT_TRANSACTION_BATCH_INTERVAL_FROM_IDLE,         0.0005 ); BUGGIFY_KNOB(COMMIT_TRANSACTION_BATCH_INTERVAL_FROM_IDLE, randomize && BUGGIFY, 0.005);
	init( COMMIT_TRANSACTION_BATCH_INTERVAL_MIN,                0.001 ); BUGGIFY_KNOB(COMMIT_TRANSACTION_BATCH_INTERVAL_MIN, randomize && BUGGIFY, 0.1);
	init( COMMIT_TRANSACTION_BATCH_INTERVAL_MAX,                0.020 );
	init( COMMIT_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION,     0.1 );
	init( COMMIT_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA,       0.1 );
	init( COMMIT_TRANSACTION_BATCH_COUNT_MAX,                   32768 ); BUGGIFY_KNOB(COMMIT_TRANSACTION_BATCH_COUNT_MAX, randomize && BUGGIFY, 1000); // Do NOT increase this number beyond 32768, as CommitIds only budget 2 bytes for storing transaction id within each batch
	init( COMMIT_BATCHES_MEM_BYTES_HARD_LIMIT,              8LL << 30 ); BUGGIFY_KNOB(COMMIT_BATCHES_MEM_BYTES_HARD_LIMIT, randomize && BUGGIFY, deterministicRandom()->randomInt64(100LL << 20,  8LL << 30));
	init( COMMIT_BATCHES_MEM_FRACTION_OF_TOTAL,                   0.5 );
	init( COMMIT_BATCHES_MEM_TO_TOTAL_MEM_SCALE_FACTOR,           5.0 );
	init( COMMIT_TRIGGER_DELAY,                                  0.01 ); BUGGIFY_KNOB(COMMIT_TRIGGER_DELAY, randomize && BUGGIFY, deterministicRandom()->random01() * 4);

	// these settings disable batch bytes scaling.  Try COMMIT_TRANSACTION_BATCH_BYTES_MAX=1e6, COMMIT_TRANSACTION_BATCH_BYTES_SCALE_BASE=50000, COMMIT_TRANSACTION_BATCH_BYTES_SCALE_POWER=0.5?
	bool buggifyCommitTransactionBatchBytes = randomize && BUGGIFY;
	init( COMMIT_TRANSACTION_BATCH_BYTES_MIN,                  100000 ); BUGGIFY_KNOB(COMMIT_TRANSACTION_BATCH_BYTES_MIN, buggifyCommitTransactionBatchBytes, 1000000);
	init( COMMIT_TRANSACTION_BATCH_BYTES_MAX,                  100000 ); BUGGIFY_KNOB(COMMIT_TRANSACTION_BATCH_BYTES_MAX, buggifyCommitTransactionBatchBytes, 1000000);
	init( COMMIT_TRANSACTION_BATCH_BYTES_SCALE_BASE,           100000 );
	init( COMMIT_TRANSACTION_BATCH_BYTES_SCALE_POWER,             0.0 );

	init( RESOLVER_COALESCE_TIME,                                1.0 );
	init( BUGGIFIED_ROW_LIMIT,                  APPLY_MUTATION_BYTES ); BUGGIFY_KNOB(BUGGIFIED_ROW_LIMIT, randomize && BUGGIFY, deterministicRandom()->randomInt(3, 30));
	init( PROXY_SPIN_DELAY,                                     0.01 );
	init( UPDATE_REMOTE_LOG_VERSION_INTERVAL,                    2.0 );
	init( MAX_TXS_POP_VERSION_HISTORY,                           1e5 );
	init( MIN_CONFIRM_INTERVAL,                                 0.05 );

	bool shortRecoveryDuration = randomize && BUGGIFY;
	init( ENFORCED_MIN_RECOVERY_DURATION,                       0.085 ); BUGGIFY_KNOB(ENFORCED_MIN_RECOVERY_DURATION, shortRecoveryDuration, 0.01);
	init( REQUIRED_MIN_RECOVERY_DURATION,                       0.080 ); BUGGIFY_KNOB(REQUIRED_MIN_RECOVERY_DURATION, shortRecoveryDuration, 0.01);
	init( ALWAYS_CAUSAL_READ_RISKY,                              true );
	init( MAX_COMMIT_UPDATES,                                    2000 ); BUGGIFY_KNOB(MAX_COMMIT_UPDATES, randomize && BUGGIFY, 1);
	init( MAX_PROXY_COMPUTE,                                      2.0 );
	init( MAX_COMPUTE_PER_OPERATION,                              0.1 );
	init( MAX_COMPUTE_DURATION_LOG_CUTOFF,                       0.05 );
	init( PROXY_COMPUTE_BUCKETS,                                20000 );
	init( PROXY_COMPUTE_GROWTH_RATE,                             0.01 );
	init( TXN_STATE_SEND_AMOUNT,                                    4 );
	init( REPORT_TRANSACTION_COST_ESTIMATION_DELAY,               0.1 );
	// In simulation, we cannot efficiently simulate a very large workload, so using the
	// default CP_MIN_TAG_WRITE_PAGES_RATE value of 100 causes undercounting of write throughput
	// that is substantial. To work around this, set CP_MIN_TAG_WRITE_PAGES_RATE to 1 in simulation.
	init( CP_MIN_TAG_WRITE_PAGES_RATE,                             50 ); if( isSimulated ) CP_MIN_TAG_WRITE_PAGES_RATE = 1;
	init( CP_THROTTLE_TAGS_TRACKED,                                10 ); BUGGIFY_KNOB(CP_THROTTLE_TAGS_TRACKED, randomize && BUGGIFY, deterministicRandom()->randomInt(5, 15));
	init( PROXY_REJECT_BATCH_QUEUED_TOO_LONG,                    true );
	init( PROXY_USE_RESOLVER_PRIVATE_MUTATIONS,                 false ); BUGGIFY_KNOB(PROXY_USE_RESOLVER_PRIVATE_MUTATIONS, randomize && BUGGIFY && !ENABLE_VERSION_VECTOR_TLOG_UNICAST, deterministicRandom()->coinflip());

	init( BURSTINESS_METRICS_ENABLED  ,                         false );
	init( BURSTINESS_METRICS_LOG_INTERVAL,                        0.1 );

	init( RESET_MASTER_BATCHES,                                   200 );
	init( RESET_RESOLVER_BATCHES,                                 200 );
	init( RESET_MASTER_DELAY,                                   300.0 );
	init( RESET_RESOLVER_DELAY,                                 300.0 );

	init( GLOBAL_CONFIG_MIGRATE_TIMEOUT,                          5.0 );
	init( GLOBAL_CONFIG_REFRESH_INTERVAL,                         1.0 ); BUGGIFY_KNOB(GLOBAL_CONFIG_REFRESH_INTERVAL, randomize && BUGGIFY, 0.1);
	init( GLOBAL_CONFIG_REFRESH_TIMEOUT,                         10.0 ); BUGGIFY_KNOB(GLOBAL_CONFIG_REFRESH_TIMEOUT, randomize && BUGGIFY, 1.0);

	// Master Server
	// masterCommitter() in the master server will allow lower priority tasks (e.g. DataDistibution)
	//  by delay()ing for this amount of time between accepted batches of TransactionRequests.
	bool fastBalancing = randomize && BUGGIFY;
	init( COMMIT_SLEEP_TIME,								  0.0001 ); BUGGIFY_KNOB(COMMIT_SLEEP_TIME, randomize && BUGGIFY, 0);
	init( KEY_BYTES_PER_SAMPLE,                                  2e4 ); BUGGIFY_KNOB(KEY_BYTES_PER_SAMPLE, fastBalancing, 1e3);
	init( MIN_BALANCE_TIME,                                      0.2 );
	init( MIN_BALANCE_DIFFERENCE,                                1e6 ); BUGGIFY_KNOB(MIN_BALANCE_DIFFERENCE, fastBalancing, 1e4);
	init( SECONDS_BEFORE_NO_FAILURE_DELAY,                  8 * 3600 );
	init( MAX_TXS_SEND_MEMORY,                                   1e7 ); BUGGIFY_KNOB(MAX_TXS_SEND_MEMORY, randomize && BUGGIFY, 1e5);
	init( MAX_RECOVERY_VERSIONS,           200 * VERSIONS_PER_SECOND );
	init( MAX_RECOVERY_TIME,                                    20.0 ); BUGGIFY_KNOB(MAX_RECOVERY_TIME, randomize && BUGGIFY, 1.0);
	init( PROVISIONAL_START_DELAY,                               1.0 );
	init( PROVISIONAL_MAX_DELAY,                                60.0 );
	init( PROVISIONAL_DELAY_GROWTH,                              1.5 );
	init( SECONDS_BEFORE_RECRUIT_BACKUP_WORKER,                  4.0 ); BUGGIFY_KNOB(SECONDS_BEFORE_RECRUIT_BACKUP_WORKER, randomize && BUGGIFY, deterministicRandom()->random01() * 8);
	init( CC_INTERFACE_TIMEOUT,                                 10.0 ); BUGGIFY_KNOB(CC_INTERFACE_TIMEOUT, randomize && BUGGIFY, 0.0);
	init( FORCE_RECOVERY_SUSPEND_ENABLED,                       true );
	init( FORCE_RECOVERY_SUSPEND_TIME,                          30.0 );

	// Resolver
	init( SAMPLE_OFFSET_PER_KEY,                                 100 );
	init( SAMPLE_EXPIRATION_TIME,                                1.0 );
	init( SAMPLE_POLL_TIME,                                      0.1 );
	init( RESOLVER_STATE_MEMORY_LIMIT,                      40 * 1e6 ); if( randomize && BUGGIFY ) RESOLVER_STATE_MEMORY_LIMIT = 1e6;
	init( LAST_LIMITED_RATIO,                                    2.0 );

	// Backup Worker
	init( BACKUP_TIMEOUT,                                        0.4 );
	init( BACKUP_NOOP_POP_DELAY,                                 5.0 );
	init( BACKUP_FILE_BLOCK_BYTES,                       1024 * 1024 );
	init( BACKUP_LOCK_BYTES,                                     3e9 ); BUGGIFY_KNOB(BACKUP_LOCK_BYTES, randomize && BUGGIFY, deterministicRandom()->randomInt(1024, 4096) * 4096);
	init( BACKUP_UPLOAD_DELAY,                                  10.0 ); BUGGIFY_KNOB(BACKUP_UPLOAD_DELAY, randomize && BUGGIFY, deterministicRandom()->random01() * 60);
	init( USE_EMERGENCY_BACKUP_WORKER,                         false );

	//Cluster Controller
	init( CLUSTER_CONTROLLER_LOGGING_DELAY,                      5.0 );
	init( MASTER_FAILURE_REACTION_TIME,                          0.4 ); BUGGIFY_KNOB(MASTER_FAILURE_REACTION_TIME, randomize && BUGGIFY, 10.0);
	init( MASTER_FAILURE_SLOPE_DURING_RECOVERY,                  0.1 );
	init( WORKER_COORDINATION_PING_DELAY,                         60 );
	init( SIM_SHUTDOWN_TIMEOUT,                                   10 );
	init( SHUTDOWN_TIMEOUT,                                      600 ); BUGGIFY_KNOB(SHUTDOWN_TIMEOUT, randomize && BUGGIFY, 60.0);
	init( MASTER_SPIN_DELAY,                                     1.0 ); BUGGIFY_KNOB(MASTER_SPIN_DELAY, randomize && BUGGIFY, 10.0);
	init( CC_PRUNE_CLIENTS_INTERVAL,                            60.0 );
	init( CC_CHANGE_DELAY,                                       0.1 );
	init( CC_CLASS_DELAY,                                       0.01 );
	init( COLOCATE_STATELESS_PROCESSES,                        false ); BUGGIFY_KNOB(COLOCATE_STATELESS_PROCESSES, randomize && BUGGIFY, deterministicRandom()->random01() < 0.25);
	init( WAIT_FOR_GOOD_RECRUITMENT_DELAY,                       1.0 );
	init( WAIT_FOR_GOOD_REMOTE_RECRUITMENT_DELAY,                5.0 );
	init( ATTEMPT_RECRUITMENT_DELAY,                           0.035 );
	init( ATTEMPT_STORAGE_RECRUITMENT_DELAY,                     0.5 );
	init( WAIT_FOR_DISTRIBUTOR_JOIN_DELAY,                       1.0 );
	init( WAIT_FOR_RATEKEEPER_JOIN_DELAY,                        1.0 );
	init( WAIT_FOR_CONSISTENCYSCAN_JOIN_DELAY,                   1.0 );
	init( WAIT_FOR_BLOB_MANAGER_JOIN_DELAY,                      1.0 );
	init( WAIT_FOR_ENCRYPT_KEY_PROXY_JOIN_DELAY,                 1.0 );
	init( WAIT_FOR_TENANT_BALANCER_JOIN_DELAY,                   1.0 );
	init( WORKER_FAILURE_TIME,                                   1.0 ); BUGGIFY_KNOB(WORKER_FAILURE_TIME, randomize && BUGGIFY, 10.0);
	init( CHECK_OUTSTANDING_INTERVAL,                            0.5 ); BUGGIFY_KNOB(CHECK_OUTSTANDING_INTERVAL, randomize && BUGGIFY, 0.001);
	init( VERSION_LAG_METRIC_INTERVAL,                           0.5 ); BUGGIFY_KNOB(VERSION_LAG_METRIC_INTERVAL, randomize && BUGGIFY, 10.0);
	init( MAX_VERSION_DIFFERENCE,           20 * VERSIONS_PER_SECOND );
	init( INITIAL_UPDATE_CROSS_DC_INFO_DELAY,                    300 );
	init( CHECK_REMOTE_HEALTH_INTERVAL,                           60 );
	init( FORCE_RECOVERY_CHECK_DELAY,                            5.0 );
	init( RATEKEEPER_FAILURE_TIME,                               1.0 );
	init( CONSISTENCYSCAN_FAILURE_TIME,                          1.0 );
	init( BLOB_MANAGER_FAILURE_TIME,                             1.0 );
	init( BLOB_MIGRATOR_FAILURE_TIME,                            1.0 );
	init( REPLACE_INTERFACE_DELAY,                              60.0 );
	init( REPLACE_INTERFACE_CHECK_DELAY,                         5.0 );
	init( COORDINATOR_REGISTER_INTERVAL,                         5.0 );
	init( CLIENT_REGISTER_INTERVAL,                            600.0 );
	init( CC_ENABLE_WORKER_HEALTH_MONITOR,                     false );
	init( CC_WORKER_HEALTH_CHECKING_INTERVAL,                   60.0 );
	init( CC_DEGRADED_LINK_EXPIRATION_INTERVAL,                300.0 );
	init( CC_MIN_DEGRADATION_INTERVAL,                         120.0 );
	init( TENANT_BALANCER_FAILURE_TIME,                          1.0 );
	init( ENCRYPT_KEY_PROXY_FAILURE_TIME,                        0.1 ); BUGGIFY_KNOB(ENCRYPT_KEY_PROXY_FAILURE_TIME, isSimulated, 1.0 + deterministicRandom()->random01());
	init( CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE,                      3 );
	init( CC_MAX_EXCLUSION_DUE_TO_HEALTH,                          2 );
	init( CC_HEALTH_TRIGGER_RECOVERY,                          false );
	init( CC_TRACKING_HEALTH_RECOVERY_INTERVAL,               3600.0 );
	init( CC_MAX_HEALTH_RECOVERY_COUNT,                            5 );
	init( CC_HEALTH_TRIGGER_FAILOVER,                          false );
	init( CC_FAILOVER_DUE_TO_HEALTH_MIN_DEGRADATION,               5 );
	init( CC_FAILOVER_DUE_TO_HEALTH_MAX_DEGRADATION,              10 );
	init( CC_ENABLE_ENTIRE_SATELLITE_MONITORING,               false );
	init( CC_SATELLITE_DEGRADATION_MIN_COMPLAINER,                 3 );
	init( CC_SATELLITE_DEGRADATION_MIN_BAD_SERVER,                 3 );
	init( CC_ENABLE_REMOTE_LOG_ROUTER_MONITORING,               true );
	init( CC_THROTTLE_SINGLETON_RERECRUIT_INTERVAL,              0.5 );
	init( CC_ENABLE_TENANT_BALANCER,                            false); BUGGIFY_KNOB(CC_ENABLE_TENANT_BALANCER, isSimulated, deterministicRandom()->coinflip());

	init( INCOMPATIBLE_PEERS_LOGGING_INTERVAL,                   600 ); BUGGIFY_KNOB(INCOMPATIBLE_PEERS_LOGGING_INTERVAL, randomize && BUGGIFY, 60.0);
	init( EXPECTED_MASTER_FITNESS,            ProcessClass::UnsetFit );
	init( EXPECTED_TLOG_FITNESS,              ProcessClass::UnsetFit );
	init( EXPECTED_LOG_ROUTER_FITNESS,        ProcessClass::UnsetFit );
	init( EXPECTED_COMMIT_PROXY_FITNESS,      ProcessClass::UnsetFit );
	init( EXPECTED_GRV_PROXY_FITNESS,         ProcessClass::UnsetFit );
	init( EXPECTED_RESOLVER_FITNESS,          ProcessClass::UnsetFit );
	init( RECRUITMENT_TIMEOUT,                                   600 ); BUGGIFY_KNOB(RECRUITMENT_TIMEOUT, randomize && BUGGIFY, deterministicRandom()->coinflip() ? 60.0 : 1.0);

	init( POLICY_RATING_TESTS,                                   200 ); BUGGIFY_KNOB(POLICY_RATING_TESTS, randomize && BUGGIFY, 20);
	init( POLICY_GENERATIONS,                                    100 ); BUGGIFY_KNOB(POLICY_GENERATIONS, randomize && BUGGIFY, 10);
	init( DBINFO_SEND_AMOUNT,                                      5 );
	init( DBINFO_BATCH_DELAY,                                    0.1 );
	init( SINGLETON_RECRUIT_BME_DELAY,                          10.0 );

	//Move Keys
	init( SHARD_READY_DELAY,                                    0.25 );
	init( SERVER_READY_QUORUM_INTERVAL,                         std::min(1.0, std::min(MAX_READ_TRANSACTION_LIFE_VERSIONS, MAX_WRITE_TRANSACTION_LIFE_VERSIONS)/(5.0*VERSIONS_PER_SECOND)) );
	init( SERVER_READY_QUORUM_TIMEOUT,                          15.0 ); BUGGIFY_KNOB(SERVER_READY_QUORUM_TIMEOUT, randomize && BUGGIFY, 1.0);
	init( REMOVE_RETRY_DELAY,                                    1.0 );
	init( MOVE_KEYS_KRM_LIMIT,                                  2000 ); BUGGIFY_KNOB(MOVE_KEYS_KRM_LIMIT, randomize && BUGGIFY, 2);
	init( MOVE_KEYS_KRM_LIMIT_BYTES,                             1e5 ); BUGGIFY_KNOB(MOVE_KEYS_KRM_LIMIT_BYTES, randomize && BUGGIFY, 5e4); //This must be sufficiently larger than CLIENT_KNOBS->KEY_SIZE_LIMIT (fdbclient/Knobs.h) to ensure that at least two entries will be returned from an attempt to read a key range map
	init( MOVE_SHARD_KRM_ROW_LIMIT,                            20000 );
 	init( MOVE_SHARD_KRM_BYTE_LIMIT,                             1e6 );
	init( MAX_SKIP_TAGS,                                           1 ); //The TLogs require tags to be densely packed to be memory efficient, so be careful increasing this knob
	init( MAX_ADDED_SOURCES_MULTIPLIER,                          0.0 ); BUGGIFY_KNOB(MAX_ADDED_SOURCES_MULTIPLIER, randomize && BUGGIFY, 2.0);

	//FdbServer
	bool longReboots = randomize && BUGGIFY;
	init( MIN_REBOOT_TIME,                                       4.0 ); BUGGIFY_KNOB(MIN_REBOOT_TIME, longReboots, 10.0);
	init( MAX_REBOOT_TIME,                                       5.0 ); BUGGIFY_KNOB(MAX_REBOOT_TIME, longReboots, 20.0);
	init( LOG_DIRECTORY,                                          ".");  // Will be set to the command line flag.
	init( CONN_FILE,                                               "");  // Will be set to the command line flag.
	init( SERVER_MEM_LIMIT,                                8LL << 30 );
	init( SYSTEM_MONITOR_FREQUENCY,                              5.0 );

	//Ratekeeper
	bool slowRatekeeper = randomize && BUGGIFY;
	init( SMOOTHING_AMOUNT,                                      1.0 ); BUGGIFY_KNOB(SMOOTHING_AMOUNT, slowRatekeeper, 5.0);
	init( SLOW_SMOOTHING_AMOUNT,                                10.0 ); BUGGIFY_KNOB(SLOW_SMOOTHING_AMOUNT, slowRatekeeper, 50.0);
	init( METRIC_UPDATE_RATE,                                     .1 ); BUGGIFY_KNOB(METRIC_UPDATE_RATE, slowRatekeeper, 0.5);
	init( DETAILED_METRIC_UPDATE_RATE,                           5.0 );
	init( RATEKEEPER_DEFAULT_LIMIT,                              1e6 ); BUGGIFY_KNOB(RATEKEEPER_DEFAULT_LIMIT, randomize && BUGGIFY, 0);
	init( RATEKEEPER_LIMIT_REASON_SAMPLE_RATE,                   0.1 );
	init( RATEKEEPER_PRINT_LIMIT_REASON,                       false ); BUGGIFY_KNOB(RATEKEEPER_PRINT_LIMIT_REASON, randomize && BUGGIFY, true);
	init( RATEKEEPER_MIN_RATE,                                   0.0 );
	init( RATEKEEPER_MAX_RATE,                                   1e9 );
	init( RATEKEEPER_BATCH_MIN_RATE,                             0.0 );
	init( RATEKEEPER_BATCH_MAX_RATE,                             1e9 );

	bool smallStorageTarget = randomize && BUGGIFY;
	init( TARGET_BYTES_PER_STORAGE_SERVER,                    1000e6 ); BUGGIFY_KNOB(TARGET_BYTES_PER_STORAGE_SERVER, smallStorageTarget, 3000e3);
	init( SPRING_BYTES_STORAGE_SERVER,                         100e6 ); BUGGIFY_KNOB(SPRING_BYTES_STORAGE_SERVER, smallStorageTarget, 300e3);
	init( AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES,               800e6 ); BUGGIFY_KNOB(AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES, smallStorageTarget, 2500e3);
	init( AUTO_TAG_THROTTLE_SPRING_BYTES_STORAGE_SERVER,       200e6 ); BUGGIFY_KNOB(AUTO_TAG_THROTTLE_SPRING_BYTES_STORAGE_SERVER, smallStorageTarget, 500e3);
	init( TARGET_BYTES_PER_STORAGE_SERVER_BATCH,               750e6 ); BUGGIFY_KNOB(TARGET_BYTES_PER_STORAGE_SERVER_BATCH, smallStorageTarget, 1500e3);
	init( SPRING_BYTES_STORAGE_SERVER_BATCH,                   100e6 ); BUGGIFY_KNOB(SPRING_BYTES_STORAGE_SERVER_BATCH, smallStorageTarget, 150e3);
	init( STORAGE_HARD_LIMIT_BYTES,                           1500e6 ); BUGGIFY_KNOB(STORAGE_HARD_LIMIT_BYTES, smallStorageTarget, 4500e3);
	init( STORAGE_HARD_LIMIT_BYTES_OVERAGE,                   5000e3 ); BUGGIFY_KNOB(STORAGE_HARD_LIMIT_BYTES_OVERAGE, smallStorageTarget, 100e3); // byte+version overage ensures storage server makes enough progress on freeing up storage queue memory at hard limit by ensuring it advances desiredOldestVersion enough per commit cycle.
	init( STORAGE_HARD_LIMIT_BYTES_SPEED_UP_SIM, STORAGE_HARD_LIMIT_BYTES ); BUGGIFY_KNOB(STORAGE_HARD_LIMIT_BYTES_SPEED_UP_SIM, smallStorageTarget, STORAGE_HARD_LIMIT_BYTES_SPEED_UP_SIM * 10);
	init( STORAGE_HARD_LIMIT_BYTES_OVERAGE_SPEED_UP_SIM, STORAGE_HARD_LIMIT_BYTES_OVERAGE ); BUGGIFY_KNOB(STORAGE_HARD_LIMIT_BYTES_OVERAGE_SPEED_UP_SIM, smallStorageTarget, STORAGE_HARD_LIMIT_BYTES_OVERAGE_SPEED_UP_SIM * 10);
	init( STORAGE_HARD_LIMIT_VERSION_OVERAGE, VERSIONS_PER_SECOND / 4.0 );
	init( STORAGE_DURABILITY_LAG_HARD_MAX,                    2000e6 ); BUGGIFY_KNOB(STORAGE_DURABILITY_LAG_HARD_MAX, smallStorageTarget, 100e6);
	init( STORAGE_DURABILITY_LAG_SOFT_MAX,                     250e6 ); BUGGIFY_KNOB(STORAGE_DURABILITY_LAG_SOFT_MAX, smallStorageTarget, 10e6);
	init( STORAGE_INCLUDE_FEED_STORAGE_QUEUE,                   true ); BUGGIFY_KNOB(STORAGE_INCLUDE_FEED_STORAGE_QUEUE, randomize && BUGGIFY, false);
	init( STORAGE_QUEUE_USE_ALLOCATOR_SIZE,                    false ); BUGGIFY_KNOB(STORAGE_QUEUE_USE_ALLOCATOR_SIZE, randomize && BUGGIFY, true);
	init( STORAGE_SHARD_CONSISTENCY_CHECK_INTERVAL,              0.0 ); if ( isSimulated ) STORAGE_SHARD_CONSISTENCY_CHECK_INTERVAL = 5.0;

	//FIXME: Low priority reads are disabled by assigning very high knob values, reduce knobs for 7.0
	init( LOW_PRIORITY_STORAGE_QUEUE_BYTES,                    775e8 ); BUGGIFY_KNOB(LOW_PRIORITY_STORAGE_QUEUE_BYTES, smallStorageTarget, 1750e3);
	init( LOW_PRIORITY_DURABILITY_LAG,                         200e6 ); BUGGIFY_KNOB(LOW_PRIORITY_DURABILITY_LAG, smallStorageTarget, 15e6);

	bool smallTlogTarget = randomize && BUGGIFY;
	init( TARGET_BYTES_PER_TLOG,                              2400e6 ); BUGGIFY_KNOB(TARGET_BYTES_PER_TLOG, smallTlogTarget, 2000e3);
	init( SPRING_BYTES_TLOG,                                   400e6 ); BUGGIFY_KNOB(SPRING_BYTES_TLOG, smallTlogTarget, 200e3);
	init( TARGET_BYTES_PER_TLOG_BATCH,                        1400e6 ); BUGGIFY_KNOB(TARGET_BYTES_PER_TLOG_BATCH, smallTlogTarget, 1400e3);
	init( SPRING_BYTES_TLOG_BATCH,                             300e6 ); BUGGIFY_KNOB(SPRING_BYTES_TLOG_BATCH, smallTlogTarget, 150e3);
	init( TLOG_SPILL_THRESHOLD,                               1500e6 ); BUGGIFY_KNOB(TLOG_SPILL_THRESHOLD, smallTlogTarget, 1500e3); BUGGIFY_KNOB(TLOG_SPILL_THRESHOLD, randomize && BUGGIFY, 0);
	init( REFERENCE_SPILL_UPDATE_STORAGE_BYTE_LIMIT,            20e6 ); BUGGIFY_KNOB(REFERENCE_SPILL_UPDATE_STORAGE_BYTE_LIMIT, (randomize && BUGGIFY) || smallTlogTarget, 1e6);
	init( TLOG_HARD_LIMIT_BYTES,                              3000e6 ); BUGGIFY_KNOB(TLOG_HARD_LIMIT_BYTES, smallTlogTarget, 30e6);
	init( TLOG_RECOVER_MEMORY_LIMIT, TARGET_BYTES_PER_TLOG + SPRING_BYTES_TLOG );

	init( MAX_TRANSACTIONS_PER_BYTE,                            1000 );

	init( MIN_AVAILABLE_SPACE,                                   1e8 ); if( isSimulated ) MIN_AVAILABLE_SPACE /= 10;
	init( MIN_AVAILABLE_SPACE_RATIO,                            0.05 ); if( isSimulated ) MIN_AVAILABLE_SPACE_RATIO /= 10;
	init( MIN_AVAILABLE_SPACE_RATIO_SAFETY_BUFFER,              0.01 ); if( isSimulated ) MIN_AVAILABLE_SPACE_RATIO_SAFETY_BUFFER /= 10;
	init( TARGET_AVAILABLE_SPACE_RATIO,                         0.30 ); BUGGIFY_KNOB(TARGET_AVAILABLE_SPACE_RATIO, randomize && BUGGIFY, TARGET_AVAILABLE_SPACE_RATIO / 10);

	init( MAX_TL_SS_VERSION_DIFFERENCE,                         1e99 ); // BUGGIFY_KNOB(MAX_TL_SS_VERSION_DIFFERENCE, randomize && BUGGIFY, std::max(1.0, 0.25 * VERSIONS_PER_SECOND)); // spring starts at half this value //FIXME: this knob causes ratekeeper to clamp on idle cluster in simulation that have a large number of logs
	init( MAX_TL_SS_VERSION_DIFFERENCE_BATCH,                   1e99 );
	init( MAX_MACHINES_FALLING_BEHIND,                             1 );

	init( MAX_TPS_HISTORY_SAMPLES,                               600 );
	init( NEEDED_TPS_HISTORY_SAMPLES,                            200 );
	init( AUTO_TAG_THROTTLE_DURABILITY_LAG_VERSIONS,           100e6 );
	init( AUTO_TAG_THROTTLE_SPRING_DURABILITY_LAG,              50e6 );
	init( TARGET_DURABILITY_LAG_VERSIONS,                      350e6 ); // Should be larger than STORAGE_DURABILITY_LAG_SOFT_MAX
	init( TARGET_DURABILITY_LAG_VERSIONS_BATCH,                150e6 ); // Should be larger than STORAGE_DURABILITY_LAG_SOFT_MAX
	init( DURABILITY_LAG_UNLIMITED_THRESHOLD,                   50e6 );
	init( INITIAL_DURABILITY_LAG_MULTIPLIER,                    1.02 );
	init( DURABILITY_LAG_REDUCTION_RATE,                      0.9999 );
	init( DURABILITY_LAG_INCREASE_RATE,                        1.001 );
	init( STORAGE_SERVER_LIST_FETCH_TIMEOUT,                    20.0 );
	init( BW_THROTTLING_ENABLED,                                true );

	bool buggifySmallBWLag = randomize && BUGGIFY;
	init( TARGET_BW_LAG,                                        90.0 ); BUGGIFY_KNOB(TARGET_BW_LAG, buggifySmallBWLag, 10.0);
	init( TARGET_BW_LAG_BATCH,                                  60.0 ); BUGGIFY_KNOB(TARGET_BW_LAG_BATCH, buggifySmallBWLag, 4.0);
	init( TARGET_BW_LAG_UPDATE,                                  9.0 ); BUGGIFY_KNOB(TARGET_BW_LAG_UPDATE, buggifySmallBWLag, 1.0);
	init( MIN_BW_HISTORY,                                         10 );
	init( BW_ESTIMATION_INTERVAL,                               10.0 ); BUGGIFY_KNOB(BW_ESTIMATION_INTERVAL, buggifySmallBWLag, 2.0);
	init( BW_LAG_INCREASE_AMOUNT,                                1.1 );
	init( BW_LAG_DECREASE_AMOUNT,                                0.9 );
	init( BW_FETCH_WORKERS_INTERVAL,                             5.0 );
	init( BW_RW_LOGGING_INTERVAL,                                5.0 );
	init( BW_MAX_BLOCKED_INTERVAL,                              10.0 ); BUGGIFY_KNOB(BW_MAX_BLOCKED_INTERVAL, buggifySmallBWLag, 2.0);
	init( BW_RK_SIM_QUIESCE_DELAY,                             300.0 );

	init( TAG_THROTTLE_PUSH_INTERVAL,                            1.0 ); BUGGIFY_KNOB(TAG_THROTTLE_PUSH_INTERVAL, randomize && BUGGIFY, 0.0);
	init( SS_THROTTLE_TAGS_TRACKED,                                5 ); BUGGIFY_KNOB(SS_THROTTLE_TAGS_TRACKED, randomize && BUGGIFY, deterministicRandom()->randomInt(5, 15));
	init( GLOBAL_TAG_THROTTLING,                                true );
	init( GLOBAL_TAG_THROTTLING_MIN_RATE,                        1.0 );
	init( GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED,                100 );
	init( GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER,              240.0 );
	init( GLOBAL_TAG_THROTTLING_PROXY_LOGGING_INTERVAL,         60.0 );
	init( GLOBAL_TAG_THROTTLING_TRACE_INTERVAL,                 15.0 );
	init( GLOBAL_TAG_THROTTLING_REPORT_ONLY,                   false );
	init( GLOBAL_TAG_THROTTLING_FORGET_THROUGHPUT_THRESHOLD,  4096.0 );
	init( THROUGHPUT_QUOTA_TRACK_THRESHOLD,                   409600 );
	init( THROUGHPUT_QUOTA_CACHE_BATCH_SIZE,                      10 ); BUGGIFY_KNOB(THROUGHPUT_QUOTA_CACHE_BATCH_SIZE, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 20));
	init( THROUGHPUT_QUOTA_CACHE_EXPIRATION_CHECK_INTERVAL,      5.0 );
	init( THROUGHPUT_QUOTA_CACHE_EXPIRATION_THRESHOLD,          60.0 );
	init( GLOBAL_TAG_THROTTLING_LIMITING_THRESHOLD,             10e6 );
	init( CLIENT_THROUGHPUT_TRACKER_EXPIRATION_CHECK_INTERVAL,   5.0 );
	init( THROTTLE_WITH_UNSET_THROUGHPUT_QUOTAS,                true );
	init( LOW_PRIORITY_GRV_STARVATION_TOLERANCE_RATIO,           0.5 ); BUGGIFY_KNOB(LOW_PRIORITY_GRV_STARVATION_TOLERANCE_RATIO, randomize && BUGGIFY, deterministicRandom()->random01());
	init( TRACK_TENANT_GROUP_THROUGHPUT,                       false );
	init( USE_CLIENT_THROUGHPUT_TRACKER,                       false ); BUGGIFY_KNOB(USE_CLIENT_THROUGHPUT_TRACKER, randomize && BUGGIFY, true);

	init( GLOBAL_TAG_THROTTLING_TARGET_RATE_FOLDING_TIME,         1.0 );
	init( GLOBAL_TAG_THROTTLING_TRANSACTION_COUNT_FOLDING_TIME,   1.0 );
	init( GLOBAL_TAG_THROTTLING_TRANSACTION_RATE_FOLDING_TIME,    5.0 );
	init( GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME,                5.0 );
	init( CLIENT_THROUGHPUT_TRACKER_DATA_FOLDING_TIME,            1.0 );
	init( CLIENT_THROUGHPUT_TRACKER_RATE_FOLDING_TIME,            5.0 );

	init( THROTTLING_ID_BUSY_THRESHOLD,                           0.8 );
	init( THROTTLING_ID_FORGET_THRESHOLD,                         0.5 );

	//Storage Metrics
	init( STORAGE_METRICS_AVERAGE_INTERVAL,                    120.0 );
	init( STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS,        1000.0 / STORAGE_METRICS_AVERAGE_INTERVAL );  // milliHz!
	init( SPLIT_JITTER_AMOUNT,                                  0.05 ); BUGGIFY_KNOB(SPLIT_JITTER_AMOUNT, randomize && BUGGIFY, 0.2);
	init( IOPS_UNITS_PER_SAMPLE,                                10000 * 1000 / STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS / 100 );
	init( BYTES_WRITTEN_UNITS_PER_SAMPLE,                           SHARD_MIN_BYTES_PER_KSEC / STORAGE_METRICS_AVERAGE_INTERVAL_PER_KSECONDS / 25 );
	init( BYTES_READ_UNITS_PER_SAMPLE,                        100000 ); // 100K bytes
	init( OPS_READ_UNITS_PER_SAMPLE, 100 * STORAGE_METRICS_AVERAGE_INTERVAL ); // One sample per measurement interval for each 100 ops/s read rate 
	init( READ_HOT_SUB_RANGE_CHUNK_SIZE,                    10000000 ); // 10MB
	init( EMPTY_READ_PENALTY,                                     20 ); // 20 bytes
	init( DD_SHARD_COMPARE_LIMIT,                               1000 );
	init( READ_SAMPLING_ENABLED,                                true ); BUGGIFY_KNOB(READ_SAMPLING_ENABLED, randomize && BUGGIFY, false);// enable/disable read sampling
	init( DD_MOVING_DATA_TRACE_INTERVAL,                         120 );
	init( MOVING_WINDOW_SAMPLE_SIZE,                        10000000 ); // 10MB
	init( READ_METRICS_INCLUDE_SYSTEM_KEYS,                     true );

	//Storage Server
	init( STORAGE_LOGGING_DELAY,                                 5.0 );
	init( STORAGE_SERVER_POLL_METRICS_DELAY,                     1.0 );
	init( FUTURE_VERSION_DELAY,                                  1.0 );
	init( STORAGE_LIMIT_BYTES,                                500000 );
	init( BUGGIFY_LIMIT_BYTES,                                  1000 );
	init( FETCH_USING_STREAMING,                               false ); BUGGIFY_KNOB(FETCH_USING_STREAMING, randomize && BUGGIFY, true); //Determines if fetch keys uses streaming reads
	init( FETCH_USING_BLOB,                                    false );
	init( FETCH_BLOCK_BYTES,                                     2e6 );
	init( FETCH_KEYS_PARALLELISM_BYTES,                          4e6 ); BUGGIFY_KNOB(FETCH_KEYS_PARALLELISM_BYTES, randomize && BUGGIFY, 3e6);
	init( FETCH_KEYS_PARALLELISM,                                  2 );
	init( FETCH_KEYS_PARALLELISM_CHANGE_FEED,                      6 );
	init( FETCH_KEYS_LOWER_PRIORITY,                               0 ); BUGGIFY_KNOB(FETCH_KEYS_LOWER_PRIORITY, randomize && BUGGIFY, 1);
	init( SERVE_FETCH_CHECKPOINT_PARALLELISM,                      4 );
	init( SERVE_AUDIT_STORAGE_PARALLELISM,                         1 );
	init( BUGGIFY_BLOCK_BYTES,                                 10000 );
	init( STORAGE_RECOVERY_VERSION_LAG_LIMIT,				2 * MAX_READ_TRANSACTION_LIFE_VERSIONS );
	init( STORAGE_COMMIT_BYTES,                             10000000 ); BUGGIFY_KNOB(STORAGE_COMMIT_BYTES, randomize && BUGGIFY, 2000000);
	init( STORAGE_FETCH_BYTES,                               2500000 ); BUGGIFY_KNOB(STORAGE_FETCH_BYTES, randomize && BUGGIFY,  500000);
	init( STORAGE_DURABILITY_LAG_REJECT_THRESHOLD,              0.25 );
	init( STORAGE_DURABILITY_LAG_MIN_RATE,                       0.1 );
	init( STORAGE_COMMIT_INTERVAL,                               0.5 ); BUGGIFY_KNOB(STORAGE_COMMIT_INTERVAL, randomize && BUGGIFY, 2.0);
	init( ENABLE_READ_RANGE_BUSYNESS_REPORTING,                 true ); BUGGIFY_KNOB(ENABLE_READ_RANGE_BUSYNESS_REPORTING, randomize && BUGGIFY, false);
	init( SHARD_READ_SAMPLE_UPDATE_INTERVAL,                    30.0 ); BUGGIFY_KNOB(SHARD_READ_SAMPLE_UPDATE_INTERVAL, randomize && BUGGIFY, 0);

	// Constants which affect the fraction of data which is sampled
	// by storage severs to estimate key-range sizes and splits.
	//
	// The rough goal is for the sample size to be a fixed fraction of the total
	// size of all keys and values, 1/BYTE_SAMPLING_FACTOR, which defaults to 1/250.
	// This includes an estimated overhead per entry of BYTE_SAMPLING_OVERHEAD,
	// which defaults to 100 bytes.
	//
	// NOTE: This BYTE_SAMPLING_FACTOR and BYTE_SAMPLING_OVERHEAD knobs can't be
	// changed after a database has been created. Data which has been already
	// sampled can't be resampled, and the estimates of the size of key ranges
	// implicitly includes these constants.
	init( BYTE_SAMPLING_FACTOR,                                  250 ); //cannot buggify because of differences in restarting tests
	init( BYTE_SAMPLING_OVERHEAD,                                100 );
	init( MIN_BYTE_SAMPLING_PROBABILITY,                           0 ); // Adjustable only for test of PhysicalShardMove. should be always zero for other cases
	init( MAX_STORAGE_SERVER_WATCH_BYTES,                      100e6 ); BUGGIFY_KNOB(MAX_STORAGE_SERVER_WATCH_BYTES, randomize && BUGGIFY, 10e3);
	init( MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE,                        1e9 ); BUGGIFY_KNOB(MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE, randomize && BUGGIFY, 1e3);
	init( LONG_BYTE_SAMPLE_RECOVERY_DELAY,                      60.0 );
	init( BYTE_SAMPLE_LOAD_PARALLELISM,                            8 ); BUGGIFY_KNOB(BYTE_SAMPLE_LOAD_PARALLELISM, randomize && BUGGIFY, 1);
	init( BYTE_SAMPLE_LOAD_DELAY,                                0.0 ); BUGGIFY_KNOB(BYTE_SAMPLE_LOAD_DELAY, randomize && BUGGIFY, 0.1);
	init( BYTE_SAMPLE_START_DELAY,                               1.0 ); BUGGIFY_KNOB(BYTE_SAMPLE_START_DELAY, randomize && BUGGIFY, 0.0);
	init( BEHIND_CHECK_DELAY,                                    2.0 );
	init( BEHIND_CHECK_COUNT,                                      2 );
	init( BEHIND_CHECK_VERSIONS,             5 * VERSIONS_PER_SECOND );
	init( WAIT_METRICS_WRONG_SHARD_CHANCE,   isSimulated ? 1.0 : 0.1 );
	init( MIN_TAG_READ_PAGES_RATE,                               100 ); BUGGIFY_KNOB(MIN_TAG_READ_PAGES_RATE, randomize && BUGGIFY, 0);
	// In simulation, we cannot efficiently simulate a very large workload, so using the
	// default MIN_TAG_WRITE_PAGES_RATE value of 100 causes undercounting of write throughput
	// that is substantial. To work around this, set MIN_TAG_WRITE_PAGES_RATE to 1 in simulation.
	init( MIN_TAG_WRITE_PAGES_RATE,                              100 ); if( isSimulated ) MIN_TAG_WRITE_PAGES_RATE = 1;
	init( TAG_MEASUREMENT_INTERVAL,                              5.0 ); BUGGIFY_KNOB(TAG_MEASUREMENT_INTERVAL, randomize && BUGGIFY, 10.0);
	init( PREFIX_COMPRESS_KVS_MEM_SNAPSHOTS,                    true ); BUGGIFY_KNOB(PREFIX_COMPRESS_KVS_MEM_SNAPSHOTS, randomize && BUGGIFY, false);
	init( REPORT_DD_METRICS,                                    true );
	init( DD_METRICS_REPORT_INTERVAL,                           30.0 );
	init( FETCH_KEYS_TOO_LONG_TIME_CRITERIA,                   300.0 );
	init( MAX_STORAGE_COMMIT_TIME,                             120.0 ); //The max fsync stall time on the storage server and tlog before marking a disk as failed
	init( RANGESTREAM_LIMIT_BYTES,                               2e6 ); BUGGIFY_KNOB(RANGESTREAM_LIMIT_BYTES, randomize && BUGGIFY, 1);
	init( CHANGEFEEDSTREAM_LIMIT_BYTES,                          1e6 ); BUGGIFY_KNOB(CHANGEFEEDSTREAM_LIMIT_BYTES, randomize && BUGGIFY, 1);
	init( BLOBWORKERSTATUSSTREAM_LIMIT_BYTES,                    1e4 ); BUGGIFY_KNOB(BLOBWORKERSTATUSSTREAM_LIMIT_BYTES, randomize && BUGGIFY, 1);
	init( BW_MUTATION_STREAM_LIMIT_BYTES,                        2e5 ); BUGGIFY_KNOB(BW_MUTATION_STREAM_LIMIT_BYTES, randomize && BUGGIFY, (int64_t)deterministicRandom()->randomSkewedUInt32(1, 1e4));
	init( ENABLE_CLEAR_RANGE_EAGER_READS,                       false); BUGGIFY_KNOB(ENABLE_CLEAR_RANGE_EAGER_READS, randomize && BUGGIFY, deterministicRandom()->coinflip());
	init( CHECKPOINT_TRANSFER_BLOCK_BYTES,                      40e6 );
	init( QUICK_GET_VALUE_FALLBACK,                             true );
	init( QUICK_GET_KEY_VALUES_FALLBACK,                        true );
	init( STRICTLY_ENFORCE_BYTE_LIMIT,                          false); BUGGIFY_KNOB(STRICTLY_ENFORCE_BYTE_LIMIT, randomize && BUGGIFY, deterministicRandom()->coinflip());
	init( FRACTION_INDEX_BYTELIMIT_PREFETCH,                      0.2); BUGGIFY_KNOB(FRACTION_INDEX_BYTELIMIT_PREFETCH, randomize && BUGGIFY, 0.01 + deterministicRandom()->random01());
	init( MAX_PARALLEL_QUICK_GET_VALUE,                           10 ); BUGGIFY_KNOB(MAX_PARALLEL_QUICK_GET_VALUE, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 100));
	init( QUICK_GET_KEY_VALUES_LIMIT,                           2000 );
	init( QUICK_GET_KEY_VALUES_LIMIT_BYTES,                      1e7 );
	init( STORAGE_FEED_QUERY_HARD_LIMIT,                      100000 );
	// Read priority definitions in the form of a list of their relative concurrency share weights
	init( STORAGESERVER_READ_PRIORITIES,           "120,10,20,40,60" );
	// The total concurrency which will be shared by active priorities according to their relative weights
	init( STORAGE_SERVER_READ_CONCURRENCY,                        70 );
	// The priority number which each ReadType maps to in enumeration order
	// This exists for flexibility but assigning each ReadType to its own unique priority number makes the most sense
	// The enumeration is currently: eager, fetch, low, normal, high
	init( STORAGESERVER_READTYPE_PRIORITY_MAP,           "0,1,2,3,4" );
	init( SPLIT_METRICS_MAX_ROWS,                              10000 ); BUGGIFY_KNOB(SPLIT_METRICS_MAX_ROWS, randomize && BUGGIFY, 10);

	//Wait Failure
	init( MAX_OUTSTANDING_WAIT_FAILURE_REQUESTS,                 250 ); BUGGIFY_KNOB(MAX_OUTSTANDING_WAIT_FAILURE_REQUESTS, randomize && BUGGIFY, 2);
	init( WAIT_FAILURE_DELAY_LIMIT,                              1.0 ); BUGGIFY_KNOB(WAIT_FAILURE_DELAY_LIMIT, randomize && BUGGIFY, 5.0);

	//Worker
	init( WORKER_LOGGING_INTERVAL,                               5.0 );
	init( ROLE_REFRESH_LOGGING_INTERVAL,                        60.0 );
	init( HEAP_PROFILER_INTERVAL,                               30.0 );
	init( UNKNOWN_CC_TIMEOUT,                                  600.0 );
	init( DEGRADED_RESET_INTERVAL,                          24*60*60 ); // FIXME: short interval causes false positive degraded state to flap, e.g. when everyone tries and fails to connect to dead coordinator: BUGGIFY_KNOB(DEGRADED_RESET_INTERVAL, randomize && BUGGIFY, 10);
	init( DEGRADED_WARNING_LIMIT,                                  1 );
	init( DEGRADED_WARNING_RESET_DELAY,                   7*24*60*60 );
	init( TRACE_LOG_FLUSH_FAILURE_CHECK_INTERVAL_SECONDS,         10 );
	init( TRACE_LOG_PING_TIMEOUT_SECONDS,                        5.0 );
	init( MIN_DELAY_CC_WORST_FIT_CANDIDACY_SECONDS,             10.0 );
	init( MAX_DELAY_CC_WORST_FIT_CANDIDACY_SECONDS,             30.0 );
	init( DBINFO_FAILED_DELAY,                                   1.0 );
	init( ENABLE_WORKER_HEALTH_MONITOR,                        false ); BUGGIFY_KNOB(ENABLE_WORKER_HEALTH_MONITOR, randomize && BUGGIFY, true);
	init( WORKER_HEALTH_MONITOR_INTERVAL,                       60.0 );
	init( PEER_LATENCY_CHECK_MIN_POPULATION,                      30 );
	init( PEER_LATENCY_DEGRADATION_PERCENTILE,                  0.50 );
	init( PEER_LATENCY_DEGRADATION_THRESHOLD,                   0.05 );
	init( PEER_LATENCY_DEGRADATION_PERCENTILE_SATELLITE,        0.50 );
	init( PEER_LATENCY_DEGRADATION_THRESHOLD_SATELLITE,          0.1 );
	init( PEER_TIMEOUT_PERCENTAGE_DEGRADATION_THRESHOLD,         0.1 );
	init( PEER_DEGRADATION_CONNECTION_FAILURE_COUNT,               5 );
	init( WORKER_HEALTH_REPORT_RECENT_DESTROYED_PEER,           true );
	init( STORAGE_SERVER_REBOOT_ON_IO_TIMEOUT,                 false ); BUGGIFY_KNOB(STORAGE_SERVER_REBOOT_ON_IO_TIMEOUT, randomize && BUGGIFY, true);
	init( STORAGE_DISK_CLEANUP_MAX_RETRIES,                       10 );
	init( STORAGE_DISK_CLEANUP_RETRY_INTERVAL,  isSimulated ? 2 : 30 );

	// Test harness
	init( WORKER_POLL_DELAY,                                     1.0 );

	// Coordination
	init( COORDINATED_STATE_ONCONFLICT_POLL_INTERVAL,            1.0 ); BUGGIFY_KNOB(COORDINATED_STATE_ONCONFLICT_POLL_INTERVAL, randomize && BUGGIFY, 10.0);
	init( FORWARD_REQUEST_TOO_OLD,                        4*24*60*60 ); BUGGIFY_KNOB(FORWARD_REQUEST_TOO_OLD, randomize && BUGGIFY, 60.0);
	init( ENABLE_CROSS_CLUSTER_SUPPORT,                         true ); BUGGIFY_KNOB(ENABLE_CROSS_CLUSTER_SUPPORT, randomize && BUGGIFY, false);
	init( COORDINATOR_LEADER_CONNECTION_TIMEOUT,                20.0 );

	// Dynamic Knobs (implementation)
	init( COMPACTION_INTERVAL,             isSimulated ? 5.0 : 300.0 );
	init( BROADCASTER_SELF_UPDATE_DELAY,                         1.0 );
	init( GET_COMMITTED_VERSION_TIMEOUT,                         3.0 );
	init( GET_SNAPSHOT_AND_CHANGES_TIMEOUT,                      3.0 );
	init( FETCH_CHANGES_TIMEOUT,                                 3.0 );

	// Buggification
	init( BUGGIFIED_EVENTUAL_CONSISTENCY,                        1.0 );
	init( BUGGIFY_ALL_COORDINATION,                            false ); BUGGIFY_KNOB(BUGGIFY_ALL_COORDINATION, randomize && BUGGIFY, true);

	// Status
	init( STATUS_MIN_TIME_BETWEEN_REQUESTS,                      0.0 );
	init( MAX_STATUS_REQUESTS_PER_SECOND,                      256.0 );
	init( CONFIGURATION_ROWS_TO_FETCH,                         20000 );
	init( DISABLE_DUPLICATE_LOG_WARNING,                       false );
	init( HISTOGRAM_REPORT_INTERVAL,                           300.0 );

	// Timekeeper
	init( TIME_KEEPER_DELAY,                                      10 );
	init( TIME_KEEPER_MAX_ENTRIES,                3600 * 24 * 30 * 6 ); BUGGIFY_KNOB(TIME_KEEPER_MAX_ENTRIES, randomize && BUGGIFY, 2);

	// Fast Restore
	init( FASTRESTORE_FAILURE_TIMEOUT,                          3600 );
	init( FASTRESTORE_HEARTBEAT_INTERVAL,                         60 );
	init( FASTRESTORE_SAMPLING_PERCENT,                          100 ); BUGGIFY_KNOB(FASTRESTORE_SAMPLING_PERCENT, randomize && BUGGIFY, deterministicRandom()->random01() * 100);
	init( FASTRESTORE_NUM_LOADERS,                                 3 ); BUGGIFY_KNOB(FASTRESTORE_NUM_LOADERS, randomize && BUGGIFY, deterministicRandom()->random01() * 10 + 1);
	init( FASTRESTORE_NUM_APPLIERS,                                3 ); BUGGIFY_KNOB(FASTRESTORE_NUM_APPLIERS, randomize && BUGGIFY, deterministicRandom()->random01() * 10 + 1);
	init( FASTRESTORE_TXN_BATCH_MAX_BYTES,           1024.0 * 1024.0 ); BUGGIFY_KNOB(FASTRESTORE_TXN_BATCH_MAX_BYTES, randomize && BUGGIFY, deterministicRandom()->random01() * 1024.0 * 1024.0 + 1.0);
	init( FASTRESTORE_VERSIONBATCH_MAX_BYTES, 10.0 * 1024.0 * 1024.0 ); BUGGIFY_KNOB(FASTRESTORE_VERSIONBATCH_MAX_BYTES, randomize && BUGGIFY, deterministicRandom()->random01() < 0.2 ? 50 * 1024 : deterministicRandom()->random01() < 0.4 ? 100 * 1024 * 1024 : deterministicRandom()->random01() * 1000.0 * 1024.0 * 1024.0); // too small value may increase chance of TooManyFile error
	init( FASTRESTORE_VB_PARALLELISM,                              5 ); BUGGIFY_KNOB(FASTRESTORE_VB_PARALLELISM, randomize && BUGGIFY, deterministicRandom()->random01() < 0.2 ? 2 : deterministicRandom()->random01() * 10 + 1);
	init( FASTRESTORE_VB_MONITOR_DELAY,                           30 ); BUGGIFY_KNOB(FASTRESTORE_VB_MONITOR_DELAY, randomize && BUGGIFY, deterministicRandom()->random01() * 20 + 1);
	init( FASTRESTORE_VB_LAUNCH_DELAY,                           1.0 ); BUGGIFY_KNOB(FASTRESTORE_VB_LAUNCH_DELAY, randomize && BUGGIFY, deterministicRandom()->random01() < 0.2 ? 0.1 : deterministicRandom()->random01() * 10.0 + 1);
	init( FASTRESTORE_ROLE_LOGGING_DELAY,                          5 ); BUGGIFY_KNOB(FASTRESTORE_ROLE_LOGGING_DELAY, randomize && BUGGIFY, deterministicRandom()->random01() * 60 + 1);
	init( FASTRESTORE_UPDATE_PROCESS_STATS_INTERVAL,               5 ); BUGGIFY_KNOB(FASTRESTORE_UPDATE_PROCESS_STATS_INTERVAL, randomize && BUGGIFY, deterministicRandom()->random01() * 60 + 1);
	init( FASTRESTORE_ATOMICOP_WEIGHT,                             1 ); BUGGIFY_KNOB(FASTRESTORE_ATOMICOP_WEIGHT, randomize && BUGGIFY, deterministicRandom()->random01() * 200 + 1);
	init( FASTRESTORE_MONITOR_LEADER_DELAY,                        5 ); BUGGIFY_KNOB(FASTRESTORE_MONITOR_LEADER_DELAY, randomize && BUGGIFY, deterministicRandom()->random01() * 100);
	init( FASTRESTORE_STRAGGLER_THRESHOLD_SECONDS,                60 ); BUGGIFY_KNOB(FASTRESTORE_STRAGGLER_THRESHOLD_SECONDS, randomize && BUGGIFY, deterministicRandom()->random01() * 240 + 10);
	init( FASTRESTORE_TRACK_REQUEST_LATENCY,              	   false ); BUGGIFY_KNOB(FASTRESTORE_TRACK_REQUEST_LATENCY, randomize && BUGGIFY, false);
	init( FASTRESTORE_MEMORY_THRESHOLD_MB_SOFT,                 6144 ); BUGGIFY_KNOB(FASTRESTORE_MEMORY_THRESHOLD_MB_SOFT, randomize && BUGGIFY, 1);
	init( FASTRESTORE_WAIT_FOR_MEMORY_LATENCY,                    10 ); BUGGIFY_KNOB(FASTRESTORE_WAIT_FOR_MEMORY_LATENCY, randomize && BUGGIFY, 60);
	init( FASTRESTORE_HEARTBEAT_DELAY,                            10 ); BUGGIFY_KNOB(FASTRESTORE_HEARTBEAT_DELAY, randomize && BUGGIFY, deterministicRandom()->random01() * 120 + 2);
	init( FASTRESTORE_HEARTBEAT_MAX_DELAY,                        10 ); BUGGIFY_KNOB(FASTRESTORE_HEARTBEAT_MAX_DELAY, randomize && BUGGIFY, FASTRESTORE_HEARTBEAT_DELAY * 10);
	init( FASTRESTORE_APPLIER_FETCH_KEYS_SIZE,                   100 ); BUGGIFY_KNOB(FASTRESTORE_APPLIER_FETCH_KEYS_SIZE, randomize && BUGGIFY, deterministicRandom()->random01() * 10240 + 1);
	init( FASTRESTORE_LOADER_SEND_MUTATION_MSG_BYTES, 1.0 * 1024.0 * 1024.0 ); BUGGIFY_KNOB(FASTRESTORE_LOADER_SEND_MUTATION_MSG_BYTES, randomize && BUGGIFY, deterministicRandom()->random01() < 0.2 ? 1024 : deterministicRandom()->random01() * 5.0 * 1024.0 * 1024.0 + 1);
	init( FASTRESTORE_GET_RANGE_VERSIONS_EXPENSIVE,            false ); BUGGIFY_KNOB(FASTRESTORE_GET_RANGE_VERSIONS_EXPENSIVE, randomize && BUGGIFY, deterministicRandom()->random01() < 0.5 ? true : false);
	init( FASTRESTORE_REQBATCH_PARALLEL,                          50 ); BUGGIFY_KNOB(FASTRESTORE_REQBATCH_PARALLEL, randomize && BUGGIFY, deterministicRandom()->random01() * 100 + 1);
	init( FASTRESTORE_REQBATCH_LOG,                            false ); BUGGIFY_KNOB(FASTRESTORE_REQBATCH_LOG, randomize && BUGGIFY, deterministicRandom()->random01() < 0.2 ? true : false);
	init( FASTRESTORE_TXN_CLEAR_MAX,                             100 ); BUGGIFY_KNOB(FASTRESTORE_TXN_CLEAR_MAX, randomize && BUGGIFY, deterministicRandom()->random01() * 100 + 1);
	init( FASTRESTORE_TXN_RETRY_MAX,                              10 ); BUGGIFY_KNOB(FASTRESTORE_TXN_RETRY_MAX, randomize && BUGGIFY, deterministicRandom()->random01() * 100 + 1);
	init( FASTRESTORE_TXN_EXTRA_DELAY,                           0.0 ); BUGGIFY_KNOB(FASTRESTORE_TXN_EXTRA_DELAY, randomize && BUGGIFY, deterministicRandom()->random01() * 1 + 0.001);
	init( FASTRESTORE_NOT_WRITE_DB,                            false ); // Perf test only: set it to true will cause simulation failure
	init( FASTRESTORE_USE_RANGE_FILE,                           true ); // Perf test only: set it to false will cause simulation failure
	init( FASTRESTORE_USE_LOG_FILE,                             true ); // Perf test only: set it to false will cause simulation failure
	init( FASTRESTORE_SAMPLE_MSG_BYTES,                      1048576 ); BUGGIFY_KNOB(FASTRESTORE_SAMPLE_MSG_BYTES, randomize && BUGGIFY, deterministicRandom()->random01() * 2048);
	init( FASTRESTORE_SCHED_UPDATE_DELAY,                        0.1 ); BUGGIFY_KNOB(FASTRESTORE_SCHED_UPDATE_DELAY, randomize && BUGGIFY, deterministicRandom()->random01() * 2);
	init( FASTRESTORE_SCHED_TARGET_CPU_PERCENT,                   70 ); BUGGIFY_KNOB(FASTRESTORE_SCHED_TARGET_CPU_PERCENT, randomize && BUGGIFY, deterministicRandom()->random01() * 100 + 50); // simulate cpu usage can be larger than 100
	init( FASTRESTORE_SCHED_MAX_CPU_PERCENT,                      90 ); BUGGIFY_KNOB(FASTRESTORE_SCHED_MAX_CPU_PERCENT, randomize && BUGGIFY, FASTRESTORE_SCHED_TARGET_CPU_PERCENT + deterministicRandom()->random01() * 100);
	init( FASTRESTORE_SCHED_INFLIGHT_LOAD_REQS,                   50 ); BUGGIFY_KNOB(FASTRESTORE_SCHED_INFLIGHT_LOAD_REQS, randomize && BUGGIFY, deterministicRandom()->random01() < 0.2 ? 1 : deterministicRandom()->random01() * 30 + 1);
	init( FASTRESTORE_SCHED_INFLIGHT_SEND_REQS,                    3 ); BUGGIFY_KNOB(FASTRESTORE_SCHED_INFLIGHT_SEND_REQS, randomize && BUGGIFY, deterministicRandom()->random01() < 0.2 ? 1 : deterministicRandom()->random01() * 10 + 1);
	init( FASTRESTORE_SCHED_LOAD_REQ_BATCHSIZE,                    5 ); BUGGIFY_KNOB(FASTRESTORE_SCHED_LOAD_REQ_BATCHSIZE, randomize && BUGGIFY, deterministicRandom()->random01() < 0.2 ? 1 : deterministicRandom()->random01() * 10 + 1);
	init( FASTRESTORE_SCHED_INFLIGHT_SENDPARAM_THRESHOLD,         10 ); BUGGIFY_KNOB(FASTRESTORE_SCHED_INFLIGHT_SENDPARAM_THRESHOLD, randomize && BUGGIFY, deterministicRandom()->random01() < 0.2 ? 1 : deterministicRandom()->random01() * 15 + 1);
	init( FASTRESTORE_SCHED_SEND_FUTURE_VB_REQS_BATCH,             2 ); BUGGIFY_KNOB(FASTRESTORE_SCHED_SEND_FUTURE_VB_REQS_BATCH, randomize && BUGGIFY, deterministicRandom()->random01() < 0.2 ? 1 : deterministicRandom()->random01() * 15 + 1);
	init( FASTRESTORE_NUM_TRACE_EVENTS,                          100 ); BUGGIFY_KNOB(FASTRESTORE_NUM_TRACE_EVENTS, randomize && BUGGIFY, deterministicRandom()->random01() < 0.2 ? 1 : deterministicRandom()->random01() * 500 + 1);
	init( FASTRESTORE_EXPENSIVE_VALIDATION,                    false ); BUGGIFY_KNOB(FASTRESTORE_EXPENSIVE_VALIDATION, randomize && BUGGIFY, deterministicRandom()->random01() < 0.5 ? true : false);
	init( FASTRESTORE_WRITE_BW_MB,                                70 ); BUGGIFY_KNOB(FASTRESTORE_WRITE_BW_MB, randomize && BUGGIFY, deterministicRandom()->random01() < 0.5 ? 2 : 100);
	init( FASTRESTORE_RATE_UPDATE_SECONDS,                       1.0 ); BUGGIFY_KNOB(FASTRESTORE_RATE_UPDATE_SECONDS, randomize && BUGGIFY, deterministicRandom()->random01() < 0.5 ? 0.1 : 2);
	init( FASTRESTORE_DUMP_INSERT_RANGE_VERSION,               false );

	init( REDWOOD_DEFAULT_PAGE_SIZE,                            8192 );
	init( REDWOOD_DEFAULT_EXTENT_SIZE,              32 * 1024 * 1024 );
	init( REDWOOD_DEFAULT_EXTENT_READ_SIZE,              1024 * 1024 );
	init( REDWOOD_EXTENT_CONCURRENT_READS,                         4 );
	init( REDWOOD_KVSTORE_RANGE_PREFETCH,                       true );
	init( REDWOOD_PAGE_REBUILD_MAX_SLACK,                       0.33 );
	init( REDWOOD_PAGE_REBUILD_SLACK_DISTRIBUTION,              0.50 );
	init( REDWOOD_LAZY_CLEAR_BATCH_SIZE_PAGES,                    10 );
	init( REDWOOD_LAZY_CLEAR_MIN_PAGES,                            0 );
	init( REDWOOD_LAZY_CLEAR_MAX_PAGES,                          1e6 );
	init( REDWOOD_REMAP_CLEANUP_WINDOW_BYTES, 4LL * 1024 * 1024 * 1024 );
	init( REDWOOD_REMAP_CLEANUP_TOLERANCE_RATIO,                0.05 );
	init( REDWOOD_PAGEFILE_GROWTH_SIZE_PAGES,                  20000 ); BUGGIFY_KNOB(REDWOOD_PAGEFILE_GROWTH_SIZE_PAGES, randomize && BUGGIFY, deterministicRandom()->randomInt(200, 1000));
	init( REDWOOD_METRICS_INTERVAL,                              5.0 );
	init( REDWOOD_HISTOGRAM_INTERVAL,                           30.0 );
	init( REDWOOD_EVICT_UPDATED_PAGES,                          true ); BUGGIFY_KNOB(REDWOOD_EVICT_UPDATED_PAGES, randomize && BUGGIFY, false);
	init( REDWOOD_DECODECACHE_REUSE_MIN_HEIGHT,                    2 ); BUGGIFY_KNOB(REDWOOD_DECODECACHE_REUSE_MIN_HEIGHT, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 7));
	init( REDWOOD_NODE_MAX_UNBALANCE,                              2 );
	init( REDWOOD_IO_PRIORITIES,                       "32,32,32,32" );
	init( REDWOOD_CURSOR_DEBUG_TRACE_LIMIT,                     1000 );

	// Server request latency measurement
	init( LATENCY_SKETCH_ACCURACY,                              0.01 );
	init( FILE_LATENCY_SKETCH_ACCURACY,                         0.01 );
	init( LATENCY_METRICS_LOGGING_INTERVAL,                     60.0 );

	// Relocation request queue measurement
	init( RELOCATION_METRICS_SMOOTHING, 						300.0);
	init( RELOCATION_METRICS_WINDOW, 							 10.0);

	// Cluster recovery
	init ( CLUSTER_RECOVERY_EVENT_NAME_PREFIX,              "Master" );

	// Encryption
	init( SIM_KMS_MAX_KEYS,                                     4096 );
	init( ENCRYPT_PROXY_MAX_DBG_TRACE_LENGTH,                 100000 );

	// encrypt key proxy
	init( USE_REST_SIM_KMS_VAULT,                              false );
	init( ENABLE_BLOB_GRANULE_COMPRESSION,                      true ); BUGGIFY_KNOB(ENABLE_BLOB_GRANULE_COMPRESSION, randomize && BUGGIFY, deterministicRandom()->coinflip());
	init( BLOB_GRANULE_COMPRESSION_FILTER,                    "ZSTD" ); BUGGIFY_KNOB(BLOB_GRANULE_COMPRESSION_FILTER, randomize && BUGGIFY, CompressionUtils::toString(CompressionUtils::getRandomFilter()));
	init( ENCRYPTION_LOGGING_INTERVAL,                           5.0 );

	// KMS connector type
	init( KMS_CONNECTOR_TYPE,                     "RESTKmsConnector" );

	// Blob granlues
	init( BG_URL,        isSimulated ? "file://simfdb/fdbblob/" : "" ); // TODO: store in system key space or something, eventually
	bool buggifyMediumGranules = simulationMediumShards || (randomize && BUGGIFY);
	// BlobGranuleVerify* simulation tests use "knobs", BlobGranuleCorrectness* use "tenant", default in real clusters is "knobs"
	init( BG_METADATA_SOURCE,                                "knobs" );
	// FIXME: remove knob in 9.0
	init( BG_USE_BLOB_RANGE_CHANGE_LOG,                        true  ); BUGGIFY_KNOB(BG_USE_BLOB_RANGE_CHANGE_LOG, randomize && BUGGIFY, false);
	init( BG_SNAPSHOT_FILE_TARGET_BYTES,                    20000000 ); if (BUGGIFY_KNOB(BG_SNAPSHOT_FILE_TARGET_BYTES, buggifySmallShards, 50000 * deterministicRandom()->randomInt(1, 4))) {} else BUGGIFY_KNOB(BG_SNAPSHOT_FILE_TARGET_BYTES, buggifyMediumGranules, 50000 * deterministicRandom()->randomInt(1, 20));
	init( BG_SNAPSHOT_FILE_TARGET_CHUNK_BYTES,               64*1024 ); BUGGIFY_KNOB(BG_SNAPSHOT_FILE_TARGET_CHUNK_BYTES, randomize && BUGGIFY, BG_SNAPSHOT_FILE_TARGET_BYTES / (1 << deterministicRandom()->randomInt(0, 8)));
	init( BG_DELTA_BYTES_BEFORE_COMPACT, BG_SNAPSHOT_FILE_TARGET_BYTES/2 ); BUGGIFY_KNOB(BG_DELTA_BYTES_BEFORE_COMPACT, randomize && BUGGIFY, BG_DELTA_BYTES_BEFORE_COMPACT * (1.0 + deterministicRandom()->random01() * 3.0)/2.0);
	init( BG_DELTA_FILE_TARGET_BYTES,   BG_DELTA_BYTES_BEFORE_COMPACT/10 );
	init( BG_DELTA_FILE_TARGET_CHUNK_BYTES,                  32*1024 ); BUGGIFY_KNOB(BG_DELTA_FILE_TARGET_CHUNK_BYTES, randomize && BUGGIFY, BG_DELTA_FILE_TARGET_BYTES / (1 << deterministicRandom()->randomInt(0, 7)));
	init( BG_MAX_SPLIT_FANOUT,                                    10 ); BUGGIFY_KNOB(BG_MAX_SPLIT_FANOUT, randomize && BUGGIFY, deterministicRandom()->randomInt(5, 15));
	init( BG_MAX_MERGE_FANIN,                                     10 ); BUGGIFY_KNOB(BG_MAX_MERGE_FANIN, randomize && BUGGIFY, deterministicRandom()->randomInt(2, 15));
	init( BG_HOT_SNAPSHOT_VERSIONS,                          5000000 );

	init( BG_CONSISTENCY_CHECK_ENABLED,                         true ); BUGGIFY_KNOB(BG_CONSISTENCY_CHECK_ENABLED, randomize && BUGGIFY, false);
	init( BG_CONSISTENCY_CHECK_TARGET_SPEED_KB,                 1000 ); BUGGIFY_KNOB(BG_CONSISTENCY_CHECK_TARGET_SPEED_KB, randomize && BUGGIFY, BG_CONSISTENCY_CHECK_TARGET_SPEED_KB * (deterministicRandom()->randomInt(2, 50) / 10));
	init( BG_KEY_TUPLE_TRUNCATE_OFFSET,                            0 );
	init( BG_KEY_SNOWTRAM_AWARE,          isSimulated ? false : true ); // disable by default in simulation for now until we can convert more workloads to snowtram keys and not have issues with this in restarting tests
	init( BG_ENABLE_SPLIT_TRUNCATED,                           false ); BUGGIFY_KNOB(BG_ENABLE_SPLIT_TRUNCATED, randomize && BUGGIFY, true);
	init( BG_ENABLE_READ_DRIVEN_COMPACTION,                     true ); BUGGIFY_KNOB(BG_ENABLE_READ_DRIVEN_COMPACTION, randomize && BUGGIFY, false);
	init( BG_RDC_BYTES_FACTOR,                                     2 ); BUGGIFY_KNOB(BG_RDC_BYTES_FACTOR, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 10));
	init( BG_RDC_READ_FACTOR,                                      3 ); BUGGIFY_KNOB(BG_RDC_READ_FACTOR, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 10));
	init( BG_WRITE_MULTIPART,                                  false ); BUGGIFY_KNOB(BG_WRITE_MULTIPART, randomize && BUGGIFY, true);
	init( BG_ENABLE_DYNAMIC_WRITE_AMP,                          true ); BUGGIFY_KNOB(BG_ENABLE_DYNAMIC_WRITE_AMP, randomize && BUGGIFY, false);
	init( BG_DYNAMIC_WRITE_AMP_MIN_FACTOR,                       0.5 );
	init( BG_DYNAMIC_WRITE_AMP_DECREASE_FACTOR,                  0.8 );

	init( BG_ENABLE_MERGING,                                    true ); BUGGIFY_KNOB(BG_ENABLE_MERGING, randomize && BUGGIFY, false);
	init( BG_MERGE_CANDIDATE_THRESHOLD_SECONDS, isSimulated ? 20.0 : 30 * 60 ); BUGGIFY_KNOB(BG_MERGE_CANDIDATE_THRESHOLD_SECONDS, randomize && BUGGIFY, 5.0);
	init( BG_MERGE_CANDIDATE_DELAY_SECONDS, BG_MERGE_CANDIDATE_THRESHOLD_SECONDS / 10.0 );

	init( BLOB_WORKER_INITIAL_SNAPSHOT_PARALLELISM,                2 ); BUGGIFY_KNOB(BLOB_WORKER_INITIAL_SNAPSHOT_PARALLELISM, randomize && BUGGIFY, 1);
	// The resnapshot/delta parallelism knobs are deprecated and replaced by the budget_bytes knobs! FIXME: remove after next release
	init( BLOB_WORKER_RESNAPSHOT_PARALLELISM,                     40 ); BUGGIFY_KNOB(BLOB_WORKER_RESNAPSHOT_PARALLELISM, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 10));
	init( BLOB_WORKER_DELTA_FILE_WRITE_PARALLELISM,             2000 ); BUGGIFY_KNOB(BLOB_WORKER_DELTA_FILE_WRITE_PARALLELISM, randomize && BUGGIFY, deterministicRandom()->randomInt(10, 100));
	init( BLOB_WORKER_RDC_PARALLELISM,                             2 ); BUGGIFY_KNOB(BLOB_WORKER_RDC_PARALLELISM, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 6));
	init( BLOB_WORKER_RDC_MIN_WAVE_DELAY,                        1.0 ); BUGGIFY_KNOB(BLOB_WORKER_RDC_MIN_WAVE_DELAY, randomize && BUGGIFY, deterministicRandom()->randomInt(0, 1000) / 1000.0); 
	init( BLOB_WORKER_RESNAPSHOT_BUDGET_BYTES,        1024*1024*1024 );
	init( BLOB_WORKER_DELTA_WRITE_BUDGET_BYTES,       1024*1024*1024 );
	init( BLOB_WORKER_TIMEOUT,                                  10.0 ); BUGGIFY_KNOB(BLOB_WORKER_TIMEOUT, randomize && BUGGIFY, 1.0);
	// more than MVCC window since behind delta files can block for that window for committed check
	init( BLOB_WORKER_REQUEST_TIMEOUT,                          10.0 ); BUGGIFY_KNOB(BLOB_WORKER_REQUEST_TIMEOUT, randomize && BUGGIFY, 1.0);
	init( BLOB_WORKERLIST_FETCH_INTERVAL,                        1.0 );
	init( BLOB_WORKER_BATCH_GRV_INTERVAL,                        0.1 );
	init( BLOB_WORKER_EMPTY_GRV_INTERVAL,                        0.5 );
	init( BLOB_WORKER_GRV_HISTORY_MAX_SIZE,                    10000 ); BUGGIFY_KNOB(BLOB_WORKER_GRV_HISTORY_MAX_SIZE, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 20));
	init( BLOB_WORKER_GRV_HISTORY_MIN_VERSION_GRANULARITY,    100000 ); BUGGIFY_KNOB(BLOB_WORKER_GRV_HISTORY_MIN_VERSION_GRANULARITY, randomize && BUGGIFY, (int64_t)deterministicRandom()->randomSkewedUInt32(1, 1000000));
	init( BLOB_WORKER_DO_REJECT_WHEN_FULL,                      true ); BUGGIFY_KNOB(BLOB_WORKER_DO_REJECT_WHEN_FULL, randomize && BUGGIFY, false);
	init( BLOB_WORKER_REJECT_WHEN_FULL_THRESHOLD,                0.9 );
	init( BLOB_WORKER_FORCE_FLUSH_CLEANUP_DELAY,                30.0 ); BUGGIFY_KNOB(BLOB_WORKER_FORCE_FLUSH_CLEANUP_DELAY, randomize && BUGGIFY, deterministicRandom()->randomInt(0, 10) - 1);
	init( BLOB_WORKER_DISK_ENABLED,                            false ); BUGGIFY_KNOB(BLOB_WORKER_DISK_ENABLED, randomize && BUGGIFY, true);
	init( BLOB_WORKER_STORE_TYPE,                                  3 );
	init( BLOB_WORKER_REJOIN_TIME,                              10.0 );

	init( BLOB_MANAGER_STATUS_EXP_BACKOFF_MIN,                   0.1 );
	init( BLOB_MANAGER_STATUS_EXP_BACKOFF_MAX,                   5.0 );
	init( BLOB_MANAGER_STATUS_EXP_BACKOFF_EXPONENT,              1.5 );
	init( BLOB_MANAGER_CONCURRENT_MERGE_CHECKS,                   64 ); BUGGIFY_KNOB(BLOB_MANAGER_CONCURRENT_MERGE_CHECKS, randomize && BUGGIFY, 1 << deterministicRandom()->randomInt(0, 7));
	init( FORCE_PURGE_WAIT_BLOBBIFY_TIMEOUT, isSimulated ? 20.0 : 600.0 ); BUGGIFY_KNOB(FORCE_PURGE_WAIT_BLOBBIFY_TIMEOUT, randomize && BUGGIFY, 1 + 30 * deterministicRandom()->random01());
	init( BLOB_MANAGER_PURGE_GRANULE_PARALLELISM,               1000 ); BUGGIFY_KNOB(BLOB_MANAGER_PURGE_GRANULE_PARALLELISM, randomize && BUGGIFY, (int)deterministicRandom()->randomSkewedUInt32(1, 100));
	init( BLOB_MANAGER_ENABLE_MEDIAN_ASSIGNMENT_LIMITING,       true ); BUGGIFY_KNOB(BLOB_MANAGER_ENABLE_MEDIAN_ASSIGNMENT_LIMITING, randomize && BUGGIFY, false);
	init( BLOB_MANAGER_MEDIAN_ASSIGNMENT_ALLOWANCE,              2.0 ); BUGGIFY_KNOB(BLOB_MANAGER_MEDIAN_ASSIGNMENT_ALLOWANCE, randomize && BUGGIFY, (1.0 + deterministicRandom()->random01() * 2));
	init( BLOB_MANAGER_MEDIAN_ASSIGNMENT_MIN_SAMPLES_PER_WORKER,   3 );
	init( BLOB_MANAGER_MEDIAN_ASSIGNMENT_MAX_SAMPLES_PER_WORKER,  10 );
	init( BLOB_MANIFEST_BACKUP,                                false );
	init( BLOB_MANIFEST_BACKUP_INTERVAL,  isSimulated ?  5.0 : 600.0 );
	init( BLOB_MIGRATOR_CHECK_INTERVAL,    isSimulated ?  1.0 : 60.0 );
	init( BLOB_MIGRATOR_BULK_LOAD_CHECK_INTERVAL,                5.0 ); BUGGIFY_KNOB(BLOB_MIGRATOR_BULK_LOAD_CHECK_INTERVAL, randomize && BUGGIFY, 10.0 * deterministicRandom()->random01());
	init( BLOB_MANIFEST_RW_ROWS,            isSimulated ?  10 : 1000 );
	init( BLOB_MANIFEST_MAX_ROWS_PER_TRANSACTION,  isSimulated ?  30 : 10000 );
	init( BLOB_MANIFEST_RETRY_INTERVAL,        isSimulated ?  1 : 30 );
	init( BLOB_MIGRATOR_ERROR_RETRIES,                            20 );
	init( BLOB_MIGRATOR_PREPARE_TIMEOUT,                       120.0 );
	init( BLOB_RESTORE_MANIFEST_FILE_MAX_SIZE, isSimulated ? 10000 : 10000000 );
	init( BLOB_RESTORE_MANIFEST_RETENTION_MAX,                    10 );
	init( BLOB_RESTORE_MLOGS_RETENTION_SECS,  isSimulated ?  180 : 3600 * 24 * 14 );
	init( BLOB_RESTORE_LOAD_KEY_VERSION_MAP_STEP_SIZE,  isSimulated ?  10 : 2000 );
	init( BLOB_RESTORE_SKIP_EMPTY_RANGES,                      false ); BUGGIFY_KNOB(BLOB_RESTORE_SKIP_EMPTY_RANGES, randomize && BUGGIFY, true);

	init( BLOB_GRANULES_FLUSH_BATCH_SIZE,      isSimulated ?  2 : 64 );

	init( BGCC_TIMEOUT,                   isSimulated ? 10.0 : 120.0 );
	init( BGCC_MIN_INTERVAL,                isSimulated ? 1.0 : 10.0 );

	// Blob Metadata
	init( BLOB_METADATA_CACHE_TTL, isSimulated ? 120 : 24 * 60 * 60 ); BUGGIFY_KNOB(BLOB_METADATA_CACHE_TTL, randomize && BUGGIFY, deterministicRandom()->randomInt(50, 100));

	// HTTP KMS Connector
	init( REST_KMS_CONNECTOR_KMS_DISCOVERY_URL_MODE,           "file");
	init( REST_KMS_CONNECTOR_VALIDATION_TOKEN_MODE,            "file");
	init( REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE,          1024);
	init( REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE, 10 * 1024);
	init( REST_KMS_CONNECTOR_REFRESH_KMS_URLS,                   true);
	init( REST_KMS_CONNECTOR_REFRESH_KMS_URLS_INTERVAL_SEC,       600);
	// Below KMS configurations are responsible for:
	// Discovering KMS URLs, fetch encryption keys endpoint and validation token details.
	// Configurations are expected to be passed as command-line arguments.
	// NOTE: Care must be taken when attempting to update below configurations for a up/running FDB cluster.
	init( REST_KMS_CONNECTOR_DISCOVER_KMS_URL_FILE,                "");
	init( REST_KMS_CONNECTOR_GET_ENCRYPTION_KEYS_ENDPOINT,         "");
	init( REST_KMS_CONNECTOR_GET_LATEST_ENCRYPTION_KEYS_ENDPOINT,  "");
	init( REST_KMS_CONNECTOR_GET_BLOB_METADATA_ENDPOINT,           "");
	init( REST_KMS_CONNECTOR_INIT_REQUEST_RETRY_DELAY,           0.05);
	init( REST_KMS_CONNECTOR_MAX_REQUEST_RETRY_DELAY,            60.0);
	// Details to fetch validation token from a localhost file
	// acceptable format: "<token_name1>$<absolute_file_path1>,<token_name2>$<absolute_file_path2>,.."
	// NOTE: 'token-name" can NOT contain '$' character
	init( REST_KMS_CONNECTOR_VALIDATION_TOKEN_DETAILS,             "");
	init( ENABLE_REST_KMS_COMMUNICATION,                        false); BUGGIFY_KNOB(ENABLE_REST_KMS_COMMUNICATION, randomize && BUGGIFY, true);
	init( REST_KMS_CONNECTOR_REMOVE_TRAILING_NEWLINE,           false);
	init( REST_KMS_CURRENT_BLOB_METADATA_REQUEST_VERSION,           1);
	init( REST_KMS_MAX_BLOB_METADATA_REQUEST_VERSION,               1);
	init( REST_KMS_CURRENT_CIPHER_REQUEST_VERSION,                  1);
	init( REST_KMS_MAX_CIPHER_REQUEST_VERSION,                      1);
	init( REST_SIM_KMS_VAULT_DIR,                                  "");
	init( REST_SIM_KMS_VAULT_INJECT_RESPONSE_DELAY,             true ); BUGGIFY_KNOB(REST_SIM_KMS_VAULT_INJECT_RESPONSE_DELAY, randomize && BUGGIFY, deterministicRandom()->coinflip());
	init( REST_KMS_STABILITY_CHECK_INTERVAL,                      5.0);
	init( KMS_LOGGING_INTERVAL,                                   5.0);


	init( CONSISTENCY_SCAN_ACTIVE_THROTTLE_RATIO,                0.5 ); BUGGIFY_KNOB(CONSISTENCY_SCAN_ACTIVE_THROTTLE_RATIO, randomize && BUGGIFY, deterministicRandom()->random01());
	init( ENABLE_CONSISTENCY_SCAN,                              true );

	// Drop in-memory state associated with an idempotency id after this many seconds. Once dropped, this id cannot be
	// expired proactively, but will eventually get cleaned up by the idempotency id cleaner.
	init( IDEMPOTENCY_ID_IN_MEMORY_LIFETIME,                       10);
	// Attempt to clean old idempotency ids automatically this often
 	init( IDEMPOTENCY_IDS_CLEANER_POLLING_INTERVAL,                10);
	// Don't clean idempotency ids younger than this
 	init( IDEMPOTENCY_IDS_MIN_AGE_SECONDS,              3600 * 24 * 7);

	// Bulk Load
	init( BULK_LOAD_FILE_POINTER_BATCH_SIZE,                     100 ); BUGGIFY_KNOB(BULK_LOAD_FILE_POINTER_BATCH_SIZE, randomize && BUGGIFY, deterministicRandom()->randomInt(10, 100));
	init( BULK_LOAD_PARALLEL_FILE_VALIDATIONS,                     1 ); BUGGIFY_KNOB(BULK_LOAD_PARALLEL_FILE_VALIDATIONS, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 10));
	init( BULK_LOAD_AWARE_STORAGE_QUOTA_ENFORCEMENT,            false); BUGGIFY_KNOB(BULK_LOAD_AWARE_STORAGE_QUOTA_ENFORCEMENT, isSimulated, deterministicRandom()->coinflip());
	init( BULK_LOAD_OVER_QUOTA_CHECK_INTERVAL,                     10); if ( isSimulated ) BULK_LOAD_OVER_QUOTA_CHECK_INTERVAL = 5; BUGGIFY_KNOB(BULK_LOAD_OVER_QUOTA_CHECK_INTERVAL, randomize && BUGGIFY, deterministicRandom()->randomInt(1, 5));
	init( BULK_LOAD_FILE_VALIDATION_LEVEL,                         1 ); BUGGIFY_KNOB(BULK_LOAD_FILE_VALIDATION_LEVEL, randomize && BUGGIFY, deterministicRandom()->randomInt(0, 3));
	init( BULK_LOAD_STAGE_GRANULES,                            false ); if ( isSimulated ) BULK_LOAD_STAGE_GRANULES = deterministicRandom()->coinflip();
	init( BULK_LOAD_SS_MOVE,                                   false ); // if ( isSimulated ) BULK_LOAD_SS_MOVE = deterministicRandom()->coinflip();
	// TODO enable once DD integration complete!

	// clang-format on

	if (clientKnobs) {
		clientKnobs->IS_ACCEPTABLE_DELAY =
		    clientKnobs->IS_ACCEPTABLE_DELAY *
		    std::min(MAX_READ_TRANSACTION_LIFE_VERSIONS, MAX_WRITE_TRANSACTION_LIFE_VERSIONS) /
		    (5.0 * VERSIONS_PER_SECOND);
		clientKnobs->INIT_MID_SHARD_BYTES = MIN_SHARD_BYTES;
		clientKnobs->BULK_LOAD_MAX_CREATE_VERSION_DIFFERENCE =
		    std::max(clientKnobs->BULK_LOAD_MAX_CREATE_VERSION_DIFFERENCE, MAX_READ_TRANSACTION_LIFE_VERSIONS);
		clientKnobs->BULK_LOAD_MAX_CREATE_VERSION_DIFFERENCE =
		    std::max(clientKnobs->BULK_LOAD_MAX_CREATE_VERSION_DIFFERENCE, MAX_WRITE_TRANSACTION_LIFE_VERSIONS);
	}
}
