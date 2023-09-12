#############
Release Notes
#############

71.3.6
======

Fixes
-----

* Fix memory leak in Status.actor.cpp by avoiding creating database context as part of lockedStatusFetcher operation.


71.3.5
======

Reliability
-----------

* Ensure exponential backoff while refreshing KMS URLs.
* Update EncryptKeyProxy KMS health check timeouts.
* Avoid commit proxy and storage server commit timeout due to KMS failure.


Fixes
-----

* Fix quota throttler clear cost estimation.
* Fix the DataLossRecovery workload stuck issue.
* Wait file deleted in blob manifest cleanup.
* Fixed slowtask and OOM in large blob granule purges, and improved observability.
* Improve Blob Manager’s granule balancing algorithm in pathological cases.
* Allow granule purge to continue when the cluster is unhealthy.
* Restore original function names for old blob granule API.
* Fix memory leak in Status.actor.cpp by avoiding creating database context as part of lockedStatusFetcher operation.


Other Changes
-------------

* Report how many logical ranges and shards have been over-replicated by custom replication.

71.3.4
======

Reliability
-----------

* Support tracking write throughput for multiple tags on a single storage server

Fixes
-----

* Fix a bug in the change feed cache on the blob workers causing it to hang when enabled on an encrypted cluster.
* Fixed locality based exclusions to move tlog processes off excluded servers.
* Fix the go tuple tests for new go version.
* Fixes change feed head-of-line blocking if a client requests a whenAtLeast on a feed version significantly ahead of where the feed current location.
* Fix forward compatibility of blob granule APIs to allow upgrading to future major release with changed fields.
* Fix worker server reporting IoTimeoutError as injected fault.
* Fix fdbcli status json to report blob granules backup status after blob manager restarts.
* Fix blob manager to find the right key for blob granules split in edge cases.
* Fix a workload issue that was caused DataLossRecovery test to hang.
* Reduce the frequency of polling tenants' storage usage.
* Stagger storage quota estimation requests and observability improvements.
* Fix test storage server recruitment when cluster is unhealthy.
* Add a new flag to skip missing data tenants during blob granules restore. The flag is false as default.
* Stop tracking a storage server after its removal to avoid infinite looping.
* Prevent Status actor from bubbling up timeout error leading to cluster recovery.
* Fixes bug and adds test for properly truncating keys in the case of large splits.


Other Changes
-------------

* Improved observability for change feeds, blob worker, blob manager, and encrypt key proxy.


71.3.3
======

Fixes
-----

* Fix BlobManager tenant watch to avoid process getting stuck forever.


71.3.2
======

Fixes
-----

* Segfault in DDShardTracker where the tracker gets destroyed before ::run finished
* GRV queue leak
* The first time a GRV proxy hits its request limit it indefinitely rejects all future GRVs even after its request queue becomes empty


71.3.1
======

Features
--------
- Add HoltLinearSmoother class
- EKP and KMS Health Check
- Adding blob granule restarting tests to use tenants, encryption
- Implement IStorageMetricsService interface
- Make stepSize configurable for preloadApplyMutationsKeyVersionMap
- Add networkoption to disable non-TLS connections
- New ConsistencyScan
- Add WatchMapSize and OutstandingWatches to TransactionMetrics
- Add blob manifest url and mlogs url in status json.
- Add tenant_id_prefix to metacluster status
- Support blob granule restore in fdbrestore
- Move lastFlushTs to BlobGranuleBackupConfig
- Cache change feeds durably on blob workers
- Support restoring a cluster with a tenant in the error state
- EaR RESTSimKMS Vault
- EaR: Update KMS URL refresh policy and fix bugs
- EaR: Add test case to validate decryption with invalid key
- Decouple token bucket knobs for different types of throttlers 
- Adding blob ranges to backup keys
- Allow reading from system keyspace even with default tenant
- Add ExpectStableThroughput simulation test
- Simplify GlobalTagThrottler limiting TPS calculation


Performance
-----------
- Re-enabling change feed coalesce knob
- Tune default GlobalTagThrottler knobs
- Improve performance of TransactionTagCounter
- Update knob values for Storage Quota polling intervals
- Removing verbose logs that are not useful for 71.3
- Remove some unnecessary ref-counting in the PTree
- Adding delta file count before resnapshot to minimize impact of small delta files
- EaR: reduce metrics logging
- Made blob metadata load lazily from EKP
- Fix retransmits in corruption check
- Increase coverage for slow tests in TestHarness2
- Dump manifest by using multiple transactions
- EaR: Optimize logging from GetEncryptCipherKey
- Speed up BackupCorrectnessClean
- Speed up DR upgrade tests


Reliability
-----------
- Fixing stable feeds check by allowing more time post speedup sim
- Check if var is None before comparing with int
- Handle operation_cancelled properly in configuration database
- Reset connection idle time when restarting connection monitor
- BlobGranuleRestore - skip muations applying if restore target version
- Update SimKmsVault unit test assert checking max encryption keys
- Check serverList before update storage metadata 
- Test use of the metacluster after a restore 
- Decrease the number of records in MutationLogReaderCorrectness 
- EaR: Remove usage of ENABLE_CONFIGURABLE_ENCRYPTION knob
- Only starting bg restarting correctness
- Ignore AutomaticIdempotency tests 
- Remove mockkms dir from the build dir

Fixes
-----

* Make network address returned by SimExternalConnection's dns resolution public to fix resolving an fdb process' ip in simulation
* Transaction could block if system priority not set, which would block BM startup
* Make redwood tests terminate after certain amount of time
* Fix poll and notify bug of opsReadSample
* Fixing BlobGranuleRequests to properly bump read version on retry
* Miscellaneous bug fixes and improvements for GlobalTagThrottler 
* Fix get*OperationCost functions for empty mutations/results
* Coordination server crash on file_not_found error
* Fix RangeResult.readThrough misuse 
* Fix queued relocations missing     
* Stop consistency scanner while restore is in progress
* Fix computeRestoreEndVersion bug when outLogs is null 
* Fix issues when start DataDistributor in mock DD test 
* Remove blobGranuleLockKeys after blob granule restore 
* Fix MoveKeysClean.toml failure
* Fix a dynamic knobs bug where fulfilling a promise could cause it to get deleted
* Fix a couple of incorrect snowflake/release-71.3 protocol versions
* Fix flushing empty range
* Disable all parallel restore tests
* Fix check in getExactRange that determines whether we can return early
* Fix issue with inconsistent coordinator disk queue 
* Fix tenant map update race when applying mutations 
* Fix per-scan-loop throttling to properly throttle in the loop 
* Fix worker server handleIOErrors heap-use-after-free
* Fix blob restore stuck issue 
* Fix bug in FDB MultiVersionTransaction.actor.cpp 
* Disable Change-Feed cache for 71.3 restarting tests


71.3.0
======

Features
--------
- Added system clock timing to CommitDebug trace events using MonotonicTime field.
- BlobGranule driven backup + restore.
- Encryption at-Rest, supports per-tenant and/or per-cluster encryption semantics.
- External Vault support for per-tenant blob granule storage location.
- Improved APIs for creating and managing Blob Granule ranges.
- Updated tenants to be ID oriented. Tenants cannot be upgraded from previous releases.
- Restored data clusters can join a metacluster. A management cluster can be recovered from the data clusters.
- Added support for locking tenants.
- Added a new function fdb_database_get_client_status providing client-side connection status json information.
- Added new database options to define defaults for transaction options report_conflicting_keys and used_during_commit_protection_disable.
- Added a new network option retain_client_library_copies to avoid deleting the temporary library copies after completion of the process. This may be useful in various debugging and profiling scenarios.
- Added a new network option trace_initialize_on_setup to enable client traces already on fdb_setup_network, so that traces do not get lost on client configuration issues.
- TraceEvents related to TLS handshakes, new connections, and tenant access by authorization token are no longer subject to suppression or throttling, using an internal “AuditedEvent” TraceEvent classification.
- Usage of authorization token is logged as part of AuditedEvent, with a 5-second suppression time window for duplicate entries. (suppression time window is controlled by AUDIT_TIME_WINDOW flow knob).
- Client-side arena blocks and packet buffers that held authorization tokens are zeroed out immediately before deallocation to minimize the likelihood of leaked tokens.
- Authorization token’s “aud” field may have singular string as value, instead of a singleton array (optional JWT standard).
- RPC response to tenant-scoped get-key-location requests now returns a key range clamped to the tenant keyspace to prevent leaking key range information of the normal keyspace to the other tenants.
- Updated tenant authorization to be ID oriented: instead of base64-encoded tenant names, a token’s “tenants” field shall contain one or more base64-encoded 8-byte big-endian tenant ID numbers. (which is also a tenant key prefix).


Performance
-----------
- Reduced overhead of creating transactions in the Java bindings by using the default option of used_during_commit_protection_disable, instead of setting it on every transaction.
- Remove duplicated transaction timeout management in the multi-version client. This significantly reduces the overall CPU usage related to setting timeouts.


Reliability
-----------
- Added quota driven workload isolation.
- Added commands for hot range detection and custom sharding + over-replication.
- Added a new network option fail_incompatible_client. If the option is set, transactions will fail with fail_incompatible_client in cases where they attempt to connect to a cluster without providing a compatible client library.
- Added a new network option trace_initialize_on_setup to enable client traces already on fdb_setup_network, so that traces do not get lost on client configuration issues.
- Starting with API version 710300, fdb_setup_network will fail if at least one of the external clients fails to initialize, e.g. because it does not support the selected API version. Added a network option ignore_external_client_failures to restore the previous behavior ignoring external client initialization errors.
- In case where the request to determine the cluster version fails because of a fatal error, avoid entering the busy loop repeating the request indefinitely.
- Fixed a run loop profiler deadlock in the multi-version client, the profiler can be now be enabled on the client-side using the network option enable_run_loop_profiling.
- Use different base trace file names for client threads in multi-threaded mode. This fixes the issue of one client thread deleting traces of another one. 