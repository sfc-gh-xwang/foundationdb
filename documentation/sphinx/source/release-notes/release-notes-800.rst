.. _release-notes:

#############
Release Notes
#############

8.0.2
=====

Feature Improvements
--------------------
* Support multiple quota cost calculation formulas. `(FDBCORE-6418) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6418>`_

8.0.1
=====

Feature Improvements
--------------------
* Add print output to tenant movement for progress tracking. `(SNOW-895031) <https://snowflakecomputing.atlassian.net/browse/SNOW-895031>`_
* Add trace details about over-replicated shard and range count. `(FDBCORE-6018) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6018>`_
* Add the ability to disable tenant renames via knob and disable them by default. `(FDBCORE-6230) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6230>`_
* Decrease logging level for BlobGranuleFlushRetry to avoid verbose logging. `(PR 826) <https://github.com/snowflakedb/frostdb/pull/826>`_
* Use public endpoint for client throughput reporting to GRV proxies. `(FDBCORE-6261) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6261>`_
* Prioritize transaction tags of tenant groups for quota throttling purposes. `(FDBCORE-6261) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6261>`_
* Decouple quota throttler's throughput tracking mechanism from average cost tracking mechanism. `(FDBCORE-6327) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6327>`_
* Disable TRACK_THROUGHPUT_ON_CLIENTS for compatibility with 8.0.0 clients `(PR 1064) <https://github.com/snowflakedb/frostdb/pull/1064>`_

Fixes
-----
* Fix singleton recruitment logic to avoid extra singletons. `(PR 904) <https://github.com/snowflakedb/frostdb/pull/904>`_
* Fix EKP re-recruitment failing to halt existing EKP process. `(FDBCORE-6059) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6059>`_
* Make sure that the process ID data we are writing does not get freed before being written. `(PR 946) <https://github.com/snowflakedb/frostdb/pull/946>`_
* The cluster controller would leak memory when fetching status. `(PR 955) <https://github.com/snowflakedb/frostdb/pull/955>`_
* Some blob restore transactions were not lock aware and would fail if a tenant was locked. `(FDBCORE-6209) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6209>`_
* After an aborted tenant move, a tenant could be left in a locked state. `(PR 1084) <https://github.com/snowflakedb/frostdb/pull/1084>`_
* The commit proxy's tenant name index could become inconsistent during a restore. `(FDBCORE-6227) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6227>`_
* Deleting multiple tenants in the same transaction could leave unnecessary tombstones. `(PR 1093) <https://github.com/snowflakedb/frostdb/pull/1093>`_
* REST requests that failed with a retryable error would send an improper request on retry and effectively extend the retry interval. `(FDBCORE-6313) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6313>`_
* The commit proxy would unnecessarily validate encryption keys for all tenant transactions in ``cluster_aware`` mode. `(PR 1114) <https://github.com/snowflakedb/frostdb/pull/1114>`_
* Revert ratekeeper smoothing behavior that caused reduced stability in ratekeeper's transaction rates. `(FDBCORE-6354) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6354>`_
* Handle IO timeout on commit proxies and storage servers due to KMS connectivity failures. `(FDBCORE-5370) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-5370>`_
* Ensure exponential backoff while refreshing KMS URLs for a request. `(FDBCORE-5950) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-5950>`_
* Fix a bug for blob manifest cleanup to make sure manifest files are deleted. `(FDBCORE-5952) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-5952>`_
* Improve error handling for blob granules flush in blob manager. `(FDBCORE-5920) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-5920>`_
* Simplify the quota throttler's estimation of clear costs, and fix inconsistencies between server and client. `(SNOW-897605) <https://snowflakecomputing.atlassian.net/browse/SNOW-897605>`_ and `(FDBCORE-6217) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6217>`_
* Fixed hybrid force purge to drop data even if the blob workers are completely unavailable, so that hybrid can be dropped on a cluster where it is causing problems. `(FDBCORE-5951) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-5951>`_

Performance Improvements
------------------------
* Reduce transaction batch size during blob restore, and reduce restore progress check interval during blob restore. `(FDBCORE-6161) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6161>`_
* Improved the blob manager's algorithm to balance granules amongst blob workers such that it wouldn't over-favor new blob workers with fewer granules to the point of hotspotting. `(PR 826) <https://github.com/snowflakedb/frostdb/pull/826>`_
* Hybrid read doesn't have to wait until the next commit cycle on an empty cluster to check if a tenant is locked. `(FDBCORE-6192) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6192>`_


Reliability
-----------
* Lower KMS health check interval to 3 seconds. `(FDBCORE-6014) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6014>`_

8.0.0
=====

Features
--------
* Bulk Loading APIs, DB state and tests.
* Range Lock APIs, related DB state and tests.
* Add clearknob fdbcli command.
* Read metrics API for all read requests.
* Emergency tenant movement tool.


Feature Improvements
---------------------
* Columnar cache skew handling APIs.
* Client-size average transaction cost tracking.
* Expected transaction cost client API.
* Disable GRV cache for busy throttling IDs.
* Disable insecure versions of SSL/TLS to avoid downgrade attacks. 
* Rename OPT_TLS_DISABLE_PLAINTEXT_CONNECTION network option to OPT_TLS_REQUIRED.
* Inplace Encryption for Blob Granule. 
* Encryption Latency Metrics. 
* KMS Malformed Response Metric Counter.
* Add more KMS information in status json.
* Add fdb_future_get_bytes to read byte strings from futures.


Fixes
-----
* Fix parent-child race with actor pointer in mutation streaming.
* Fix buggification in DR test causing test to hang.
* Sim HTTP Server port stability. 
* Fix Authz hybrid unit test to improve code coverage.
* Dynamic knobs bug fixes. 
* Encryption Fix Header Validation Check.
* Fix several issues where multiple instances of the same singleton could run simultaneously.


Performance Improvements
------------------------
* Hybrid reads stream large amounts of mutations instead of returning them in one request.
* Reducing BlobWorker SlowTasks by breaking up file operations with yields.
* Reduce unnecessary refcounting in PTree. 
* Boost::unorder_flat_map for ThrottlingIdMap and TransactionTagMap. 
* Write tag throughput tracking optimizations.
* Add optional recruitment algorithm to colocate stateless processes.
* Enable causal read risky by default.
* DD re-evaluation to reduce overall data movement.


Reliability
-----------
* Add destination token for large packet debugging.
* Enforce Check phase of workloads in simulation tests.
* Adding additional tenant creation and validation to hybrid workloads.
* Add metacluster version of hybrid correctness.
* Improve clarity of invalid_cluster_id error.
* Improve version correction to detect time jumps and automatically increase the version.
* Fix memory leak caused by creating too many DatabaseContext objects.
* Handle KMS timeout in storage server and commit proxy.
* Print an warning message to stderr if a client is terminated while the network thread is still running.


Change Log
----------
* https://github.com/snowflakedb/frostdb/pull/679
* https://github.com/snowflakedb/frostdb/pull/851
* https://github.com/snowflakedb/frostdb/pull/545
* https://github.com/snowflakedb/frostdb/pull/508
* https://github.com/snowflakedb/frostdb/pull/560
* https://github.com/snowflakedb/frostdb/pull/570
* https://github.com/snowflakedb/frostdb/pull/682
* https://github.com/snowflakedb/frostdb/pull/585
* https://github.com/snowflakedb/frostdb/pull/471 
* https://github.com/snowflakedb/frostdb/pull/534 
* https://github.com/snowflakedb/frostdb/pull/619
* https://github.com/snowflakedb/frostdb/pull/345
* https://github.com/snowflakedb/frostdb/pull/351
* https://github.com/snowflakedb/frostdb/pull/405
* https://github.com/snowflakedb/frostdb/pull/521
* https://github.com/snowflakedb/frostdb/pull/304
* https://github.com/snowflakedb/frostdb/pull/329
* https://github.com/snowflakedb/frostdb/pull/941 
* https://github.com/snowflakedb/frostdb/pull/321
* https://github.com/snowflakedb/frostdb/pull/327
* https://github.com/snowflakedb/frostdb/pull/373
* https://github.com/snowflakedb/frostdb/pull/422
* https://github.com/snowflakedb/frostdb/pull/661
* https://github.com/snowflakedb/frostdb/pull/594
* https://github.com/snowflakedb/frostdb/pull/659
* https://github.com/snowflakedb/frostdb/pull/334
* https://github.com/snowflakedb/frostdb/pull/483
* https://github.com/snowflakedb/frostdb/pull/687
* https://github.com/snowflakedb/frostdb/pull/702
* https://github.com/snowflakedb/frostdb/pull/668
* https://github.com/snowflakedb/frostdb/pull/769
* https://github.com/snowflakedb/frostdb/pull/954
* https://github.com/snowflakedb/frostdb/pull/923
* https://github.com/snowflakedb/frostdb/pull/666
* https://github.com/snowflakedb/frostdb/pull/506


Earlier release notes
---------------------
* :doc:`71.3 (API Version 710300) </release-notes/release-notes-713>`
* :doc:`7.1 (API Version 710) </release-notes/release-notes-710>`
* :doc:`7.0 (API Version 700) </release-notes/release-notes-700>`
* :doc:`6.3 (API Version 630) </release-notes/release-notes-630>`
* :doc:`6.2 (API Version 620) </release-notes/release-notes-620>`
* :doc:`6.1 (API Version 610) </release-notes/release-notes-610>`
* :doc:`6.0 (API Version 600) </release-notes/release-notes-600>`
* :doc:`5.2 (API Version 520) </release-notes/release-notes-520>`
* :doc:`5.1 (API Version 510) </release-notes/release-notes-510>`
* :doc:`5.0 (API Version 500) </release-notes/release-notes-500>`
* :doc:`4.6 (API Version 460) </release-notes/release-notes-460>`
* :doc:`4.5 (API Version 450) </release-notes/release-notes-450>`
* :doc:`4.4 (API Version 440) </release-notes/release-notes-440>`
* :doc:`4.3 (API Version 430) </release-notes/release-notes-430>`
* :doc:`4.2 (API Version 420) </release-notes/release-notes-420>`
* :doc:`4.1 (API Version 410) </release-notes/release-notes-410>`
* :doc:`4.0 (API Version 400) </release-notes/release-notes-400>`
* :doc:`3.0 (API Version 300) </release-notes/release-notes-300>`
* :doc:`2.0 (API Version 200) </release-notes/release-notes-200>`
* :doc:`1.0 (API Version 100) </release-notes/release-notes-100>`
* :doc:`Beta 3 (API Version 23) </release-notes/release-notes-023>`
* :doc:`Beta 2 (API Version 22) </release-notes/release-notes-022>`
* :doc:`Beta 1 (API Version 21) </release-notes/release-notes-021>`
* :doc:`Alpha 6 (API Version 16) </release-notes/release-notes-016>`
* :doc:`Alpha 5 (API Version 14) </release-notes/release-notes-014>`
