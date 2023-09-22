.. _release-notes:

#############
Release Notes
#############

8.0.0
=====

Features
--------
* Bulk Loading APIs, DB state and tests.
* Range Lock APIs, related DB state and tests.
* Add clearknob fdbcli command.
* Read metrics API for all read requests.
* Emergency tenant movement tool.


Features Improvements
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