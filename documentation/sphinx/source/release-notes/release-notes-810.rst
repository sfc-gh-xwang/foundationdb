.. _release-notes:

#############
Release Notes
#############

8.1.0
=====

Feature Improvements
--------------------

* NDV-aware quota throttling `(FDBCORE-5706) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-5706>`_
* Improved ReportThroughputRequest logging `(FDBCORE-6235) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6235>`_
* Support prioritization in quota throttler `(FDBCORE-6031) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6031>`_
* Expand Java bindings to include throughput quota functions `(FDBCORE-6152) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6152>`_
* Add the ability to list tenants by locked state `(SNOW-808839) <https://snowflakecomputing.atlassian.net/browse/SNOW-808839>`_
* Expanded trace logging coverage of metacluster commands `(SNOW-894200) <https://snowflakecomputing.atlassian.net/browse/SNOW-894200>`_
* Disable CLEAR_RANGE_EAGER_READS by default `(SNOW-895990) <https://snowflakecomputing.atlassian.net/browse/SNOW-895990>`_
* Encryption at Rest: expose latest cipher key staleness status through fdbcli `(FDBCORE-5302) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-5302>`_
* Add blobrange list and listgranules fdbcli commands `(FDBCORE-6065) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6065>`_
* Nuke active change feeds purge `(FDBCORE-6070) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6070>`_
* Blobbify after “Create Table As Select” commands `(FDBCORE-6304) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6304>`_
* Handle REST request retryable request breaching maxRetries `(FDBCORE-6393) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6393>`_
* Make RESTClient request retries configurable `(FDBCORE-6380) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6380>`_
* BlobWorker openGranule - use single transaction to check full restore and others `(FDBCORE-6521) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6521>`_
* Enable hybrid storage to be aware of snowtram key format and spaces `(FDBCORE-6326) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6326>`_
* Improve the evaluation order of DD team destination eligibility `(FDBCORE-6058) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6058>`_


Reliability
-----------

* Changing fdbdev.profiles.json to use release-8.1 docker image `(FDBCORE-5928) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-5928>`_
* Add AuthZ bulkload fdbcli test `(FDBCORE-5837) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-5837>`_
* Improvements to Hybrid debug logging `(SNOW-892996) <https://snowflakecomputing.atlassian.net/browse/SNOW-892996>`_
* Validate data cluster connection string changes `(SNOW-645874) <https://snowflakecomputing.atlassian.net/browse/SNOW-645874>`_
* Improve Hybrid Stats `(FDBCORE-6068) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6068>`_
* Bulk load active counter `(FDBCORE-6020) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6020>`_
* Update default throughput quota throttling knobs `(FDBCORE-6188) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6188>`_
* More accurate storage queue size estimation `(FDBCORE-5894) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-5894>`_
* Encryption at-Rest logging improvements `(FDBCORE-6210) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6210>`_
* Shorten perpetual_wiggle_delay in simulation to avoid check timeout `(FDBCORE-6159) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6159>`_
* Trace RFC7638 thumbprint for each AuthZ public key  `(FDBCORE-6181) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6181>`_
* Add more diagnostic to verify failed case for cached tokens `(FDBCORE-6282) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6282>`_
* Perform more accurate checking of the recovery tracker `(PR 1143) <https://github.com/snowflakedb/frostdb/pull/1143>`_
* Remove debug transaction call that can be very verbose `(PR 1147) <https://github.com/snowflakedb/frostdb/pull/1147>`_
* Update random move keys to choose a team according to the storage policy `(PR 1173) <https://github.com/snowflakedb/frostdb/pull/1173>`_
* Remove go ctest test `(PR 1175) <https://github.com/snowflakedb/frostdb/pull/1175>`_
* Don't buggify leader election knobs in latency sensitive test `(PR 1208) <https://github.com/snowflakedb/frostdb/pull/1208>`_
* Avoid Cluser blocking on isRestoring check on RateKeeper (re)start `(FDBCORE-6285) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6285>`_
* Fix issue with quota throttler target rate initialization `(FDBCORE-6154) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6154>`_
* Fix issue where multiple instances of a singleton role could run simultaneously `(PR 1154) <https://github.com/snowflakedb/frostdb/pull/1154>`_
* Status could fail with a broken promise if the encryption key proxy died during the request `(PR 1156) <https://github.com/snowflakedb/frostdb/pull/1156>`_
* Fix a race in Ratekeeper and disable between throttling on restore `(FDBCORE-5036) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-5036>`_
* Return normalKeyMutations when resolver decide it’s committed and trace metrics `(FDBCORE-6056) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6056>`_
* Add a new blob restore state to fix a race after data copy `(FDBCORE-5856) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-5856>`_
* Initialize metacluster version to avoid invalid access `(PR 927) <https://github.com/snowflakedb/frostdb/pull/927>`_
* Encryption at-Rest Fix Header Validation `(PR 941) <https://github.com/snowflakedb/frostdb/pull/941>`_
* Fix read metrics test validation `(PR 1149) <https://github.com/snowflakedb/frostdb/pull/1149>`_
* Reset recruiting field when singleton recruitment fails  `(PR 1154) <https://github.com/snowflakedb/frostdb/pull/1154>`_
* Fix broken promise in status caused by failures to fetch KMSHealthStatus `(PR 1156) <https://github.com/snowflakedb/frostdb/pull/1156>`_
* Fix binding tester tenant list error `(PR 1234) <https://github.com/snowflakedb/frostdb/pull/1234>`_
* Fix hybrid storage corruption caused by change feed fetching race `(FDBCORE-6255) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6255>`_


Performance Improvements
------------------------

* Optimization for blobbify-after-ctas `(FDBCORE-6364) <https://snowflakecomputing.atlassian.net/browse/FDBCORE-6364>`_

Earlier release notes
---------------------
* :doc:`8.0 (API Version 800) </release-notes/release-notes-800>`
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
