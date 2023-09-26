/*
 * BulkLoadOpsWorkload.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/Util.h"
#include "fdbclient/BlobGranuleFiles.actor.h"
#include "fdbclient/BulkLoad.h"
#include "fdbclient/BulkLoadMetadata.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/BlobGranuleValidation.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BGCorrectnessUtils.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define BLW_DEBUG true

// A workload specifically designed to stress bulk loading functionality
struct BulkLoadOpsWorkload : TestWorkload {
	static constexpr auto NAME = "BulkLoadOps";
	// test settings
	double testDuration;
	int concurrentLoads;
	bool sequential;
	int sequentialGap;

	Future<Void> client;
	Future<Void> unitClient;
	bool stopTest;

	Optional<TenantName> tenantName;
	Optional<TenantGroupName> tenantGroupName;
	Optional<Reference<Tenant>> tenant;
	Reference<BlobConnectionProvider> bstore;

	// This tenant is for testing reservations. This is needed because the tests will create
	// situations where reservations for this tenant should fail, but we don't want that to cause
	// background bulk loads for the above tenant to fail.
	// TODO: Simplify creation / tracking of multiple tenants.
	Optional<TenantName> tenantResName;
	Optional<TenantGroupName> tenantResGroupName;
	Optional<Reference<Tenant>> tenantRes;
	Reference<BlobConnectionProvider> bstoreRes;

	// This tenant is for testing listTenants
	Optional<TenantName> listTenantName;
	Optional<TenantGroupName> listTenantGroupName;
	Optional<Reference<Tenant>> listTenant;

	int32_t nextKey;

	std::vector<Future<Void>> loadClients;

	BulkLoadOpsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 30.0);
		concurrentLoads = getOption(options, "concurrentLoads"_sr, deterministicRandom()->randomInt(1, 100));

		stopTest = false;
		tenantName = StringRef("bulkLoadOpsTenant" + std::to_string(clientId));
		tenantGroupName = StringRef("bulkLoadOpsTenant" + std::to_string(clientId) + "Group");

		tenantResName = StringRef("bulkLoadOpsTenantRes" + std::to_string(clientId));
		tenantResGroupName = StringRef("bulkLoadOpsTenantRes" + std::to_string(clientId) + "Group");

		if (clientId == 0) {
			listTenantName = StringRef("bulkLoadOpsListTenant" + std::to_string(clientId));
			listTenantGroupName = StringRef("bulkLoadOpsListTenant" + std::to_string(clientId) + "Group");
		}

		int64_t rand = wcx.sharedRandomNumber;
		sequential = rand % 2;
		rand /= 2;

		sequentialGap = 1 + rand % 2;
		rand /= 2;

		nextKey = 10000000 * clientId;

		TraceEvent("BulkLoadOpsWorkloadInit").detail("ConcurrentLoads", concurrentLoads);
	}

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	std::string newPrefix() {
		if (sequential) {
			nextKey += sequentialGap;
			return format("%08x", nextKey);
		} else {
			return deterministicRandom()->randomUniqueID().toString();
		}
	}

	ACTOR Future<TenantMapEntry> setupTenant(Database cx, TenantName name, Optional<TenantGroupName> group) {
		if (BLW_DEBUG) {
			fmt::print("Creating tenant: {0} in tenant group: {1}\n",
			           name.printable(),
			           group.present() ? group->printable() : "not given");
		}

		TenantMapEntry givenEntry;
		if (group.present()) {
			givenEntry.tenantGroup = group;
		}
		Optional<TenantMapEntry> entry = wait(TenantAPI::createTenant(cx.getReference(), name, givenEntry));
		ASSERT(entry.present());

		if (BLW_DEBUG) {
			fmt::print("Created tenant {0}: {1}\n", name.printable(), entry.get().prefix.printable());
		}

		return entry.get();
	}

	ACTOR Future<Version> doGRV(Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				Version readVersion = wait(tr.getReadVersion());
				return readVersion;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// randomly reopen tenant and do not wait for it to be ready, to test races
	void maybeReopenTenant(Database const& cx) {
		if (tenantName.present() && BUGGIFY_WITH_PROB(0.01)) {
			makeReference<Tenant>(cx, tenantName.get());
		}
	}

	ACTOR Future<Void> _setup(Database cx, BulkLoadOpsWorkload* self) {
		if (self->clientId != 0) {
			return Void();
		}
		// create initial target ranges
		TraceEvent("BulkLoadOpsSetup").detail("ConcurrentLoads", self->concurrentLoads).log();

		// enable blob granules
		wait(success(ManagementAPI::changeConfig(cx.getReference(), "blob_granules_enabled=1", true)));

		if (cx->clientInfo->get().tenantMode != TenantMode::REQUIRED && deterministicRandom()->coinflip()) {
			self->tenantName.reset();
			self->tenantResName.reset();
		}

		// Setup `self->bstore` for `self->tenant`
		state BGTenantMap tenantData(self->dbInfo);
		if (self->tenantName.present()) {
			state std::vector<std::pair<int64_t, TenantMapEntry>> tenants;
			state TenantMapEntry entry = wait(self->setupTenant(cx, self->tenantName.get(), self->tenantGroupName));

			self->tenant = makeReference<Tenant>(cx, self->tenantName.get());
			wait(self->tenant.get()->ready());

			tenants.push_back({ self->tenant.get()->id(), entry });
			tenantData.addTenants(tenants);
		}

		KeyRange range = normalKeys;
		if (self->tenant.present()) {
			range = range.withPrefix(self->tenant.get()->prefix());
		}
		wait(store(self->bstore, loadBStoreForTenant(&tenantData, range)));
		ASSERT(self->bstore.isValid());

		// Setup `self->bstoreRes` for `self->tenantRes`
		// TODO: Fix code duplication
		state BGTenantMap tenantResData(self->dbInfo);
		if (self->tenantResName.present()) {
			state std::vector<std::pair<int64_t, TenantMapEntry>> tenantsRes;
			state TenantMapEntry entryRes =
			    wait(self->setupTenant(cx, self->tenantResName.get(), self->tenantResGroupName));

			self->tenantRes = makeReference<Tenant>(cx, self->tenantResName.get());
			wait(self->tenantRes.get()->ready());

			tenantsRes.push_back({ self->tenantRes.get()->id(), entryRes });
			tenantResData.addTenants(tenantsRes);
		}

		KeyRange rangeRes = normalKeys;
		if (self->tenantRes.present()) {
			rangeRes = rangeRes.withPrefix(self->tenantRes.get()->prefix());
		}
		wait(store(self->bstoreRes, loadBStoreForTenant(&tenantResData, rangeRes)));
		ASSERT(self->bstoreRes.isValid());

		// Setup `self->listTenant` (no bstore needed)
		if (self->listTenantName.present()) {
			wait(success(self->setupTenant(cx, self->listTenantName.get(), self->listTenantGroupName)));
			self->listTenant = makeReference<Tenant>(cx, self->listTenantName.get());
			wait(self->listTenant.get()->ready());
		}

		TraceEvent("BulkLoadOpsSetupComplete")
		    .detail("UseTenant", self->tenant.present())
		    .detail("UseTenantRes", self->tenantRes.present());
		return Void();
	}

	std::string newKey() {
		if (sequential) {
			nextKey += sequentialGap;
			return format("%08x", nextKey);
		} else {
			return deterministicRandom()->randomUniqueID().toString();
		}
	}

	Future<Void> start(Database const& cx) override {
		unitClient = Future<Void>(Void());
		if (clientId == 0) {
			// sometimes don't do both unit tests and normal load tests
			if (deterministicRandom()->random01() < 0.9) {
				for (int i = 0; i < concurrentLoads; i++) {
					loadClients.push_back(bulkLoadClient(cx->clone(), this, i));
				}
			}

			if (deterministicRandom()->random01() < 0.8 || loadClients.empty()) {
				unitClient = bulkLoadUnitTests(cx->clone(), this);
			}
		}
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		client = Future<Void>();
		stopTest = true;
		if (clientId == 0) {
			return _check(cx, this);
		}
		return true;
	}

	ACTOR Future<bool> _check(Database cx, BulkLoadOpsWorkload* self) {
		if (deterministicRandom()->coinflip()) {
			cx->internal = IsInternal::False;
		}

		TraceEvent("BulkLoadOpsCheck").detail("ClientId", self->clientId).log();

		wait(waitForAll(self->loadClients));
		TraceEvent("BulkLoadOpsClientsComplete");

		wait(self->unitClient);
		TraceEvent("BulkLoadOpsUnitsComplete");

		Standalone<VectorRef<BulkLoadIDRef>> loadsNotCleanedUp = wait(cx->listAllBulkLoads(false, self->tenant));
		if (BLW_DEBUG) {
			fmt::print("Found {0} loads at end of test:\n", loadsNotCleanedUp.size());
			for (auto& it : loadsNotCleanedUp) {
				fmt::print("    {0}\n", it.printable());
			}
		}

		ASSERT(loadsNotCleanedUp.empty());

		TraceEvent("BulkLoadOpsQuietCheckComplete");

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Value> createOneFile(BulkLoadFileToWriteRef toWrite,
	                                  Standalone<StringRef> filePrefix,
	                                  int rowCount,
	                                  int valueSize,
	                                  int64_t* logicalSize,
	                                  Standalone<VectorRef<KeyValueRef>>* outData = nullptr) {
		state Standalone<GranuleSnapshot> snapshot;
		snapshot.reserve(snapshot.arena(), rowCount);
		for (int i = 0; i < rowCount; i++) {
			Key k = filePrefix.withSuffix(format("%08x", i));
			Value val = genVal(i, valueSize);
			snapshot.push_back_deep(snapshot.arena(), KeyValueRef(k, val));
			*logicalSize += k.size() + val.size();
		}
		if (outData) {
			outData->arena().dependsOn(snapshot.arena());
			outData->append(outData->arena(), snapshot.begin(), snapshot.size());
		}

		Value serialized = wait(serializeChunkedSnapshot(
		    toWrite.fileName, &snapshot, 16 * 1024, toWrite.compression, toWrite.cipherKeysCtx, true));
		return serialized;
	}

	// pair of logical, physical size
	ACTOR Future<std::pair<int64_t, int64_t>> writeOneFileToBlob(
	    BulkLoadOpsWorkload* self,
	    Reference<BlobConnectionProvider> bstore,
	    Optional<Reference<Tenant>> tenant,
	    BulkLoadFileToWriteRef toWrite,
	    Standalone<StringRef> filePrefix,
	    int rowCount,
	    int valueSize,
	    Standalone<VectorRef<KeyValueRef>>* outData = nullptr) {
		ASSERT(toWrite.type == BulkLoadFileType::SNAPSHOT);
		ASSERT(toWrite.format == BulkLoadFileFormat::ROW_V1);

		if (tenant.present()) {
			filePrefix = filePrefix.withPrefix(tenant.get()->prefix());
		}

		state int64_t logicalSize = 0;
		Value serialized = wait(self->createOneFile(toWrite, filePrefix, rowCount, valueSize, &logicalSize, outData));
		state int64_t physicalSize = serialized.size();

		std::string fname = toWrite.fileName.toString();
		// filename and volume is already predetermined, so just access it instead of doing createForWrite
		Reference<BackupContainerFileSystem> blobConn = bstore->getForAccess(fname);
		state std::string fileContents = serialized.toString();
		wait(blobConn->writeEntireFile(fname, fileContents));
		std::pair<int64_t, int64_t> result = { logicalSize, physicalSize };
		return result;
	}

	ACTOR Future<std::string> stageSingleFile(Database cx,
	                                          BulkLoadOpsWorkload* self,
	                                          Reference<BlobConnectionProvider> bstore,
	                                          BulkLoadID id,
	                                          KeyRange range,
	                                          Optional<Reference<Tenant>> tenant) {
		// sometimes request extra pointers
		int count = deterministicRandom()->randomInt(1, 3);
		state Standalone<VectorRef<BulkLoadFileToWriteRef>> filesToWrite =
		    wait(cx->getBulkLoadFilesToWrite(id, count, tenant));

		state Standalone<VectorRef<BulkLoadWrittenFileRef>> filesWritten;
		filesWritten.arena().dependsOn(filesToWrite.arena());

		// write tiny file
		std::pair<int64_t, int64_t> sizes =
		    wait(self->writeOneFileToBlob(self, bstore, tenant, filesToWrite[0], range.begin, 1, 32));

		BulkLoadWrittenFileRef writtenFile;
		writtenFile.fileId = filesToWrite[0].fileId;
		writtenFile.keyRange = range;
		writtenFile.logicalSize = sizes.first;
		writtenFile.physicalSize = sizes.second;

		filesWritten.push_back(filesWritten.arena(), writtenFile);

		wait(cx->markBulkLoadFilesWritten(id, filesWritten, tenant));

		if (deterministicRandom()->random01() < 0.1) {
			// re-registering the same file should be idempotent
			wait(cx->markBulkLoadFilesWritten(id, filesWritten, tenant));
		}

		return filesToWrite[0].fileName.toString();
	}

	ACTOR Future<Void> waitLoadComplete(Database cx, Optional<Reference<Tenant>> tenant, BulkLoadID id) {
		// currently because data isn't actually loaded we throw an error. we catch this error and act as if load
		// completed for now
		try {
			wait(cx->waitBulkLoadComplete(id, tenant));
			if (SERVER_KNOBS->BULK_LOAD_SS_MOVE) {
				return Void();
			}
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_api_not_implemented && !SERVER_KNOBS->BULK_LOAD_SS_MOVE) {
				return Void();
			}
			throw e;
		}
		return Void();
	}

	ACTOR Future<Void> doInjectAbort(Database cx,
	                                 BulkLoadOpsWorkload* self,
	                                 BulkLoadID id,
	                                 Future<Void> loadDone,
	                                 bool* injectedAbort,
	                                 bool* abortCompleted) {
		wait(delay(deterministicRandom()->random01() * 5) || loadDone);
		*injectedAbort = true;
		try {
			wait(cx->abortBulkLoad(id, self->tenant));
			*abortCompleted = true;
		} catch (Error& e) {
			if (e.code() == error_code_bulk_load_abort_failed) {
				// ensure load completes if abort failed
				wait(loadDone);
			} else {
				throw e;
			}
		}
		return Void();
	}

	ACTOR Future<Void> doWaitLoad(BulkLoadID id, Future<Void> loadF, const bool* injectedAbort, bool* loadComplete) {
		try {
			wait(loadF);
			*loadComplete = true;
		} catch (Error& e) {
			if (e.code() == error_code_bulk_load_aborted) {
				if (!*injectedAbort) {
					TraceEvent("BulkLoadAbortedBySystem").detail("BulkLoadID", id);
				}
				ASSERT(*injectedAbort);
			} else {
				throw e;
			}
		}
		return Void();
	}

	ACTOR Future<bool> waitOrInjectAbort(Database cx,
	                                     BulkLoadOpsWorkload* self,
	                                     BulkLoadID id,
	                                     KeyRange range,
	                                     bool injectAbort,
	                                     Standalone<VectorRef<KeyValueRef>>* checkData = nullptr) {
		state bool injectedAbort = false;
		state bool abortCompleted = false;
		state bool loadCompleted = false;

		self->maybeReopenTenant(cx);

		// wait for bulk load to finish, and maybe try to inject an abort while waiting
		// if we do inject an abort, we wait to verify that either:
		//  - bulk load completes and the abort injection gets bulk_load_abort_failed
		//  - abort injection completes and the bulk load gets bulk_load_aborted
		Future<Void> loadFuture = self->waitLoadComplete(cx, self->tenant, id);
		Future<Void> abortFuture = injectAbort
		                               ? self->doInjectAbort(cx, self, id, loadFuture, &injectedAbort, &abortCompleted)
		                               : Future<Void>(Void());
		wait(self->doWaitLoad(id, loadFuture, &injectedAbort, &loadCompleted) && abortFuture);

		// both load and abort cannot have successfully completed
		ASSERT(loadCompleted != abortCompleted);
		// but one of them must have completed
		ASSERT(loadCompleted || abortCompleted);

		if (BLW_DEBUG) {
			fmt::print("BLW {0} {1}: got done\n", range.begin.printable(), id.printable());
		}

		// Validate bulk load state at end.

		BulkLoadStatus status = wait(cx->getBulkLoadStatus(id, self->tenant));
		BulkLoadStatus::State current = status.state;

		if (BLW_DEBUG) {
			fmt::print(
			    "BLW {0} {1}: done status {2}\n", range.begin.printable(), id.printable(), static_cast<int>(current));
		}

		if (abortCompleted) {
			ASSERT(current == BulkLoadStatus::State::ABORTED || current == BulkLoadStatus::State::ABORTED_COMPLETE);
		} else if (!injectedAbort) {
			ASSERT(current == BulkLoadStatus::State::DONE);
		} else {
			ASSERT(current == BulkLoadStatus::State::DONE || current == BulkLoadStatus::State::ABORTED ||
			       current == BulkLoadStatus::State::ABORTED_COMPLETE);
		}

		return current == BulkLoadStatus::State::DONE;
	}

	ACTOR Future<Standalone<VectorRef<KeyValueRef>>> stageFilesForBulkLoad(Database cx,
	                                                                       BulkLoadOpsWorkload* self,
	                                                                       Reference<BlobConnectionProvider> bstore,
	                                                                       BulkLoadID id,
	                                                                       KeyRange range) {
		state int targetFiles = deterministicRandom()->randomInt(1, 11);

		state Standalone<VectorRef<BulkLoadFileToWriteRef>> filesToWrite;
		state int targetFilesToStage = targetFiles;

		state Standalone<VectorRef<KeyValueRef>> dataWritten;

		while (targetFilesToStage > 0) {
			int batchSize = deterministicRandom()->randomSkewedUInt32(1, targetFiles + 1);
			batchSize = std::min(targetFilesToStage, batchSize);
			targetFilesToStage -= batchSize;

			self->maybeReopenTenant(cx);

			Standalone<VectorRef<BulkLoadFileToWriteRef>> thisBatch =
			    wait(cx->getBulkLoadFilesToWrite(id, batchSize, self->tenant));
			filesToWrite.arena().dependsOn(thisBatch.arena());
			filesToWrite.append(filesToWrite.arena(), thisBatch.begin(), thisBatch.size());
		}

		ASSERT(filesToWrite.size() == targetFiles);

		state int targetFilesToWrite = targetFiles;
		state int prefix = 0;
		state int i;
		while (targetFilesToWrite > 0) {
			state int batchSize = deterministicRandom()->randomSkewedUInt32(1, targetFiles + 1);
			batchSize = std::min(targetFilesToWrite, batchSize);
			targetFilesToWrite -= batchSize;

			state Standalone<VectorRef<BulkLoadWrittenFileRef>> filesWritten;
			state std::vector<Future<std::pair<int64_t, int64_t>>> blobWrites;
			for (i = 0; i < batchSize; i++, prefix++) {
				// sometimes skip file to make non-contiguous
				if (filesWritten.empty() || deterministicRandom()->random01() < 0.8) {
					ASSERT_EQ(filesToWrite[prefix].tenantPrefix.present(), self->tenant.present());
					if (self->tenant.present()) {
						ASSERT_EQ(filesToWrite[prefix].tenantPrefix.get(), self->tenant.get()->prefix());
					}
					std::string filePrefixStr = std::to_string(prefix);
					Standalone<StringRef> filePrefix =
					    range.begin.withSuffix("/"_sr).withSuffix(filePrefixStr).withSuffix("/"_sr);
					KeyRange fileRange(KeyRangeRef(filePrefix, strinc(filePrefix)));

					filesWritten.arena().dependsOn(fileRange.arena());
					BulkLoadWrittenFileRef writtenFile;
					writtenFile.fileId = filesToWrite[prefix].fileId;
					writtenFile.keyRange = fileRange;
					writtenFile.physicalSize = -1;
					writtenFile.logicalSize = -1;

					filesWritten.push_back(filesWritten.arena(), writtenFile);
					// FIXME: actually use target value size sometimes
					blobWrites.push_back(self->writeOneFileToBlob(
					    self, bstore, self->tenant, filesToWrite[prefix], filePrefix, 10, 100, &dataWritten));
					wait(success(blobWrites.back()));
				}
			}

			wait(waitForAll(blobWrites));
			for (int i = 0; i < blobWrites.size(); i++) {
				std::tie(filesWritten[i].logicalSize, filesWritten[i].physicalSize) = blobWrites[i].get();
			}

			self->maybeReopenTenant(cx);

			wait(cx->markBulkLoadFilesWritten(id, filesWritten, self->tenant));
		}

		return dataWritten;
	}

	ACTOR Future<Void> verifyLoadData(Database cx,
	                                  BulkLoadOpsWorkload* self,
	                                  Reference<BlobConnectionProvider> bstore,
	                                  Optional<Reference<Tenant>> tenant,
	                                  KeyRange range,
	                                  Standalone<VectorRef<KeyValueRef>>* outData) {
		ASSERT(!outData->empty());

		state Version grv = wait(self->doGRV(cx));

		try {
			state Optional<Key> tenantPrefix;
			if (tenant.present()) {
				wait(tenant.get()->ready());
				tenantPrefix = tenant.get()->prefix();
				fmt::print("DBG: Found tenant: {0} for [{1} - {2})\n",
				           tenantPrefix.get(),
				           range.begin.printable(),
				           range.end.printable());
			}

			bool readDirty = !SERVER_KNOBS->BULK_LOAD_SS_MOVE;
			std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> result =
			    wait(readFromBlob(cx, bstore, range, 0, grv, tenant, LockAware::False, false, readDirty));

			ASSERT_EQ(outData->size(), result.first.size());
			for (int i = 0; i < result.first.size(); i++) {
				Key expectedKey = (*outData)[i].key;
				if (tenantPrefix.present()) {
					expectedKey = expectedKey.removePrefix(tenantPrefix.get());
				}
				ASSERT_EQ(expectedKey, result.first[i].key);
				ASSERT_EQ((*outData)[i].value, result.first[i].value);
			}
		} catch (Error& e) {
			if (e.code() == error_code_blob_granule_transaction_too_old) {
				TraceEvent(SevError, "BulkLoadVerifyDataMissingHybrid").detail("Range", range);
			}
			throw e;
		}

		if (!SERVER_KNOBS->BULK_LOAD_SS_MOVE) {
			return Void();
		}

		// also check SS data
		state Transaction tr = Transaction::fromTenantOrDefault(self->tenant, cx);
		loop {
			try {
				RangeReadResult ssRows = wait(tr.getRange(range, 1000000));
				ASSERT(!ssRows.more);

				ASSERT_EQ(outData->size(), ssRows.size());
				for (int i = 0; i < ssRows.size(); i++) {
					Key expectedKey = (*outData)[i].key;
					if (tenantPrefix.present()) {
						expectedKey = expectedKey.removePrefix(tenantPrefix.get());
					}
					ASSERT_EQ(expectedKey, ssRows[i].key);
					ASSERT_EQ((*outData)[i].value, ssRows[i].value);
				}

				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	ACTOR Future<Void> doOneBulkLoad(Database cx, BulkLoadOpsWorkload* self, KeyRange range, bool injectAbort) {
		if (BLW_DEBUG) {
			fmt::print("BLW {0}: starting\n", range.begin.printable());
		}
		// set up bulk load
		self->maybeReopenTenant(cx);
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		if (BLW_DEBUG) {
			fmt::print("BLW {0} {1}: started\n", range.begin.printable(), id.printable());
		}

		state Standalone<VectorRef<KeyValueRef>> dataWritten =
		    wait(self->stageFilesForBulkLoad(cx, self, self->bstore, id, range));

		// FIXME: also do aborting during staging/all files registered. already handled by unit tests some

		self->maybeReopenTenant(cx);
		wait(cx->bulkLoadAllFilesRegistered(id, self->tenant));

		if (BLW_DEBUG) {
			fmt::print(
			    "BLW: {0} {1}: registered {2} rows\n", range.begin.printable(), id.printable(), dataWritten.size());
		}

		bool success = wait(self->waitOrInjectAbort(cx, self, id, range, injectAbort, &dataWritten));
		CODE_PROBE(success, "WaitOrInjectAbort succeeded");
		CODE_PROBE(!success, "WaitOrInjectAbort aborted");

		if (success && SERVER_KNOBS->BULK_LOAD_STAGE_GRANULES) {
			if (BLW_DEBUG) {
				fmt::print("BLW: {0} {1}: verifying data\n", range.begin.printable(), id.printable());
			}
			wait(self->verifyLoadData(cx, self, self->bstore, self->tenant, range, &dataWritten));
			if (BLW_DEBUG) {
				fmt::print("BLW: {0} {1}: verified data\n", range.begin.printable(), id.printable());
			}
		}

		if (BLW_DEBUG) {
			fmt::print("BLW {0} {1}: cleaning up\n", range.begin.printable(), id.printable());
		}
		// FIXME: once auto-cleanup implemented, sometimes don't clean up, especially if aborted, and ensure auto clean
		// up gets it
		self->maybeReopenTenant(cx);
		wait(cx->cleanupBulkLoad(id, self->tenant));

		// after load either completed or aborted, make sure we can write and read to range
		if (BLW_DEBUG) {
			fmt::print("BLW {0} {1}: write + read after load\n", range.begin.printable(), id.printable());
		}
		state Transaction tr = Transaction::fromTenantOrDefault(self->tenant, cx);
		loop {
			try {
				wait(success(tr.get(range.begin)));
				tr.set(range.begin, "abcd"_sr);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		if (BLW_DEBUG) {
			fmt::print("BLW {0} {1}: cleaned up\n", range.begin.printable(), id.printable());
		}

		return Void();
	}

	ACTOR Future<Void> bulkLoadClient(Database cx, BulkLoadOpsWorkload* self, int clientIdx) {
		if (deterministicRandom()->coinflip()) {
			cx->internal = IsInternal::False;
		}

		state bool completedAny = false;
		if (BLW_DEBUG) {
			fmt::print("BLW: client {0} workload starting\n", clientIdx);
		}
		loop {
			if (completedAny && self->stopTest) {
				if (BLW_DEBUG) {
					fmt::print("BLW: client {0} workload done\n", clientIdx);
				}
				return Void();
			}

			std::string nextRangeKey = "R_" + self->newKey();
			state KeyRange range(KeyRangeRef(StringRef(nextRangeKey), strinc(StringRef(nextRangeKey))));
			bool injectAbort = deterministicRandom()->random01() < 0.2;
			try {
				fmt::print("BLW {0}: starting one bulk load (client {1})\n", range.begin.printable(), clientIdx);
				wait(self->doOneBulkLoad(cx, self, range, injectAbort));
				fmt::print("BLW {0}: completed one bulk load (client {1})\n", range.begin.printable(), clientIdx);
			} catch (Error& e) {
				if (BLW_DEBUG) {
					fmt::print("BLW {0}: unexpected error {1}\n", range.begin.printable(), e.name());
				}
				throw e;
			}

			completedAny = true;
		}
	}

	enum UnitTestTypes {
		OVERLAPPING_LOADS_UNIT,
		DONE_THEN_ABORT_UNIT,
		ABORT_THEN_DONE_UNIT,
		EARLY_CLEANUP_UNIT,
		INVALID_LOAD_ID_UNIT,
		WRITE_DURING_LOAD_UNIT,
		READ_DURING_LOAD_UNIT,
		LIST_DURING_LOAD_UNIT,
		LIST_ALL_DURING_LOAD_UNIT,
		STATUS_DURING_LOAD_UNIT,
		SANITY_CHECK_VERSIONSTAMP_UNIT,
		NON_EMPTY_RANGE_UNIT,
		ALREADY_BLOB_RANGE_UNIT,
		BLOBBIFY_DURING_LOAD_UNIT,
		REGISTER_NO_FILES_UNIT,
		DUPLICATE_FILES_SAME_REGISTER_UNIT,
		DUPLICATE_FILES_MULTIPLE_REGISTER_UNIT,
		OVERLAPPING_FILES_SAME_REGISTER_UNIT,
		OVERLAPPING_FILES_MULTIPLE_REGISTER_UNIT,
		REGISTER_FILES_ALREADY_LOADING_UNIT,
		REGISTER_NONEXISTENT_FILE_ID_UNIT,
		REGISTER_NONEXISTENT_FILE_UNIT,
		UNUSED_POINTER_CLEANUP_UNIT,
		ABORTED_POINTER_CLEANUP_UNIT,
		FILE_SIZE_MISMATCH_UNIT,
		RESERVATION_FAILED,
		RESERVATION_SUCCEEDED,
		ABORT_THEN_RELOAD_UNIT,
		BLOB_CLEANUP_ON_ABORT_UNIT,
		OP_COUNT = 29 /* keep this last */
	};

	// FIXME: once file contents validation isn't asserting and throws an error instead, add unit tests for invalid file
	// data
	ACTOR Future<Void> expectAbortedBySystem(Database cx,
	                                         BulkLoadOpsWorkload* self,
	                                         BulkLoadID id,
	                                         Optional<Reference<Tenant>> tenant) {
		try {
			// retry of all files registered might also throw bulk_load_aborted
			wait(cx->bulkLoadAllFilesRegistered(id, tenant));
			wait(cx->waitBulkLoadComplete(id, tenant));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_aborted);
		}
		wait(cx->cleanupBulkLoad(id, tenant));
		return Void();
	}

	ACTOR Future<Void> abortAndCleanupAfterUnit(Database cx, BulkLoadID id, Optional<Reference<Tenant>> tenant) {
		wait(cx->abortBulkLoad(id, tenant));
		wait(cx->cleanupBulkLoad(id, tenant));
		return Void();
	}

	ACTOR Future<Void> delayCleanupWindow(Database cx, BulkLoadOpsWorkload* self) {
		state Version waitVersion = wait(self->doGRV(cx));
		waitVersion += CLIENT_KNOBS->BULK_LOAD_MAX_CREATE_VERSION_DIFFERENCE;
		loop {
			wait(delay(1.0));
			Version newV = wait(self->doGRV(cx));
			if (newV >= waitVersion) {
				return Void();
			}
		}
	}

	ACTOR Future<Void> overlappingLoadsUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that client cannot start a bulk load that overlaps with an active one
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		try {
			wait(success(cx->startBulkLoad(range, self->tenant)));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_range_conflict);
		}

		// wait to clean up until after any retransmits or retries of the second failed bulk load cannot proceed
		wait(self->delayCleanupWindow(cx, self));

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));

		return Void();
	}

	ACTOR Future<Void> doneThenAbortUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that client cannot abort an already completed bulk load
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		wait(success(self->stageSingleFile(cx, self, self->bstore, id, range, self->tenant)));
		wait(cx->bulkLoadAllFilesRegistered(id, self->tenant));
		wait(self->waitLoadComplete(cx, self->tenant, id));

		try {
			wait(cx->abortBulkLoad(id, self->tenant));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_abort_failed);
		}

		// cleanup
		wait(cx->cleanupBulkLoad(id, self->tenant));

		return Void();
	}

	ACTOR Future<Void> abortThenDoneUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that client cannot mark an already aborted bulk load as complete
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		wait(cx->abortBulkLoad(id, self->tenant));

		try {
			wait(cx->updateBulkLoadState(id, BulkLoadStatus::State::DONE, self->tenant));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_aborted);
		}

		wait(cx->cleanupBulkLoad(id, self->tenant));

		return Void();
	}

	ACTOR Future<Void> earlyCleanupUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that client cannot clean up a bulk load before it is done or aborted
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		try {
			wait(cx->cleanupBulkLoad(id, self->tenant));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_cleanup_failed);
		}

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));

		return Void();
	}

	ACTOR Future<Void> checkCorrectInvalidIDError(Future<Void> f) {
		try {
			wait(success(f));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_not_found);
		}
		return Void();
	}

	ACTOR Future<Void> invalidLoadIDUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that client accessing invalid bulk load id gets expected error
		state BulkLoadID id = deterministicRandom()->randomUniqueID().toString();
		wait(self->checkCorrectInvalidIDError(cx->bulkLoadAllFilesRegistered(id, self->tenant)));
		wait(self->checkCorrectInvalidIDError(cx->abortBulkLoad(id, self->tenant)));
		wait(self->checkCorrectInvalidIDError(cx->waitBulkLoadComplete(id, self->tenant)));
		// don't test cleanup because it's idempotent if it was already cleaned up
		wait(self->checkCorrectInvalidIDError(
		    cx->waitBulkLoadStateAtLeast(id, BulkLoadStatus::State::STAGING, self->tenant)));
		wait(self->checkCorrectInvalidIDError(
		    cx->updateBulkLoadState(id, BulkLoadStatus::State::STAGING, self->tenant)));
		wait(self->checkCorrectInvalidIDError(success(cx->getBulkLoadStatus(id, self->tenant))));
		wait(self->checkCorrectInvalidIDError(success(cx->getBulkLoadVersion(id, self->tenant))));
		wait(self->checkCorrectInvalidIDError(success(cx->getBulkLoadFilesToWrite(id, 1, self->tenant))));

		Standalone<VectorRef<BulkLoadWrittenFileRef>> filesWritten;
		BulkLoadWrittenFileRef writtenFile;
		writtenFile.fileId = "file_id"_sr;
		writtenFile.keyRange = range;
		writtenFile.physicalSize = 1;
		writtenFile.logicalSize = 1;
		filesWritten.push_back(filesWritten.arena(), writtenFile);
		wait(self->checkCorrectInvalidIDError(success(cx->markBulkLoadFilesWritten(id, filesWritten, self->tenant))));

		return Void();
	}

	ACTOR Future<Void> writeDuringLoadUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that clients cannot write to the range of an active bulk load
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		state Transaction tr = Transaction::fromTenantOrDefault(self->tenant, cx);
		try {
			loop {
				try {
					tr.set(range.begin, "abcd"_sr);
					wait(tr.commit());
					ASSERT(false);
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		} catch (Error& e) {
			ASSERT(e.code() == error_code_illegal_locked_range_access);
		}

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));

		// make sure we can write after load aborted
		tr.reset();
		loop {
			try {
				tr.set(range.begin, "abcd"_sr);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	ACTOR Future<Void> readDuringLoadUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that clients cannot read from the range of an active bulk load
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		state Transaction tr(cx);
		try {
			loop {
				try {
					wait(success(tr.get(range.begin)));
					ASSERT(false);
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		} catch (Error& e) {
			// TODO: same error or new one?
			ASSERT(e.code() == error_code_bulk_load_range_conflict);
		}

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));

		return Void();
	}

	ACTOR Future<Void> listDuringLoadUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that the active bulk load is correctly listed
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->listTenant));
		state Standalone<VectorRef<BulkLoadIDRef>> list = wait(cx->listBulkLoads(true, self->listTenant));
		state Standalone<VectorRef<BulkLoadIDRef>> otherTenantList = wait(cx->listBulkLoads(true, self->tenant));

		ASSERT(list.size() == 1);
		ASSERT(std::find(list.begin(), list.end(), id) != std::end(list));
		ASSERT(std::find(otherTenantList.begin(), otherTenantList.end(), id) == std::end(otherTenantList));

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, id, self->listTenant));

		wait(store(list, cx->listBulkLoads(true, self->listTenant)));
		ASSERT(list.size() == 0);

		return Void();
	}

	ACTOR Future<Void> listAllDuringLoadUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that the all bulk loads are correctly listed
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		state BulkLoadID listId = wait(cx->startBulkLoad(range, self->listTenant));
		state Standalone<VectorRef<BulkLoadIDRef>> list = wait(cx->listAllBulkLoads(true, self->tenant));

		ASSERT(std::find(list.begin(), list.end(), id) != std::end(list));
		ASSERT(std::find(list.begin(), list.end(), listId) != std::end(list));
		ASSERT(list.size() >= 2);

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));
		wait(self->abortAndCleanupAfterUnit(cx, listId, self->listTenant));

		return Void();
	}

	ACTOR Future<Void> statusDuringLoadUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that the active bulk load has a valid status
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		state BulkLoadStatus status = wait(cx->getBulkLoadStatus(id, self->tenant));
		ASSERT(status.state != BulkLoadStatus::State::UNKNOWN);

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));

		return Void();
	}

	ACTOR Future<Void> sanityCheckVersionstampUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that client cannot clean up a bulk load before it is done or aborted

		// since we can't access the transaction object used to start the bulk load and validate that it is exactly the
		// same version, we just do a GRV before and after and make sure it matches
		state Version start = wait(self->doGRV(cx));
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		state Version end = wait(self->doGRV(cx));

		Version createVersion = wait(cx->getBulkLoadVersion(id, self->tenant));

		ASSERT(createVersion >= start);
		ASSERT(createVersion <= end);

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));

		return Void();
	}

	ACTOR Future<Void> nonEmptyRangeUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that a bulk load cannot start to a non-empty range
		state Transaction tr = Transaction::fromTenantOrDefault(self->tenant, cx);
		loop {
			try {
				tr.set(range.begin, "foo"_sr);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		try {
			wait(success(cx->startBulkLoad(range, self->tenant)));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_range_not_empty);
		}

		return Void();
	}

	ACTOR Future<Void> alreadyBlobRangeUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that a bulk load cannot start if a blobbified range already overlaps the start range
		bool success1 = wait(cx->blobbifyRange(range, self->tenant));
		ASSERT(success1);

		try {
			wait(success(cx->startBulkLoad(range, self->tenant)));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_range_not_empty);
		}

		return Void();
	}

	ACTOR Future<Void> blobbifyDuringLoadUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		// Test that clients cannot blobbify a range intersecting with an active bulk load
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		try {
			wait(success(cx->blobbifyRange(range, self->tenant)));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_illegal_locked_range_access);
		}

		// cleanup
		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));

		// make sure we can blobbify after unit
		bool successful = wait(cx->blobbifyRange(range, self->tenant));
		ASSERT(successful);

		return Void();
	}

	ACTOR Future<Void> registerNoFilesUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		// maybe request file pointers or not
		if (deterministicRandom()->coinflip()) {
			wait(success(cx->getBulkLoadFilesToWrite(id, 1, self->tenant)));
		}

		wait(self->expectAbortedBySystem(cx, self, id, self->tenant));
		return Void();
	}

	ACTOR Future<Void> duplicateFilesSameRegisterUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		Standalone<VectorRef<BulkLoadFileToWriteRef>> filesToWrite =
		    wait(cx->getBulkLoadFilesToWrite(id, 2, self->tenant));

		Standalone<VectorRef<BulkLoadWrittenFileRef>> filesWritten;
		filesWritten.arena().dependsOn(filesToWrite.arena());
		KeyRange range1 = singleKeyRange(range.begin.withSuffix("a"_sr));
		KeyRange range2 = singleKeyRange(range.begin.withSuffix("b"_sr));
		filesWritten.arena().dependsOn(range1.arena());
		filesWritten.arena().dependsOn(range2.arena());

		BulkLoadWrittenFileRef writtenFile1;
		writtenFile1.fileId = filesToWrite[0].fileId;
		writtenFile1.keyRange = range1;
		writtenFile1.physicalSize = deterministicRandom()->randomInt(1, 10000000);
		writtenFile1.logicalSize = deterministicRandom()->randomInt(1, 10000000);

		filesWritten.push_back(filesWritten.arena(), writtenFile1);

		BulkLoadWrittenFileRef writtenFile2;
		writtenFile2.fileId = filesToWrite[0].fileId; // same id as other file!
		writtenFile2.keyRange = range2;
		writtenFile2.physicalSize = deterministicRandom()->randomInt(1, 10000000);
		writtenFile2.logicalSize = deterministicRandom()->randomInt(1, 10000000);

		filesWritten.push_back(filesWritten.arena(), writtenFile2);

		try {
			wait(cx->markBulkLoadFilesWritten(id, filesWritten, self->tenant));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_invalid_file_metadata);
		}

		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));
		return Void();
	}

	ACTOR Future<Void> duplicateFilesMultipleRegisterUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		wait(success(self->stageSingleFile(cx, self, self->bstore, id, range, self->tenant)));
		wait(success(self->stageSingleFile(cx, self, self->bstore, id, range, self->tenant)));

		wait(self->expectAbortedBySystem(cx, self, id, self->tenant));
		return Void();
	}

	ACTOR Future<Void> overlappingFilesSameRegisterUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		Standalone<VectorRef<BulkLoadFileToWriteRef>> filesToWrite =
		    wait(cx->getBulkLoadFilesToWrite(id, 2, self->tenant));

		Standalone<VectorRef<BulkLoadWrittenFileRef>> filesWritten;
		filesWritten.arena().dependsOn(filesToWrite.arena());

		BulkLoadWrittenFileRef writtenFile1;
		writtenFile1.fileId = filesToWrite[0].fileId;
		writtenFile1.keyRange = range;
		writtenFile1.physicalSize = deterministicRandom()->randomInt(1, 10000000);
		writtenFile1.logicalSize = deterministicRandom()->randomInt(1, 10000000);

		filesWritten.push_back(filesWritten.arena(), writtenFile1);

		BulkLoadWrittenFileRef writtenFile2;
		writtenFile2.fileId = filesToWrite[1].fileId;
		writtenFile2.keyRange = range; // same range as other file
		writtenFile2.physicalSize = deterministicRandom()->randomInt(1, 10000000);
		writtenFile2.logicalSize = deterministicRandom()->randomInt(1, 10000000);

		filesWritten.push_back(filesWritten.arena(), writtenFile2);

		try {
			wait(cx->markBulkLoadFilesWritten(id, filesWritten, self->tenant));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_invalid_file_metadata);
		}

		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));
		return Void();
	}

	ACTOR Future<Void> overlappingFilesMultipleRegisterUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		wait(success(self->stageSingleFile(cx, self, self->bstore, id, range, self->tenant)));
		wait(success(self->stageSingleFile(cx, self, self->bstore, id, range, self->tenant)));

		wait(self->expectAbortedBySystem(cx, self, id, self->tenant));
		return Void();
	}

	ACTOR Future<Void> registerFilesAlreadyLoadingUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		wait(success(self->stageSingleFile(cx, self, self->bstore, id, range, self->tenant)));
		wait(cx->bulkLoadAllFilesRegistered(id, self->tenant));

		try {
			wait(success(cx->getBulkLoadFilesToWrite(id, 1, self->tenant)));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_not_registering_files);
		}

		Standalone<VectorRef<BulkLoadWrittenFileRef>> filesWritten;
		BulkLoadWrittenFileRef writtenFile;
		writtenFile.fileId = "invalid_file_id"_sr;
		writtenFile.keyRange = range;
		writtenFile.physicalSize = deterministicRandom()->randomInt(1, 10000000);
		writtenFile.logicalSize = deterministicRandom()->randomInt(1, 10000000);

		filesWritten.push_back(filesWritten.arena(), writtenFile);

		try {
			wait(cx->markBulkLoadFilesWritten(id, filesWritten, self->tenant));
			ASSERT(false);
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_not_registering_files);
		}

		wait(self->waitLoadComplete(cx, self->tenant, id));
		wait(cx->cleanupBulkLoad(id, self->tenant));

		return Void();
	}

	ACTOR Future<Void> registerNonexistentFileIdUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		Standalone<VectorRef<BulkLoadWrittenFileRef>> filesWritten;
		BulkLoadWrittenFileRef writtenFile;
		writtenFile.fileId = "invalid_file_id"_sr;
		writtenFile.keyRange = range;
		writtenFile.physicalSize = deterministicRandom()->randomInt(1, 10000000);
		writtenFile.logicalSize = deterministicRandom()->randomInt(1, 10000000);

		filesWritten.push_back(filesWritten.arena(), writtenFile);

		try {
			wait(cx->markBulkLoadFilesWritten(id, filesWritten, self->tenant));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_file_id_not_found);
		}

		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));
		return Void();
	}

	ACTOR Future<Void> expectFileDeleted(BulkLoadOpsWorkload* self,
	                                     Reference<BlobConnectionProvider> bstore,
	                                     std::string filename,
	                                     bool shouldBeDeleted) {
		try {
			state Reference<BackupContainerFileSystem> blobConn = bstore->getForAccess(filename);
			state Reference<IAsyncFile> reader = wait(blobConn->readFile(filename));
			// read 1 byte from file to make sure it exists
			state uint8_t data;
			int readSize = wait(reader->read(&data, 1, 0));
			ASSERT(!shouldBeDeleted);
		} catch (Error& e) {
			ASSERT(shouldBeDeleted);
			ASSERT(e.code() == error_code_file_not_found);
		}
		return Void();
	}

	ACTOR Future<Void> registerNonexistentFileUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		state Standalone<VectorRef<BulkLoadFileToWriteRef>> filesToWrite =
		    wait(cx->getBulkLoadFilesToWrite(id, 1, self->tenant));

		state Standalone<VectorRef<BulkLoadWrittenFileRef>> filesWritten;
		filesWritten.arena().dependsOn(filesToWrite.arena());

		// don't actually write file to blob!

		BulkLoadWrittenFileRef writtenFile;
		writtenFile.fileId = filesToWrite[0].fileId;
		writtenFile.keyRange = range;
		writtenFile.physicalSize = deterministicRandom()->randomInt(1, 10000000);
		writtenFile.logicalSize = deterministicRandom()->randomInt(1, 10000000);

		filesWritten.push_back(filesWritten.arena(), writtenFile);

		try {
			wait(cx->markBulkLoadFilesWritten(id, filesWritten, self->tenant));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_file_missing);
		}

		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));
		return Void();
	}

	ACTOR Future<Void> unusedPointerCleanupUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		state std::string stagedFile =
		    wait(self->stageSingleFile(cx,
		                               self,
		                               self->bstore,
		                               id,
		                               KeyRangeRef(range.begin.withSuffix("a"_sr), range.begin.withSuffix("b"_sr)),
		                               self->tenant));

		state Standalone<VectorRef<BulkLoadFileToWriteRef>> filesToWrite =
		    wait(cx->getBulkLoadFilesToWrite(id, 1, self->tenant));

		// write file but DON'T register it
		wait(success(self->writeOneFileToBlob(
		    self, self->bstore, self->tenant, filesToWrite[0], range.begin.withSuffix("b"_sr), 1, 32)));

		wait(cx->bulkLoadAllFilesRegistered(id, self->tenant));
		wait(self->waitLoadComplete(cx, self->tenant, id));

		wait(self->expectFileDeleted(self, self->bstore, filesToWrite[0].fileName.toString(), true));
		// FIXME: once load actually keeps granules this needs to be inverted, we should check that the file is kept
		wait(self->expectFileDeleted(self, self->bstore, stagedFile, !SERVER_KNOBS->BULK_LOAD_STAGE_GRANULES));

		// cleanup
		wait(cx->cleanupBulkLoad(id, self->tenant));
		return Void();
	}

	ACTOR Future<Void> abortedPointerCleanupUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		state std::string stagedFile =
		    wait(self->stageSingleFile(cx,
		                               self,
		                               self->bstore,
		                               id,
		                               KeyRangeRef(range.begin.withSuffix("a"_sr), range.begin.withSuffix("b"_sr)),
		                               self->tenant));

		state Standalone<VectorRef<BulkLoadFileToWriteRef>> filesToWrite =
		    wait(cx->getBulkLoadFilesToWrite(id, 1, self->tenant));

		// write file but DON'T register it
		wait(success(self->writeOneFileToBlob(
		    self, self->bstore, self->tenant, filesToWrite[0], range.begin.withSuffix("b"_sr), 1, 32)));

		wait(cx->abortBulkLoad(id, self->tenant));
		wait(self->expectAbortedBySystem(cx, self, id, self->tenant));

		wait(self->expectFileDeleted(self, self->bstore, stagedFile, true));
		wait(self->expectFileDeleted(self, self->bstore, filesToWrite[0].fileName.toString(), true));

		return Void();
	}

	ACTOR Future<Void> fileSizeMismatchUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));

		state Standalone<VectorRef<BulkLoadFileToWriteRef>> filesToWrite =
		    wait(cx->getBulkLoadFilesToWrite(id, 1, self->tenant));

		state Standalone<VectorRef<BulkLoadWrittenFileRef>> filesWritten;
		filesWritten.arena().dependsOn(filesToWrite.arena());

		// write tiny file to blob
		std::pair<int64_t, int64_t> sizes =
		    wait(self->writeOneFileToBlob(self, self->bstore, self->tenant, filesToWrite[0], range.begin, 1, 32));

		BulkLoadWrittenFileRef writtenFile;
		writtenFile.fileId = filesToWrite[0].fileId;
		writtenFile.keyRange = range;
		writtenFile.logicalSize = sizes.first;
		writtenFile.physicalSize = sizes.second + 1; // intentionally incorrect!!

		filesWritten.push_back(filesWritten.arena(), writtenFile);

		try {
			wait(cx->markBulkLoadFilesWritten(id, filesWritten, self->tenant));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_invalid_file_metadata);
		}

		wait(self->abortAndCleanupAfterUnit(cx, id, self->tenant));
		return Void();
	}

	static Future<Void> setStorageQuota(Database cx, TenantGroupName tenantGroupName, int64_t quota) {
		if (BLW_DEBUG) {
			fmt::print("BLW Set Quota {} for TenantGroup {}\n", quota, tenantGroupName);
		}
		return runRYWTransactionVoid(cx,
		                             [tenantGroupName = tenantGroupName,
		                              quota = quota](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			                             tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			                             TenantMetadata::storageQuota().set(tr, tenantGroupName, quota);
			                             return Void();
		                             });
	}

	static Future<Void> clearStorageQuota(Database cx, TenantGroupName tenantGroupName) {
		return runRYWTransactionVoid(
		    cx, [tenantGroupName = tenantGroupName](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			    TenantMetadata::storageQuota().erase(tr, tenantGroupName);
			    return Void();
		    });
	}

	static Future<Optional<int64_t>> getStorageQuota(Database cx, TenantGroupName tenantGroupName) {
		return runRYWTransaction(cx, [tenantGroupName = tenantGroupName](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			return TenantMetadata::storageQuota().get(tr, tenantGroupName);
		});
	}

	ACTOR Future<Void> reservationFailed(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		if (!self->tenantRes.present() || !self->tenantResGroupName.present()) {
			return Void();
		}
		// Set storage quota to 0 to ensure any reservation will fail
		wait(setStorageQuota(cx, self->tenantResGroupName.get(), 1));

		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenantRes));
		wait(success(self->stageSingleFile(cx, self, self->bstoreRes, id, range, self->tenantRes)));

		wait(self->expectAbortedBySystem(cx, self, id, self->tenantRes));

		// cleanup
		wait(cx->cleanupBulkLoad(id, self->tenantRes));
		wait(clearStorageQuota(cx, self->tenantResGroupName.get()));
		return Void();
	}

	ACTOR Future<Void> reservationSucceeded(Database cx,
	                                        BulkLoadOpsWorkload* self,
	                                        KeyRange range,
	                                        KeyRange rangeSecond) {
		if (!self->tenantRes.present() || !self->tenantResGroupName.present()) {
			return Void();
		}

		state BulkLoadID id1 = wait(cx->startBulkLoad(range, self->tenantRes));

		state Standalone<VectorRef<BulkLoadFileToWriteRef>> filesToWrite =
		    wait(cx->getBulkLoadFilesToWrite(id1, 1, self->tenantRes));

		state Standalone<VectorRef<BulkLoadWrittenFileRef>> filesWritten;
		filesWritten.arena().dependsOn(filesToWrite.arena());

		// write tiny file to blob
		std::pair<int64_t, int64_t> sizes =
		    wait(self->writeOneFileToBlob(self, self->bstoreRes, self->tenantRes, filesToWrite[0], range.begin, 1, 32));

		state BulkLoadWrittenFileRef writtenFile;
		writtenFile.fileId = filesToWrite[0].fileId;
		writtenFile.keyRange = range;
		writtenFile.logicalSize = sizes.first;
		writtenFile.physicalSize = sizes.second;

		filesWritten.push_back(filesWritten.arena(), writtenFile);

		wait(setStorageQuota(cx, self->tenantResGroupName.get(), writtenFile.logicalSize));

		// Test the base case.
		// The quota is the same as the size being written, this should succeed.
		wait(cx->markBulkLoadFilesWritten(id1, filesWritten, self->tenantRes));
		wait(cx->bulkLoadAllFilesRegistered(id1, self->tenantRes));
		wait(self->waitLoadComplete(cx, self->tenantRes, id1));
		wait(cx->cleanupBulkLoad(id1, self->tenantRes));

		// Test that reservations are cleared correctly.
		// Another bulk load on the same tenant group should go through since the previous reservation
		// should have been cleared as the bulk load completed.
		state BulkLoadID id2 = wait(cx->startBulkLoad(rangeSecond, self->tenantRes));
		wait(success(self->stageSingleFile(cx, self, self->bstoreRes, id2, rangeSecond, self->tenantRes)));
		wait(cx->bulkLoadAllFilesRegistered(id2, self->tenantRes));
		wait(self->waitLoadComplete(cx, self->tenantRes, id2));
		wait(cx->cleanupBulkLoad(id2, self->tenantRes));

		wait(clearStorageQuota(cx, self->tenantResGroupName.get()));
		return Void();
	}

	ACTOR Future<Void> abortThenReloadUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		wait(cx->abortBulkLoad(id, self->tenant));
		wait(cx->cleanupBulkLoad(id, self->tenant));

		wait(self->doOneBulkLoad(cx, self, range, false));

		return Void();
	}

	ACTOR Future<Void> blobCleanupOnAbortUnit(Database cx, BulkLoadOpsWorkload* self, KeyRange range) {
		state BulkLoadID id = wait(cx->startBulkLoad(range, self->tenant));
		state int numFiles = deterministicRandom()->randomInt(1, 6);
		state int i;
		for (i = 0; i < numFiles; i++) {
			std::string suffixStart = std::to_string(i);
			std::string suffixEnd = std::to_string(i + 1);
			wait(success(self->stageSingleFile(
			    cx,
			    self,
			    self->bstore,
			    id,
			    KeyRangeRef(range.begin.withSuffix(suffixStart), range.begin.withSuffix(suffixEnd)),
			    self->tenant)));
		}

		wait(cx->bulkLoadAllFilesRegistered(id, self->tenant));

		// wait a bit, and/or wait for a certain bulk load state
		if (deterministicRandom()->coinflip()) {
			BulkLoadStatus::State stateToWait = deterministicRandom()->coinflip()
			                                        ? BulkLoadStatus::State::STAGING_BLOB_VALIDATED
			                                        : BulkLoadStatus::State::STAGING_BLOB_DONE;
			wait(cx->waitBulkLoadStateAtLeast(id, stateToWait, self->tenant));
		}

		if (deterministicRandom()->coinflip()) {
			wait(delayJittered(deterministicRandom()->randomSkewedUInt32(1, 5000) / 1000.0));
		}

		// abort load
		try {
			wait(cx->abortBulkLoad(id, self->tenant));
		} catch (Error& e) {
			ASSERT(e.code() == error_code_bulk_load_abort_failed);
			wait(cx->cleanupBulkLoad(id, self->tenant));
			return Void();
		}

		wait(cx->cleanupBulkLoad(id, self->tenant));

		CODE_PROBE(true, "blob cleanup unit test successfully aborted load");

		// verify no granules, blobbified ranges, or change feeds that intersect this range
		state KeyRange physicalRange = range;
		if (self->tenant.present()) {
			wait(self->tenant.get()->ready());
			physicalRange = physicalRange.withPrefix(self->tenant.get()->prefix());
		}
		Standalone<VectorRef<KeyRangeRef>> blobbifiedRanges = wait(cx->listBlobbifiedRanges(physicalRange, 1));
		if (!blobbifiedRanges.empty()) {
			fmt::print("ERROR: Ranges for [{0} - {1}) not empty! ({2})\n",
			           physicalRange.begin.printable(),
			           physicalRange.end.printable(),
			           blobbifiedRanges.size());
			for (auto& it : blobbifiedRanges) {
				fmt::print("    {0} - {1}\n", it.begin.printable(), it.end.printable());
			}
		}
		ASSERT(blobbifiedRanges.empty());

		state Transaction tr(cx);
		loop {
			try {
				Standalone<VectorRef<KeyRangeRef>> granules = wait(tr.getBlobGranuleRanges(physicalRange, 1));
				ASSERT(granules.empty());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		std::vector<std::pair<Key, KeyRange>> activeFeeds = wait(getActiveFeeds(&tr));
		for (auto& it : activeFeeds) {
			ASSERT(!physicalRange.intersects(it.second));
		}

		return Void();
	}

	ACTOR Future<Void> bulkLoadUnitTests(Database cx, BulkLoadOpsWorkload* self) {
		if (deterministicRandom()->coinflip()) {
			cx->internal = IsInternal::False;
		}

		loop {
			if (self->stopTest) {
				return Void();
			}
			std::set<UnitTestTypes> excludedTypes;
			excludedTypes.insert(OP_COUNT);

			std::string nextRangeKey = "U_" + self->newKey();
			state KeyRange range(KeyRangeRef(StringRef(nextRangeKey), strinc(StringRef(nextRangeKey))));
			nextRangeKey = "U_" + self->newKey();
			state KeyRange rangeSecond(KeyRangeRef(StringRef(nextRangeKey), strinc(StringRef(nextRangeKey))));

			if (SERVER_KNOBS->BULK_LOAD_FILE_VALIDATION_LEVEL <= static_cast<int>(BulkLoadFileValidationLevel::NONE)) {
				// we skip validation here so it won't catch this case
				excludedTypes.insert(REGISTER_NONEXISTENT_FILE_UNIT);
				excludedTypes.insert(FILE_SIZE_MISMATCH_UNIT);
			}
			if (!CLIENT_KNOBS->BULK_LOAD_STORAGE_RESERVATION_ENABLED) {
				excludedTypes.insert(RESERVATION_SUCCEEDED);
				excludedTypes.insert(RESERVATION_FAILED);
			}

			// FIXME: no lock implemented yet so these succeed - unexclude when implemented!
			excludedTypes.insert(READ_DURING_LOAD_UNIT);

			if (!SERVER_KNOBS->BULK_LOAD_STAGE_GRANULES) {
				excludedTypes.insert(BLOB_CLEANUP_ON_ABORT_UNIT);
			}

			// TODO REMOVE! FIXME soon!
			if (SERVER_KNOBS->BULK_LOAD_STAGE_GRANULES) {
				excludedTypes.insert(BLOB_CLEANUP_ON_ABORT_UNIT);
				excludedTypes.insert(ABORT_THEN_RELOAD_UNIT);
			}

			// prevent infinite loop
			int loopTries = 1000;
			int op = OP_COUNT;
			loop {
				op = deterministicRandom()->randomInt(0, OP_COUNT);
				if (!excludedTypes.count((UnitTestTypes)op)) {
					break;
				}
				loopTries--;
				ASSERT(loopTries >= 0);
			}
			if (BLW_DEBUG) {
				fmt::print(
				    "Selected range [{0} - {1}) for unit {2}.\n", range.begin.printable(), range.end.printable(), op);
			}

			self->maybeReopenTenant(cx);

			if (op == OVERLAPPING_LOADS_UNIT) {
				wait(self->overlappingLoadsUnit(cx, self, range));
			} else if (op == DONE_THEN_ABORT_UNIT) {
				wait(self->doneThenAbortUnit(cx, self, range));
			} else if (op == ABORT_THEN_DONE_UNIT) {
				wait(self->abortThenDoneUnit(cx, self, range));
			} else if (op == EARLY_CLEANUP_UNIT) {
				wait(self->earlyCleanupUnit(cx, self, range));
			} else if (op == INVALID_LOAD_ID_UNIT) {
				wait(self->invalidLoadIDUnit(cx, self, range));
			} else if (op == WRITE_DURING_LOAD_UNIT) {
				wait(self->writeDuringLoadUnit(cx, self, range));
			} else if (op == READ_DURING_LOAD_UNIT) {
				wait(self->readDuringLoadUnit(cx, self, range));
			} else if (op == LIST_DURING_LOAD_UNIT) {
				wait(self->listDuringLoadUnit(cx, self, range));
			} else if (op == LIST_ALL_DURING_LOAD_UNIT) {
				wait(self->listAllDuringLoadUnit(cx, self, range));
			} else if (op == STATUS_DURING_LOAD_UNIT) {
				wait(self->statusDuringLoadUnit(cx, self, range));
			} else if (op == SANITY_CHECK_VERSIONSTAMP_UNIT) {
				wait(self->sanityCheckVersionstampUnit(cx, self, range));
			} else if (op == NON_EMPTY_RANGE_UNIT) {
				wait(self->nonEmptyRangeUnit(cx, self, range));
			} else if (op == ALREADY_BLOB_RANGE_UNIT) {
				wait(self->alreadyBlobRangeUnit(cx, self, range));
			} else if (op == BLOBBIFY_DURING_LOAD_UNIT) {
				wait(self->blobbifyDuringLoadUnit(cx, self, range));
			} else if (op == REGISTER_NO_FILES_UNIT) {
				wait(self->registerNoFilesUnit(cx, self, range));
			} else if (op == DUPLICATE_FILES_SAME_REGISTER_UNIT) {
				wait(self->duplicateFilesSameRegisterUnit(cx, self, range));
			} else if (op == DUPLICATE_FILES_MULTIPLE_REGISTER_UNIT) {
				wait(self->duplicateFilesMultipleRegisterUnit(cx, self, range));
			} else if (op == OVERLAPPING_FILES_SAME_REGISTER_UNIT) {
				wait(self->overlappingFilesSameRegisterUnit(cx, self, range));
			} else if (op == OVERLAPPING_FILES_MULTIPLE_REGISTER_UNIT) {
				wait(self->overlappingFilesSameRegisterUnit(cx, self, range));
			} else if (op == REGISTER_FILES_ALREADY_LOADING_UNIT) {
				wait(self->registerFilesAlreadyLoadingUnit(cx, self, range));
			} else if (op == REGISTER_NONEXISTENT_FILE_ID_UNIT) {
				wait(self->registerNonexistentFileIdUnit(cx, self, range));
			} else if (op == REGISTER_NONEXISTENT_FILE_UNIT) {
				wait(self->registerNonexistentFileUnit(cx, self, range));
			} else if (op == UNUSED_POINTER_CLEANUP_UNIT) {
				wait(self->unusedPointerCleanupUnit(cx, self, range));
			} else if (op == ABORTED_POINTER_CLEANUP_UNIT) {
				wait(self->abortedPointerCleanupUnit(cx, self, range));
			} else if (op == FILE_SIZE_MISMATCH_UNIT) {
				wait(self->fileSizeMismatchUnit(cx, self, range));
			} else if (op == RESERVATION_FAILED) {
				wait(self->reservationFailed(cx, self, range));
			} else if (op == RESERVATION_SUCCEEDED) {
				wait(self->reservationSucceeded(cx, self, range, rangeSecond));
			} else if (op == ABORT_THEN_RELOAD_UNIT) {
				wait(self->abortThenReloadUnit(cx, self, range));
			} else if (op == BLOB_CLEANUP_ON_ABORT_UNIT) {
				wait(self->blobCleanupOnAbortUnit(cx, self, range));
			} else {
				ASSERT(false);
			}

			wait(delay(1.0));
		}
	}
};

WorkloadFactory<BulkLoadOpsWorkload> BulkLoadOpsWorkloadFactory;
