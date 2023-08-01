/*
 * BulkLoader.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include <map>
#include "fdbserver/BulkLoader.h"

#include "flow/ActorCollection.h"
#include "flow/network.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BlobGranuleFiles.actor.h"
#include "fdbclient/BulkLoadMetadata.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/BlobWorker.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define DBG_BL_REQUEST false

// FIXME: for now this is shared with blob worker. Maybe refactor?
// blob storage is down temporarily, doesn't mean file itself is invalid
bool isTransientFileValidationError(const Error& e) {
	switch (e.code()) {
	case error_code_io_error:
	case error_code_io_timeout:
	case error_code_http_request_failed:
	case error_code_connection_failed:
	case error_code_lookup_failed: // dns
	case error_code_platform_error: // injected faults
		return true;
	default:
		return false;
	};
}

bool canReplyWith(const Error& e) {
	switch (e.code()) {
	case error_code_bulk_load_not_found:
	case error_code_bulk_load_not_registering_files:
	case error_code_bulk_load_tenant_mismatch:
	case error_code_bulk_load_file_id_not_found:
	case error_code_bulk_load_invalid_file_metadata:
	case error_code_bulk_load_aborted:
	case error_code_bulk_load_abort_failed:
	case error_code_bulk_load_range_conflict:
	case error_code_bulk_load_cleanup_failed:
	case error_code_bulk_load_range_not_empty:
	case error_code_bulk_load_create_too_old:
	case error_code_bulk_load_file_missing:
	case error_code_bulk_load_cannot_validate_file:
		return true;
	default:
		return false;
	};
}

// checks that load id exists and is in the STARTED state where it can load files
ACTOR Future<Void> checkCanLoadFiles(Reference<ReadYourWritesTransaction> tr, BulkLoadID id) {
	Optional<BulkLoadStatus> status = wait(BulkLoadMetadata::statusMap().get(tr, id));
	if (!status.present()) {
		throw bulk_load_not_found();
	}
	if (status.get().state != BulkLoadStatus::State::STARTED) {
		throw bulk_load_not_registering_files();
	}
	return Void();
}

// FIXME: could cache initial state for bulk loads. Still want to do the validation with the transaction that the load
// is still happening when actually doing the operations though
ACTOR Future<Void> loadInitialState(Reference<ReadYourWritesTransaction> tr,
                                    BulkLoadID id,
                                    TenantInfo tenant,
                                    BulkLoadInfo* info,
                                    Version* version) {
	wait(checkCanLoadFiles(tr, id));
	state Future<Optional<Versionstamp>> versionF = BulkLoadMetadata::versionMap().get(tr, id);
	state Future<Optional<BulkLoadInfo>> infoF = BulkLoadMetadata::infoMap().get(tr, id);
	wait(success(versionF) && success(infoF));

	*info = infoF.get().get();
	*version = versionF.get().get().version;

	if (info->tenantId != tenant.tenantId) {
		TraceEvent("BulkLoadTenantMismatch")
		    .detail("BulkLoadID", id)
		    .detail("KnownTenantId", info->tenantId)
		    .detail("ProvidedTenantId", tenant.tenantId);
		throw bulk_load_tenant_mismatch();
	}

	if (DBG_BL_REQUEST) {
		fmt::print("BL: {0} Loaded inital state: {1} [{2} - {3}) @ {4}\n",
		           id.printable(),
		           info->tenantId,
		           info->keyRange.begin.printable(),
		           info->keyRange.end.printable(),
		           *version);
	}

	return Void();
}

int getFilePointerBatchSize(int remaining) {
	int batchSize = SERVER_KNOBS->BULK_LOAD_FILE_POINTER_BATCH_SIZE;
	if ((!g_network->isSimulated() || !g_simulator->speedUpSimulation) && BUGGIFY_WITH_PROB(0.1)) {
		batchSize = deterministicRandom()->randomInt(1, 5);
	}
	if (remaining < batchSize) {
		batchSize = remaining;
	}
	return batchSize;
}

struct CandidateFilePointer {
	BulkLoadMetadata::CreatedFilePointerRef dbState;
	BulkLoadFileToWriteRef response;
};

BulkLoadFileID uidToFileID(UID id) {
	// TODO this is the same as granuleIDToCFKey - make common function?
	BinaryWriter wr(Unversioned());
	wr << id;
	return wr.toValue();
}

// self, bstore, cipherKeysCtx, cipherKeysMeta, version, batchSize
std::vector<CandidateFilePointer> createFilePointers(Reference<BulkLoaderData> self,
                                                     Reference<BlobConnectionProvider> bstore,
                                                     Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx,
                                                     Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta,
                                                     Version version,
                                                     int count,
                                                     Arena& arena) {
	std::vector<CandidateFilePointer> result;
	result.reserve(count);

	for (int i = 0; i < count; i++) {
		CandidateFilePointer fp;

		UID fileUID = deterministicRandom()->randomUniqueID();
		std::string fname = randomBGFilename(self->id, fileUID, version, ".snapshot");
		auto fileToWrite = bstore->createForWrite(fname);
		StringRef filename(arena, fileToWrite.second);

		fp.dbState.filename = filename;
		fp.dbState.type = BulkLoadFileType::SNAPSHOT;
		fp.dbState.cipherKeysMetaRef = cipherKeysMeta;

		Standalone<StringRef> fileId = uidToFileID(fileUID);
		arena.dependsOn(fileId.arena());
		fp.response.fileId = fileId;

		fp.response.filename = filename;
		fp.response.targetSize = SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES;
		fp.response.cipherKeysCtx = cipherKeysCtx;
		fp.response.type = BulkLoadFileType::SNAPSHOT;
		fp.response.format = BulkLoadFileFormat::ROW_V1;
		Optional<CompressionFilter> compression = getBlobFileCompressFilter();
		fp.response.compression = compression.present() ? compression.get() : CompressionFilter::NONE;

		result.push_back(fp);
	}
	return result;
}

ACTOR Future<Void> handleFilesToWriteRequest(Reference<BulkLoaderData> self,
                                             Reference<BlobWorkerData> bwData,
                                             BulkLoadFilesToWriteRequest req) {
	state BulkLoadFilesToWriteReply rep;
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);

	// downgrade priority since this is likely LoadBalancedEndpoint
	wait(delay(0, TaskPriority::DefaultEndpoint));
	++self->stats.filesToWriteRequests;
	++self->stats.activeRequests;
	rep.filePointers.reserve(rep.arena, req.count);

	if (DBG_BL_REQUEST) {
		fmt::print("BL {0}: {1} HandleFilesToWriteRequest: start\n", self->id.shortString(), req.id.printable());
	}

	try {
		// load initial state for bulk load
		state BulkLoadInfo info;
		state Version version;
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(loadInitialState(tr, req.id, req.tenantInfo, &info, &version));
				break;
			} catch (Error& e) {
				if (DBG_BL_REQUEST) {
					fmt::print("BL {0}: {1} HandleFilesToWriteRequest: Error loading initial state: {2}\n",
					           self->id.shortString(),
					           req.id.printable(),
					           e.name());
				}
				wait(tr->onError(e));
			}
		}

		// try to reuse same transaction for second part to be more efficient
		if (BUGGIFY) {
			tr->reset();
		}

		state KeyRange physicalRange = info.getPhysicalKeyRange();

		// before creating file pointers, load metadata and encryption keys for tenant
		state Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx;
		state Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta;
		if (bwData->encryptMode.isEncryptionEnabled()) {
			if (DBG_BL_REQUEST) {
				fmt::print("BL {0}: {1} HandleFilesToWriteRequest: Loading Cipher Keys\n",
				           self->id.shortString(),
				           req.id.printable());
			}
			BlobGranuleCipherKeysCtx ciphKeysCtx = wait(getLatestGranuleCipherKeys(bwData, physicalRange, &rep.arena));
			cipherKeysCtx = std::move(ciphKeysCtx);
			cipherKeysMeta = BlobGranuleCipherKeysCtx::toCipherKeysMeta(cipherKeysCtx.get());
			if (DBG_BL_REQUEST) {
				fmt::print("BL {0}: {1} HandleFilesToWriteRequest: Got Cipher Keys\n",
				           self->id.shortString(),
				           req.id.printable());
			}
		}

		// load volume info as well
		state Reference<BlobConnectionProvider> bstore;
		if (SERVER_KNOBS->BG_METADATA_SOURCE != "tenant") {
			bstore = bwData->bstore;
		} else {
			if (DBG_BL_REQUEST) {
				fmt::print("BL {0}: {1} HandleFilesToWriteRequest: Loading Volume\n",
				           self->id.shortString(),
				           req.id.printable());
			}
			wait(store(bstore, loadBStoreForTenant(bwData, physicalRange)));
			if (DBG_BL_REQUEST) {
				fmt::print(
				    "BL {0}: {1} HandleFilesToWriteRequest: Got Volume\n", self->id.shortString(), req.id.printable());
			}
		}

		loop {
			if (req.count <= rep.filePointers.size()) {
				ASSERT_EQ(req.count, rep.filePointers.size());
				if (DBG_BL_REQUEST) {
					fmt::print(
					    "BL {0}: {1} HandleFilesToWriteRequest: Done\n", self->id.shortString(), req.id.printable());
				}
				req.reply.send(rep);
				--self->stats.activeRequests;
				return Void();
			}

			int batchSize = getFilePointerBatchSize(req.count - rep.filePointers.size());
			state std::vector<CandidateFilePointer> candidateFilePointers =
			    createFilePointers(self, bstore, cipherKeysCtx, cipherKeysMeta, version, batchSize, rep.arena);
			wait(yield());

			// try to commit file pointers to db
			if (DBG_BL_REQUEST) {
				fmt::print("BL {0}: {1} HandleFilesToWriteRequest: Writing {2} File Pointers to DB\n",
				           self->id.shortString(),
				           req.id.printable(),
				           candidateFilePointers.size());
			}
			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					wait(checkCanLoadFiles(tr, req.id));

					for (int i = 0; i < candidateFilePointers.size(); i++) {
						auto& fp = candidateFilePointers[i];
						std::pair<BulkLoadID, BulkLoadFileID> key = { req.id, fp.response.fileId };
						BulkLoadMetadata::createdFilePointersMap().set(tr, key, fp.dbState);
					}
					wait(tr->commit());
					tr->reset();
					break;
				} catch (Error& e) {
					if (DBG_BL_REQUEST) {
						fmt::print(
						    "BL {0}: {1} HandleFilesToWriteRequest: Error Writing {2} File Pointers to DB: {3}\n",
						    self->id.shortString(),
						    req.id.printable(),
						    candidateFilePointers.size(),
						    e.name());
					}
					wait(tr->onError(e));
				}
			}

			if (BUGGIFY && bwData->maybeInjectTargetedRestart()) {
				wait(Never());
			}

			if (DBG_BL_REQUEST) {
				fmt::print("BL {0}: {1} HandleFilesToWriteRequest: Wrote {2} File Pointers to DB\n",
				           self->id.shortString(),
				           req.id.printable(),
				           candidateFilePointers.size());
			}

			// file pointers successfully written to db, add them to response
			for (auto& fp : candidateFilePointers) {
				// data already lives in arena
				rep.filePointers.push_back(rep.arena, fp.response);
			}
			self->stats.filesToWriteCreated += candidateFilePointers.size();
		}
	} catch (Error& e) {
		--self->stats.activeRequests;
		if (DBG_BL_REQUEST) {
			fmt::print("BL {0}: {1} HandleFilesToWriteRequest: Error {2}\n",
			           self->id.shortString(),
			           req.id.printable(),
			           e.name());
		}
		if (canReplyWith(e)) {
			TraceEvent("BulkLoaderFilesToWriteError", self->id)
			    .errorUnsuppressed(e)
			    .suppressFor(10.0)
			    .detail("BulkLoadID", req.id);
			++self->stats.errors;
			req.reply.sendError(e);
			return Void();
		}
		TraceEvent(SevError, "BulkLoaderFilesToWriteUnexpectedError", self->id).error(e).detail("BulkLoadID", req.id);
		throw e;
	}
}

ACTOR Future<Void> validateBlobPointer(Reference<BulkLoaderData> self,
                                       BulkLoadID id,
                                       BulkLoadInfo info,
                                       Reference<BlobConnectionProvider> bstore,
                                       BulkLoadWrittenFileRef file,
                                       StringRef filename) {
	wait(self->fileValidationLock.take());
	state FlowLock::Releaser holdingLock(self->fileValidationLock);
	wait(delay(0, TaskPriority::BulkLoaderValidateFiles));

	// FIXME: add configurable validation level { NONE, EXISTS, DATA };

	try {
		std::string fname = filename.toString();
		state Reference<BackupContainerFileSystem> blobConn = bstore->getForAccess(fname);
		state Reference<IAsyncFile> reader = wait(blobConn->readFile(fname));
		state Future<int64_t> sizeF = reader->size();
		// read 1 byte from file to make sure it exists
		state uint8_t data;
		int readSize = wait(reader->read(&data, 1, 0));
		ASSERT(readSize == 1);
		int64_t size = wait(sizeF);
		if (size != file.physicalSize) {
			TraceEvent("BulkLoadInvalidFileSize")
			    .detail("LoadID", id)
			    .detail("FileID", file.fileId)
			    .detail("UserProvidedSize", file.physicalSize)
			    .detail("ActualSize", size);
			throw bulk_load_invalid_file_metadata();
		}
	} catch (Error& e) {
		if (e.code() == error_code_file_not_found) {
			TraceEvent("BulkLoadInvalidFilenameMissing").detail("LoadID", id).detail("FileID", file.fileId);
			throw bulk_load_file_missing();
		} else if (isTransientFileValidationError(e)) {
			CODE_PROBE(true, "bulk load transient error validating file");
			++self->stats.fileValidationErrors;
			TraceEvent("BulkLoadTransientErrorValidatingFile")
			    .errorUnsuppressed(e)
			    .suppressFor(10.0)
			    .detail("LoadID", id)
			    .detail("FileID", file.fileId)
			    .detail("Filename", filename);
			throw bulk_load_cannot_validate_file();
		} else {
			throw e;
		}
	}

	++self->stats.filesWrittenValidated;
	return Void();
}

ACTOR Future<Void> handleFilesWrittenRequest(Reference<BulkLoaderData> self,
                                             Reference<BlobWorkerData> bwData,
                                             BulkLoadFilesWrittenRequest req) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);

	// downgrade priority since this is likely LoadBalancedEndpoint
	wait(delay(0, TaskPriority::DefaultEndpoint));
	++self->stats.filesWrittenRequests;
	++self->stats.activeRequests;

	if (DBG_BL_REQUEST) {
		fmt::print("BL {0}: {1} HandleFilesWrittenRequest: start\n", self->id.shortString(), req.id.printable());
	}

	try {
		// load initial state for bulk load
		state BulkLoadInfo info;
		state Version version;
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(loadInitialState(tr, req.id, req.tenantInfo, &info, &version));
				break;
			} catch (Error& e) {
				if (DBG_BL_REQUEST) {
					fmt::print("BL {0}: {1} HandleFilesWrittenRequest: Error loading initial state: {2}\n",
					           self->id.shortString(),
					           req.id.printable(),
					           e.name());
				}
				wait(tr->onError(e));
			}
		}

		state KeyRange physicalRange = info.getPhysicalKeyRange();

		// load volume info as well
		state Reference<BlobConnectionProvider> bstore;
		if (SERVER_KNOBS->BG_METADATA_SOURCE != "tenant") {
			bstore = bwData->bstore;
		} else {
			if (DBG_BL_REQUEST) {
				fmt::print("BL {0}: {1} HandleFilesWrittenRequest: Loading Volume\n",
				           self->id.shortString(),
				           req.id.printable());
			}
			wait(store(bstore, loadBStoreForTenant(bwData, physicalRange)));
			if (DBG_BL_REQUEST) {
				fmt::print(
				    "BL {0}: {1} HandleFilesWrittenRequest: Got Volume\n", self->id.shortString(), req.id.printable());
			}
		}

		// try to reuse same transaction for second part to be more efficient
		if (BUGGIFY) {
			tr->reset();
		}

		state int idx = 0;

		// do some basic validation that all files are in range and don't overlap
		validateBulkLoadWrittenPointers(req.id, info, req.filesWritten);

		state std::set<BulkLoadFileID> validatedFiles;
		loop {
			if (idx >= req.filesWritten.size()) {
				ASSERT_EQ(idx, req.filesWritten.size());
				if (DBG_BL_REQUEST) {
					fmt::print(
					    "BL {0}: {1} HandleFilesWrittenRequest: Done\n", self->id.shortString(), req.id.printable());
				}
				req.reply.send(Void());
				--self->stats.activeRequests;
				return Void();
			}

			state int batchSize = getFilePointerBatchSize(req.filesWritten.size() - idx);
			CODE_PROBE(batchSize < req.filesWritten.size(), "multiple batches for bulk load written files");

			if (DBG_BL_REQUEST) {
				fmt::print("BL {0}: {1} HandleFilesWrittenRequest: Updating {2} File Pointers in DB\n",
				           self->id.shortString(),
				           req.id.printable(),
				           batchSize);
			}
			// try to commit file pointers to db in one txn
			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					wait(checkCanLoadFiles(tr, req.id));

					// handle idempotency by checking, if created pointer for fileid is gone, that written pointer
					// exists and matches
					state std::vector<Future<Optional<Standalone<BulkLoadMetadata::CreatedFilePointerRef>>>>
					    createdPointers;
					state std::vector<Future<Optional<Standalone<BulkLoadMetadata::WrittenFilePointerRef>>>>
					    writtenPointers;
					for (int batchIdx = 0; batchIdx < batchSize; batchIdx++) {
						int i = idx + batchIdx;
						BulkLoadMetadata::FilePointerKey key = { req.id, req.filesWritten[i].fileId };
						createdPointers.push_back(BulkLoadMetadata::createdFilePointersMap().get(tr, key));
						writtenPointers.push_back(BulkLoadMetadata::writtenFilePointersMap().get(tr, key));
					}

					wait(waitForAll(createdPointers) && waitForAll(writtenPointers));
					state int batchIdx;

					// for each written pointer that did not exist, clear the created pointer and set the written
					// pointer. for each written pointer that does exist, check it matches
					for (batchIdx = 0; batchIdx < batchSize; batchIdx++) {
						int i = idx + batchIdx;
						auto toWrite = req.filesWritten[i];
						auto created = createdPointers[batchIdx].get();
						auto written = writtenPointers[batchIdx].get();
						// FIXME: could remove this validation and then just not load written pointer if created already
						// exists as a small perf optimization
						ASSERT(!created.present() || !written.present());
						if (!created.present() && !written.present()) {
							throw bulk_load_file_id_not_found();
						}
						BulkLoadMetadata::FilePointerKey key = { req.id, req.filesWritten[i].fileId };
						CODE_PROBE(written.present(), "bulk loading file written idempotent");
						if (created.present()) {
							// convert to written pointer
							Standalone<BulkLoadMetadata::WrittenFilePointerRef> writtenPtr;
							writtenPtr.arena().dependsOn(created.get().arena());
							writtenPtr.createdPointer = created.get();
							writtenPtr.keyRange = KeyRangeRef(writtenPtr.arena(), toWrite.keyRange);
							writtenPtr.logicalSize = toWrite.logicalSize;
							writtenPtr.physicalSize = toWrite.physicalSize;

							BulkLoadMetadata::createdFilePointersMap().erase(tr, key);
							BulkLoadMetadata::writtenFilePointersMap().set(tr, key, writtenPtr);

							// if we can validate all of the files quickly enough, we can still commit the transaction.
							// But with a large enough batch size or slow enough storage, the expectation is that this
							// would try to commit, get too_old, and re-read the file pointers. But since the first time
							// validated the files and added them to the set, the second time would bypass this check
							// and immediately commit the change.
							if (!validatedFiles.count(req.filesWritten[idx + batchIdx].fileId)) {
								wait(validateBlobPointer(
								    self, req.id, info, bstore, req.filesWritten[i], created.get().filename));
								validatedFiles.insert(req.filesWritten[idx + batchIdx].fileId);
							}

						} else if (written.get().keyRange != toWrite.keyRange ||
						           written.get().logicalSize != toWrite.logicalSize ||
						           written.get().physicalSize != toWrite.physicalSize) {
							// validate for idempotency that written pointer matches toWrite
							TraceEvent("BulkLoadInvalidPointerMismatch")
							    .detail("LoadID", req.id)
							    .detail("FileID", req.filesWritten[i].fileId)
							    .detail("OldLogicalSize", written.get().logicalSize)
							    .detail("OldPhysicalSize", written.get().physicalSize)
							    .detail("OldKeyRange", written.get().keyRange)
							    .detail("NewLogicalSize", toWrite.logicalSize)
							    .detail("NewPhysicalSize", toWrite.physicalSize)
							    .detail("NewKeyRange", toWrite.keyRange);
							throw bulk_load_invalid_file_metadata();
						}
					}

					wait(tr->commit());
					tr->reset();
					break;
				} catch (Error& e) {
					if (DBG_BL_REQUEST) {
						fmt::print(
						    "BL {0}: {1} HandleFilesWrittenRequest: Error Updating {2} File Pointers in DB: {3}\n",
						    self->id.shortString(),
						    req.id.printable(),
						    batchSize,
						    e.name());
					}
					wait(tr->onError(e));
				}
			}

			if (DBG_BL_REQUEST) {
				fmt::print("BL {0}: {1} HandleFilesWrittenRequest: Updated {2} File Pointers in DB\n",
				           self->id.shortString(),
				           req.id.printable(),
				           batchSize);
			}

			if (BUGGIFY && bwData->maybeInjectTargetedRestart()) {
				wait(Never());
			}

			self->stats.filesWrittenRegistered += batchSize;
			idx += batchSize;
		}
	} catch (Error& e) {
		--self->stats.activeRequests;
		if (DBG_BL_REQUEST) {
			fmt::print("BL {0}: {1} HandleFilesWrittenRequest: Error {2}\n",
			           self->id.shortString(),
			           req.id.printable(),
			           e.name());
		}
		if (canReplyWith(e)) {
			TraceEvent("BulkLoaderFilesWrittenError", self->id)
			    .errorUnsuppressed(e)
			    .suppressFor(10.0)
			    .detail("BulkLoadID", req.id);
			++self->stats.errors;
			req.reply.sendError(e);
			return Void();
		}
		TraceEvent(SevError, "BulkLoaderFilesWrittenUnexpectedError", self->id).error(e).detail("BulkLoadID", req.id);
		throw e;
	}
}

ACTOR Future<Void> handleStartRequest(Reference<BulkLoaderData> self,
                                      Reference<BlobWorkerData> bwData,
                                      BulkLoadStartRequest req) {
	wait(delay(0, TaskPriority::DefaultEndpoint));
	++self->stats.startRequests;
	++self->stats.activeRequests;
	try {
		Optional<Reference<Tenant>> tenant;
		if (req.tenantInfo.hasTenant()) {
			tenant = makeReference<Tenant>(req.tenantInfo.tenantId);
		}

		Version maxReadVersion = req.startReadVersion + CLIENT_KNOBS->BULK_LOAD_MAX_CREATE_VERSION_DIFFERENCE;
		wait(self->db->startBulkLoadInternal(req.id, req.keyRange, maxReadVersion, tenant));
		if (BUGGIFY && bwData->maybeInjectTargetedRestart()) {
			wait(Never());
		}
		req.reply.send(Void());
		--self->stats.activeRequests;
		return Void();
	} catch (Error& e) {
		--self->stats.activeRequests;
		if (canReplyWith(e)) {
			TraceEvent("BulkLoaderStartError", self->id)
			    .errorUnsuppressed(e)
			    .suppressFor(10.0)
			    .detail("BulkLoadID", req.id);
			++self->stats.errors;
			req.reply.sendError(e);
			return Void();
		}
		TraceEvent(SevError, "BulkLoaderStartUnexpectedError", self->id).error(e).detail("BulkLoadID", req.id);
		throw e;
	}
}

ACTOR Future<Void> handleStateChangeRequest(Reference<BulkLoaderData> self,
                                            Reference<BlobWorkerData> bwData,
                                            BulkLoadStateChangeRequest req) {
	wait(delay(0, TaskPriority::DefaultEndpoint));
	++self->stats.stateChangeRequests;
	++self->stats.activeRequests;
	try {
		if (req.desired == BulkLoadStatus::State::CLEANUP) {
			wait(self->db->cleanupBulkLoadInternal(req.id, req.tenantInfo.tenantId));
		} else {
			wait(self->db->updateBulkLoadState(req.id, req.desired, req.tenantInfo.tenantId));
		}
		if (BUGGIFY && bwData->maybeInjectTargetedRestart()) {
			wait(Never());
		}
		req.reply.send(Void());
		--self->stats.activeRequests;
		return Void();
	} catch (Error& e) {
		--self->stats.activeRequests;
		if (canReplyWith(e)) {
			TraceEvent("BulkLoaderStateChangeError", self->id)
			    .errorUnsuppressed(e)
			    .suppressFor(10.0)
			    .detail("BulkLoadID", req.id)
			    .detail("Desired", req.desired);
			++self->stats.errors;
			req.reply.sendError(e);
			return Void();
		}
		TraceEvent(SevError, "BulkLoaderStateChangeUnexpectedError", self->id)
		    .error(e)
		    .detail("BulkLoadID", req.id)
		    .detail("Desired", req.desired);
		throw e;
	}
}

ACTOR Future<Void> handleWaitCompleteRequest(Reference<BulkLoaderData> self,
                                             Reference<BlobWorkerData> bwData,
                                             BulkLoadWaitCompleteRequest req) {
	wait(delay(0, TaskPriority::DefaultEndpoint));
	++self->stats.waitCompleteRequests;
	++self->stats.activeRequests;
	try {
		wait(self->db->waitBulkLoadStateAtLeast(req.id, BulkLoadStatus::State::DONE, req.tenantInfo.tenantId));
		req.reply.send(Void());
		if (BUGGIFY && bwData->maybeInjectTargetedRestart()) {
			wait(Never());
		}
		--self->stats.activeRequests;
		return Void();
	} catch (Error& e) {
		--self->stats.activeRequests;
		if (canReplyWith(e)) {
			TraceEvent("BulkLoaderWaitCompleteError", self->id)
			    .errorUnsuppressed(e)
			    .suppressFor(10.0)
			    .detail("BulkLoadID", req.id);
			++self->stats.errors;
			req.reply.sendError(e);
			return Void();
		}
		TraceEvent(SevError, "BulkLoaderWaitCompleteError", self->id).error(e).detail("BulkLoadID", req.id);
		throw e;
	}
}

// currently relies on shared state with blob worker but wouldn't be too difficult to refactor.
// Sharing brings efficiency though - only 1 tenant map
ACTOR Future<Void> bulkLoader(BulkLoaderInterface interf,
                              Reference<BulkLoaderData> self,
                              Reference<BlobWorkerData> bwData) {
	state Future<Void> collection = actorCollection(self->addActor.getFuture());
	self->addActor.send(waitFailureServer(interf.waitFailure.getFuture()));
	try {
		loop choose {
			when(BulkLoadFilesToWriteRequest req = waitNext(interf.filesToWriteRequest.getFuture())) {
				self->addActor.send(handleFilesToWriteRequest(self, bwData, req));
			}
			when(BulkLoadFilesWrittenRequest req = waitNext(interf.filesWrittenRequest.getFuture())) {
				self->addActor.send(handleFilesWrittenRequest(self, bwData, req));
			}
			when(BulkLoadStartRequest req = waitNext(interf.startRequest.getFuture())) {
				self->addActor.send(handleStartRequest(self, bwData, req));
			}
			when(BulkLoadStateChangeRequest req = waitNext(interf.stateChangeRequest.getFuture())) {
				self->addActor.send(handleStateChangeRequest(self, bwData, req));
			}
			when(BulkLoadWaitCompleteRequest req = waitNext(interf.waitCompleteRequest.getFuture())) {
				self->addActor.send(handleWaitCompleteRequest(self, bwData, req));
			}

			when(wait(collection)) {
				TraceEvent("BlobWorkerActorCollectionError", self->id);
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& e) {
		TraceEvent("BulkLoaderDied", self->id).error(e);
		throw e;
	}
}