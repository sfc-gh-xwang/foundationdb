/*
 * TesterBulkLoadCorrectnessWorkload.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */
#include "TesterApiWorkload.h"
#include "TesterUtil.h"
#include "test/fdb_api.hpp"
#include <fmt/format.h>

#include <set>
#include <map>

namespace FdbApiTester {

fdb::BulkLoadID randomLoadIDFromSet(const std::set<fdb::BulkLoadID>& s) {
	ASSERT(s.size() > 0);
	int randomIdx = Random::get().randomInt(0, s.size() - 1);
	auto it = s.begin();
	for (int i = 0; i < randomIdx; i++) {
		it++;
	}
	return *it;
}

template <class V>
fdb::BulkLoadID randomLoadIDWithState(const std::map<fdb::BulkLoadID, V>& m) {
	ASSERT(m.size() > 0);
	int randomIdx = Random::get().randomInt(0, m.size() - 1);
	auto it = m.begin();
	for (int i = 0; i < randomIdx; i++) {
		it++;
	}
	return it->first;
}

class BulkLoadCorrectnessWorkload : public ApiWorkload {
public:
	BulkLoadCorrectnessWorkload(const WorkloadConfig& config) : ApiWorkload(config) {
		loadCap = Random::get().randomInt(1, 5);
	}

private:
	int loadCap;
	// FIXME: expected error checking doesn't quite work
	// FIXME: write files
	// FIXME: also add expect-error ops? re-load same range, already done abort failed, cleanup failed, invalid id
	enum OpType {
		OP_START,
		OP_GET_FILE,
		OP_WRITE_FILE,
		OP_REGISTER_FILE,
		OP_REGISTER_COMPLETE,
		OP_WAIT_COMPLETE,
		OP_ABORT,
		OP_CLEANUP,
		OP_LAST = OP_CLEANUP
	};

	struct State {
		fdb::KeyRange keyRange;
		fdb::BulkLoadFilesToWriteResult filesToWrite;
		int64_t logicalSize;
		int64_t physicalSize;

		State() {}
		State(fdb::KeyRange keyRange) : keyRange(keyRange) {}
	};

	struct BulkLoadMetadata {
		int activeLoads = 0;

		// loads ready for staging
		std::map<fdb::BulkLoadID, State> loadsStagingNeedFiles;
		std::map<fdb::BulkLoadID, State> loadsStagingNeedToWriteFiles;
		std::map<fdb::BulkLoadID, State> loadsStagingNeedToRegisterFiles;
		std::set<fdb::BulkLoadID> loadsStagingReady;

		// loads staged and waiting
		std::set<fdb::BulkLoadID> loadsWaiting;
		std::set<fdb::BulkLoadID> loadsDone;
		std::set<fdb::BulkLoadID> loadsAborted;

		bool hasLoadsStaging() {
			return !loadsStagingNeedFiles.empty() || !loadsStagingNeedToWriteFiles.empty() ||
			       !loadsStagingNeedToRegisterFiles.empty() || !loadsStagingReady.empty();
		}

		fdb::BulkLoadID getRandomStagingID() {
			std::vector<fdb::BulkLoadID> candidates;
			if (!loadsStagingNeedFiles.empty()) {
				candidates.push_back(randomLoadIDWithState(loadsStagingNeedFiles));
			}
			if (!loadsStagingNeedToWriteFiles.empty()) {
				candidates.push_back(randomLoadIDWithState(loadsStagingNeedToWriteFiles));
			}
			if (!loadsStagingNeedToRegisterFiles.empty()) {
				candidates.push_back(randomLoadIDWithState(loadsStagingNeedToRegisterFiles));
			}
			if (!loadsStagingReady.empty()) {
				candidates.push_back(randomLoadIDFromSet(loadsStagingReady));
			}
			int randomIdx = Random::get().randomInt(0, candidates.size() - 1);
			return candidates[randomIdx];
		}
	};

	void checkVoidError(fdb::BulkLoadID id, fdb::Future f) {
		fdb::future_var::None::Type ignore;
		fdb::Error err = f.getNothrow<fdb::future_var::None>(ignore);
		if (err) {
			error("DBG: Bulk Load {0} got error {1}", fdb::toHexString(id), err.code());
		}
	}

	std::map<std::optional<int>, std::shared_ptr<BulkLoadMetadata>> metadataByTenant;

	void randomOpStart(TTaskFct cont, std::shared_ptr<BulkLoadMetadata> metadata, std::optional<int> tenantId) {
		fdb::KeyRange keyRange = randomUniquePrefixKeyRange();
		execOperation(
		    [this, metadata, keyRange](auto ctx) {
			    fdb::Future f = ctx->dbOps()->startBulkLoad(keyRange.beginKey, keyRange.endKey).eraseType();
			    ctx->continueAfter(f, [this, ctx, metadata, keyRange, f]() {
				    fdb::BulkLoadID loadId = f.get<fdb::future_var::BulkLoadIDResult>();
				    info("Bulk Load Start Got ID {0} for key range {1} - {2}",
				         fdb::toHexString(loadId),
				         fdb::toCharsRef(keyRange.beginKey),
				         fdb::toCharsRef(keyRange.endKey));
				    metadata->loadsStagingNeedFiles.insert({ loadId, State(keyRange) });
				    ctx->done();
			    });
		    },
		    cont,
		    getTenant(tenantId),
		    /* failOnError = */ false);
	}

	void randomOpGetFile(TTaskFct cont, std::shared_ptr<BulkLoadMetadata> metadata, std::optional<int> tenantId) {
		fdb::BulkLoadID id = randomLoadIDWithState(metadata->loadsStagingNeedFiles);
		info("Bulk Load {0} Getting Files To Write", fdb::toHexString(id));
		execOperation(
		    [this, metadata, id](auto ctx) {
			    int filesToGet = Random::get().randomInt(1, 3);
			    fdb::Future f = ctx->dbOps()->bulkLoadGetFilesToWrite(id, filesToGet).eraseType();
			    ctx->continueAfter(f, [this, ctx, metadata, id, filesToGet, f]() {
				    State st = metadata->loadsStagingNeedFiles[id];
				    st.filesToWrite = f.get<fdb::future_var::BulkLoadFilesToWriteResult>();
				    info("Bulk Load {0} Got {1} Files To Write", fdb::toHexString(id), st.filesToWrite.files().size());
				    ASSERT(filesToGet == st.filesToWrite.files().size());
				    for (auto& it : st.filesToWrite.files()) {
					    // sanity check returned metadata
					    info("FileId= {0}", fdb::toHexString(fdb::ByteString(it->file_id.begin, it->file_id.length)));
					    info("FileName= {0}",
					         fdb::toCharsRef(fdb::ByteString(it->filename.begin, it->filename.length)));
					    info("TargetSizeBytes= {0}", it->target_size_bytes);
					    ASSERT(it->target_size_bytes >= 0 && it->target_size_bytes <= 100000000);
					    info("FileType= {0}", it->file_type);
					    ASSERT(it->file_type == 1);
					    info("FileFormat= {0}", it->file_format);
					    ASSERT(it->file_format == 1);
					    info("Compression= {0}", it->compression);
					    ASSERT(it->compression == 0 || it->compression == 1);
				    }

				    metadata->loadsStagingNeedToWriteFiles.insert({ id, st });
				    metadata->loadsStagingNeedFiles.erase(id);
				    ctx->done();
			    });
		    },
		    cont,
		    getTenant(tenantId),
		    /* failOnError = */ false);
	}

	void randomOpWriteFile(TTaskFct cont, std::shared_ptr<BulkLoadMetadata> metadata, std::optional<int> tenantId) {
		fdb::BulkLoadID id = randomLoadIDWithState(metadata->loadsStagingNeedToWriteFiles);
		info("Bulk Load {0} Getting Files To Write", fdb::toHexString(id));
		execOperation(
		    [this, metadata, id](auto ctx) {
			    // dead-simple file - 1 file with 1 key-value pair where the key is the range start and the value is the
			    // range end
			    State st = metadata->loadsStagingNeedToWriteFiles[id];
			    auto& fileToWrite = st.filesToWrite.files()[0];

			    fdb::native::FDBKeyValue kv;
			    kv.key = st.keyRange.beginKey.data();
			    kv.key_length = st.keyRange.beginKey.length();
			    kv.value = st.keyRange.endKey.data();
			    kv.value_length = st.keyRange.endKey.length();
			    int64_t logicalSize = kv.key_length + kv.value_length;

			    fdb::Future f = ctx->dbOps()
			                        ->writeSnapshotFile(fdb::VectorRef<fdb::KeyValueRef>((fdb::KeyValueRef*)&kv, 1),
			                                            fileToWrite->compression,
			                                            &fileToWrite->tenant_prefix,
			                                            fileToWrite->encryption_ctx)
			                        .eraseType();
			    ctx->continueAfter(f, [this, ctx, metadata, id, logicalSize, st, f]() {
				    fdb::KeyRef fileBytes = f.get<fdb::future_var::KeyRef>();
				    State newSt = st;
				    newSt.logicalSize = logicalSize;
				    newSt.physicalSize = fileBytes.length();
				    info("Bulk Load {0} Wrote file of size {1}, {2}",
				         fdb::toHexString(id),
				         newSt.logicalSize,
				         newSt.physicalSize);

				    // TODO: actually write file to disk, or do in writeSnapshotFile

				    metadata->loadsStagingNeedToRegisterFiles.insert({ id, newSt });
				    metadata->loadsStagingNeedToWriteFiles.erase(id);
				    ctx->done();
			    });
		    },
		    cont,
		    getTenant(tenantId),
		    /* failOnError = */ false);
	}

	void randomOpRegisterFile(TTaskFct cont, std::shared_ptr<BulkLoadMetadata> metadata, std::optional<int> tenantId) {
		fdb::BulkLoadID id = randomLoadIDWithState(metadata->loadsStagingNeedToRegisterFiles);
		info("Bulk Load {0} Registering Files", fdb::toHexString(id));
		execOperation(
		    [this, metadata, id](auto ctx) {
			    State st = metadata->loadsStagingNeedToRegisterFiles[id];
			    fdb::BulkLoadWrittenFileRef writtenFile;
			    // FIXME: make this nicer
			    writtenFile.file_id = st.filesToWrite.files()[0]->file_id;
			    writtenFile.key_range.begin_key = st.keyRange.beginKey.data();
			    writtenFile.key_range.begin_key_length = st.keyRange.beginKey.length();
			    writtenFile.key_range.end_key = st.keyRange.endKey.data();
			    writtenFile.key_range.end_key_length = st.keyRange.endKey.length();
			    writtenFile.physical_size = st.physicalSize;
			    writtenFile.logical_size = st.logicalSize;
			    fdb::BulkLoadFilesWrittenArray filesWritten(&writtenFile, 1);
			    fdb::Future f = ctx->dbOps()->bulkLoadMarkFilesWritten(id, filesWritten).eraseType();
			    ctx->continueAfter(f, [this, ctx, metadata, id, f]() {
				    checkVoidError(id, f);
				    info("Bulk Load {0} Registered Files", fdb::toHexString(id));

				    metadata->loadsStagingReady.insert(id);
				    metadata->loadsStagingNeedToRegisterFiles.erase(id);
				    ctx->done();
			    });
		    },
		    cont,
		    getTenant(tenantId),
		    /* failOnError = */ false);
	}

	void randomOpRegisterComplete(TTaskFct cont,
	                              std::shared_ptr<BulkLoadMetadata> metadata,
	                              std::optional<int> tenantId) {
		fdb::BulkLoadID id = randomLoadIDFromSet(metadata->loadsStagingReady);
		info("Bulk Load {0} Marking all files registered", fdb::toHexString(id));
		execOperation(
		    [this, metadata, id](auto ctx) {
			    fdb::Future f = ctx->dbOps()->bulkLoadAllFilesRegistered(id).eraseType();
			    ctx->continueAfter(f, [this, ctx, metadata, id, f]() {
				    checkVoidError(id, f);
				    info("Bulk Load {0} All Files Registered", fdb::toHexString(id));
				    metadata->loadsWaiting.insert(id);
				    metadata->loadsStagingReady.erase(id);
				    ctx->done();
			    });
		    },
		    cont,
		    getTenant(tenantId),
		    /* failOnError = */ false);
	}

	void randomOpWaitComplete(TTaskFct cont, std::shared_ptr<BulkLoadMetadata> metadata, std::optional<int> tenantId) {
		fdb::BulkLoadID id = randomLoadIDFromSet(metadata->loadsWaiting);
		info("Bulk Load {0} Waiting Complete", fdb::toHexString(id));
		execOperation(
		    [this, metadata, id](auto ctx) {
			    fdb::Future f = ctx->dbOps()->waitBulkLoadComplete(id).eraseType();
			    ctx->continueAfter(f, [this, ctx, metadata, id, f]() {
				    checkVoidError(id, f);
				    info("Bulk Load {0} Complete", fdb::toHexString(id));
				    metadata->loadsWaiting.erase(id);
				    metadata->loadsDone.insert(id);

				    ctx->done();
			    });
		    },
		    [this, cont]() { schedule(cont); },
		    getTenant(tenantId),
		    /* failOnError = */ false);
	}

	void randomOpAbort(TTaskFct cont, std::shared_ptr<BulkLoadMetadata> metadata, std::optional<int> tenantId) {
		fdb::BulkLoadID id;
		bool couldAlreadyBeDone;
		if ((!metadata->loadsWaiting.empty() && Random::get().randomBool(0.5)) || !metadata->hasLoadsStaging()) {
			id = randomLoadIDFromSet(metadata->loadsWaiting);
			couldAlreadyBeDone = true;
		} else {
			id = metadata->getRandomStagingID();
			couldAlreadyBeDone = false;
		}
		info("Bulk Load {0} Aborting", fdb::toHexString(id));
		execOperation(
		    [this, metadata, id, couldAlreadyBeDone](auto ctx) {
			    fdb::Future f = ctx->dbOps()->abortBulkLoad(id).eraseType();
			    ctx->continueAfter(f, [this, ctx, metadata, id, couldAlreadyBeDone, f]() {
				    fdb::future_var::None::Type ignore;
				    fdb::Error err = f.getNothrow<fdb::future_var::None>(ignore);
				    // could be in any so just erase from all
				    metadata->loadsStagingNeedFiles.erase(id);
				    metadata->loadsStagingNeedToWriteFiles.erase(id);
				    metadata->loadsStagingNeedToRegisterFiles.erase(id);
				    metadata->loadsWaiting.erase(id);
				    if (err) {
					    info("Bulk Load {0} Got Error {1} aborting", fdb::toHexString(id), err.code());
					    ASSERT(err.code() == 2601 /*bulk load abort failed*/ && couldAlreadyBeDone);
					    metadata->loadsDone.insert(id);
				    } else {
					    info("Bulk Load {0} Aborted", fdb::toHexString(id));
					    metadata->loadsAborted.insert(id);
				    }

				    ctx->done();
			    });
		    },
		    [this, cont]() { schedule(cont); },
		    getTenant(tenantId),
		    /* failOnError = */ false);
	}

	void randomOpCleanup(TTaskFct cont, std::shared_ptr<BulkLoadMetadata> metadata, std::optional<int> tenantId) {
		// TODO REMOVE
		auto& target = Random::get().randomBool(0.5) ? metadata->loadsAborted : metadata->loadsDone;
		if (metadata->loadsAborted.empty()) {
			target = metadata->loadsDone;
		}
		if (metadata->loadsDone.empty()) {
			target = metadata->loadsAborted;
		}
		fdb::BulkLoadID id = randomLoadIDFromSet(target);
		execOperation(
		    [this, metadata, id](auto ctx) {
			    fdb::Future f = ctx->dbOps()->cleanupBulkLoad(id).eraseType();
			    ctx->continueAfter(f, [this, ctx, metadata, id, f]() {
				    checkVoidError(id, f);
				    info("Bulk Load {0} Cleaned Up", fdb::toHexString(id));
				    metadata->loadsDone.erase(id);
				    metadata->loadsAborted.erase(id);

				    ctx->done();
			    });
		    },
		    [this, cont]() { schedule(cont); },
		    getTenant(tenantId),
		    /* failOnError = */ false);
	}

	OpType chooseRandomAllowedOp(std::shared_ptr<BulkLoadMetadata> metadata) {
		// enum OpType { OP_START, OP_REGISTER_COMPLETE, OP_WAIT_COMPLETE, OP_ABORT, OP_CLEANUP, OP_LAST = OP_CLEANUP };
		std::vector<OpType> allowed;
		if (metadata->activeLoads < loadCap) {
			allowed.push_back(OP_START);
		}
		if (!metadata->loadsStagingNeedFiles.empty()) {
			allowed.push_back(OP_GET_FILE);
		}
		if (!metadata->loadsStagingNeedToWriteFiles.empty()) {
			allowed.push_back(OP_WRITE_FILE);
		}
		if (!metadata->loadsStagingNeedToRegisterFiles.empty()) {
			allowed.push_back(OP_REGISTER_FILE);
		}
		if (!metadata->loadsStagingReady.empty()) {
			allowed.push_back(OP_REGISTER_COMPLETE);
		}
		if (!metadata->loadsWaiting.empty()) {
			allowed.push_back(OP_WAIT_COMPLETE);
		}
		if (!metadata->loadsWaiting.empty() || metadata->hasLoadsStaging()) {
			allowed.push_back(OP_ABORT);
		}
		if (!metadata->loadsDone.empty() || !metadata->loadsAborted.empty()) {
			allowed.push_back(OP_CLEANUP);
		}
		ASSERT(!allowed.empty());
		int randomIdx = Random::get().randomInt(0, allowed.size() - 1);
		return allowed[randomIdx];
	}

	void randomOperation(TTaskFct cont) override {
		std::optional<int> tenantId = randomTenant();
		std::shared_ptr<BulkLoadMetadata> metadata(new BulkLoadMetadata());
		auto it = metadataByTenant.find(tenantId);
		if (it != metadataByTenant.end()) {
			metadata = it->second;
		} else {
			metadataByTenant[tenantId] = metadata;
		}
		OpType opType = chooseRandomAllowedOp(metadata);

		switch (opType) {
		case OP_START:
			randomOpStart(cont, metadata, tenantId);
			metadata->activeLoads++;
			break;
		case OP_GET_FILE:
			randomOpGetFile(cont, metadata, tenantId);
			break;
		case OP_WRITE_FILE:
			randomOpWriteFile(cont, metadata, tenantId);
			break;
		case OP_REGISTER_FILE:
			randomOpRegisterFile(cont, metadata, tenantId);
			break;
		case OP_REGISTER_COMPLETE:
			randomOpRegisterComplete(cont, metadata, tenantId);
			break;
		case OP_WAIT_COMPLETE:
			randomOpWaitComplete(cont, metadata, tenantId);
			break;
		case OP_ABORT:
			randomOpAbort(cont, metadata, tenantId);
			break;
		case OP_CLEANUP:
			metadata->activeLoads--;
			break;
		default:
			ASSERT(false);
		}
	}
};
WorkloadFactory<BulkLoadCorrectnessWorkload> BulkLoadCorrectnessWorkloadFactory("BulkLoadCorrectness");

} // namespace FdbApiTester
