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

class BulkLoadCorrectnessWorkload : public ApiWorkload {
public:
	BulkLoadCorrectnessWorkload(const WorkloadConfig& config) : ApiWorkload(config) {
		loadCap = Random::get().randomInt(1, 5);
	}

private:
	int loadCap;
	// FIXME: expected error checking doesn't quite work
	// FIXME: support non-tenant loads
	// FIXME: register files
	// FIXME: also add expect-error ops? re-load same range, already done abort failed, cleanup failed, invalid id
	enum OpType { OP_START, OP_REGISTER_COMPLETE, OP_WAIT_COMPLETE, OP_ABORT, OP_CLEANUP, OP_LAST = OP_CLEANUP };

	struct State {
		fdb::KeyRange keyRange;

		State(fdb::KeyRange keyRange) : keyRange(keyRange) {}
	};

	struct BulkLoadMetadata {
		int activeLoads = 0;
		// TODO: loads file staged need done
		// loads ready for staging
		std::map<fdb::BulkLoadID, State> loadsStaging;
		// loads staged and waiting
		std::set<fdb::BulkLoadID> loadsWaiting;
		std::set<fdb::BulkLoadID> loadsDone;
		std::set<fdb::BulkLoadID> loadsAborted;

		fdb::BulkLoadID randomLoadIDToStage() {
			ASSERT(loadsStaging.size() > 0);
			int randomIdx = Random::get().randomInt(0, loadsStaging.size() - 1);
			auto it = loadsStaging.begin();
			for (int i = 0; i < randomIdx; i++) {
				it++;
			}
			return it->first;
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
				    metadata->loadsStaging.insert({ loadId, State(keyRange) });
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
		fdb::BulkLoadID id = metadata->randomLoadIDToStage();
		info("Bulk Load {0} Marking all files registered", fdb::toHexString(id));
		execOperation(
		    [this, metadata, id](auto ctx) {
			    fdb::Future f = ctx->dbOps()->bulkLoadAllFilesRegistered(id).eraseType();
			    ctx->continueAfter(f, [this, ctx, metadata, id, f]() {
				    checkVoidError(id, f);
				    info("Bulk Load {0} All Files Registered", fdb::toHexString(id));
				    metadata->loadsWaiting.insert(id);
				    metadata->loadsStaging.erase(id);
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
		if ((!metadata->loadsWaiting.empty() && Random::get().randomBool(0.5)) || metadata->loadsStaging.empty()) {
			id = randomLoadIDFromSet(metadata->loadsWaiting);
			couldAlreadyBeDone = true;
		} else {
			id = metadata->randomLoadIDToStage();
			couldAlreadyBeDone = false;
		}
		info("Bulk Load {0} Aborting", fdb::toHexString(id));
		execOperation(
		    [this, metadata, id, couldAlreadyBeDone](auto ctx) {
			    fdb::Future f = ctx->dbOps()->abortBulkLoad(id).eraseType();
			    ctx->continueAfter(f, [this, ctx, metadata, id, couldAlreadyBeDone, f]() {
				    fdb::future_var::None::Type ignore;
				    fdb::Error err = f.getNothrow<fdb::future_var::None>(ignore);
				    // could be in either so just erase from both
				    metadata->loadsStaging.erase(id);
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

	void randomOperation(TTaskFct cont) override {
		std::optional<int> tenantId = randomTenant();
		std::shared_ptr<BulkLoadMetadata> metadata(new BulkLoadMetadata());
		auto it = metadataByTenant.find(tenantId);
		if (it != metadataByTenant.end()) {
			metadata = it->second;
		} else {
			metadataByTenant[tenantId] = metadata;
		}
		int tries = 0;
		while (true) {
			OpType opType = (OpType)Random::get().randomInt(0, OP_LAST);
			tries++;
			ASSERT(tries < 100);

			switch (opType) {
			case OP_START:
				if (metadata->activeLoads < loadCap) {
					randomOpStart(cont, metadata, tenantId);
					metadata->activeLoads++;
					return;
				}
				break;
			case OP_REGISTER_COMPLETE:
				if (!metadata->loadsStaging.empty()) {
					randomOpRegisterComplete(cont, metadata, tenantId);
					return;
				}
				break;
			case OP_WAIT_COMPLETE:
				if (!metadata->loadsWaiting.empty()) {
					randomOpWaitComplete(cont, metadata, tenantId);
					return;
				}
				break;
			case OP_ABORT:
				if (!metadata->loadsWaiting.empty() || !metadata->loadsStaging.empty()) {
					randomOpAbort(cont, metadata, tenantId);
					return;
				}
				break;
			case OP_CLEANUP:
				if (!metadata->loadsDone.empty() || !metadata->loadsAborted.empty()) {
					randomOpCleanup(cont, metadata, tenantId);
					metadata->activeLoads--;
					return;
				}
				break;
			}
		}
	}
};
WorkloadFactory<BulkLoadCorrectnessWorkload> BulkLoadCorrectnessWorkloadFactory("BulkLoadCorrectness");

} // namespace FdbApiTester
