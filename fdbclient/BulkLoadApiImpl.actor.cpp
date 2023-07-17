/*
 * BulkLoadApiImpl.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/BulkLoadApiImpl.h"
#include "fdbclient/ApiRequestHandler.h"
#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<ApiResult> startBulkLoadImpl(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBStartBulkLoadRequest>();
	BulkLoadID id = wait(db->startBulkLoad((KeyRangeRef&)input->key_range, tenant));

	auto res = TypedApiResult<FDBBulkLoadIDResult>::create(FDBApiResult_BulkLoadID);
	Arena& arena = res.arena();
	arena.dependsOn(id.arena());

	auto resp = res.getPtr();
	resp->id.begin = id.begin();
	resp->id.length = id.size();
	return res;
}

Future<ApiResult> startBulkLoad(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return startBulkLoadImpl(db, req, tenant);
}

ACTOR Future<ApiResult> bulkLoadAllFilesRegisteredImpl(DatabaseContext* db,
                                                       ApiRequest req,
                                                       Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBBulkLoadAllFilesRegisteredRequest>();
	BulkLoadID id((StringRef&)input->id);
	wait(db->bulkLoadAllFilesRegistered(id, tenant));

	// result is void but extensible this way
	return TypedApiResult<FDBVoidResult>::create(FDBApiResult_Void);
}

Future<ApiResult> bulkLoadAllFilesRegistered(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return bulkLoadAllFilesRegisteredImpl(db, req, tenant);
}

ACTOR Future<ApiResult> abortBulkLoadImpl(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBAbortBulkLoadRequest>();
	BulkLoadID id((StringRef&)input->id);
	wait(db->abortBulkLoad(id, tenant));

	// result is void but extensible this way
	return TypedApiResult<FDBVoidResult>::create(FDBApiResult_Void);
}

Future<ApiResult> abortBulkLoad(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return abortBulkLoadImpl(db, req, tenant);
}

ACTOR Future<ApiResult> cleanupBulkLoadImpl(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBCleanupBulkLoadRequest>();
	BulkLoadID id((StringRef&)input->id);
	wait(db->cleanupBulkLoad(id, tenant));

	// result is void but extensible this way
	return TypedApiResult<FDBVoidResult>::create(FDBApiResult_Void);
}

Future<ApiResult> cleanupBulkLoad(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return cleanupBulkLoadImpl(db, req, tenant);
}

ACTOR Future<ApiResult> waitBulkLoadCompleteImpl(DatabaseContext* db,
                                                 ApiRequest req,
                                                 Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBCleanupBulkLoadRequest>();
	BulkLoadID id((StringRef&)input->id);
	wait(db->waitBulkLoadComplete(id, tenant));

	// result is void but extensible this way
	return TypedApiResult<FDBVoidResult>::create(FDBApiResult_Void);
}

Future<ApiResult> waitBulkLoadComplete(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return waitBulkLoadCompleteImpl(db, req, tenant);
}

ACTOR Future<ApiResult> getBulkLoadStatusImpl(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBCleanupBulkLoadRequest>();
	BulkLoadID id((StringRef&)input->id);
	BulkLoadStatus status = wait(db->getBulkLoadStatus(id, tenant));

	auto res = TypedApiResult<FDBBulkLoadStatusResult>::create(FDBApiResult_BulkLoadStatus);

	auto resp = res.getPtr();
	resp->status.state = static_cast<uint8_t>(status.state);

	return res;
}

Future<ApiResult> getBulkLoadStatus(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return getBulkLoadStatusImpl(db, req, tenant);
}

ACTOR Future<ApiResult> listBulkLoadsImpl(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	Standalone<VectorRef<BulkLoadIDRef>> ids = wait(db->listBulkLoads(true, tenant));

	auto res = TypedApiResult<FDBBulkLoadListResult>::create(FDBApiResult_BulkLoadList);
	Arena& arena = res.arena();
	arena.dependsOn(ids.arena());

	auto resp = res.getPtr();
	resp->ids_count = ids.size();

	resp->ids_arr = new (arena) FDBBulkLoadID*[ids.size()];
	for (int i = 0; i < ids.size(); i++) {
		resp->ids_arr[i] = new (arena) FDBBulkLoadID();
		resp->ids_arr[i]->begin = ids[i].begin();
		resp->ids_arr[i]->length = ids[i].size();
	}

	return res;
}

Future<ApiResult> listBulkLoads(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return listBulkLoadsImpl(db, req, tenant);
}