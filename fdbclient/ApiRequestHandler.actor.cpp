/*
 * ApiRequestHandler.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/ApiRequestHandler.h"
#include "fdbclient/BlobGranuleApiImpl.h"
#include "fdbclient/BulkLoadApiImpl.h"

// this one fuction requires returning the error instead of throwing because it ends up as an FDBResult
Future<ApiResult> handleTransactionApiRequest(ISingleThreadTransaction* tr, ApiRequest req) {
	switch (req.getType()) {
	case FDBApiRequest_ReadBGDescription:
		return readBlobGranuleDescriptions(tr, req);
		break;
	default:
		return unknown_api_request();
	}
}

// all of these functions (so far) require throwing the error instead of returning because they end up as FDBFutures
Future<ApiResult> handleDatabaseOrTenantApiRequest(DatabaseContext* db,
                                                   ApiRequest req,
                                                   Optional<Reference<Tenant>> tenant) {
	switch (req.getType()) {
	case FDBApiRequest_StartBulkLoad:
		return startBulkLoad(db, req, tenant);
		break;
	case FDBApiRequest_BulkLoadAllFilesRegistered:
		return bulkLoadAllFilesRegistered(db, req, tenant);
		break;
	case FDBApiRequest_AbortBulkLoad:
		return abortBulkLoad(db, req, tenant);
		break;
	case FDBApiRequest_CleanupBulkLoad:
		return cleanupBulkLoad(db, req, tenant);
		break;
	case FDBApiRequest_WaitBulkLoadComplete:
		return waitBulkLoadComplete(db, req, tenant);
		break;
	case FDBApiRequest_BulkLoadFilesToWrite:
		return bulkLoadFilesToWrite(db, req, tenant);
		break;
	case FDBApiRequest_BulkLoadFilesWritten:
		return bulkLoadFilesWritten(db, req, tenant);
		break;
	case FDBApiRequest_ListBulkLoads:
		return listBulkLoads(db, req, tenant);
		break;
	case FDBApiRequest_GetBulkLoadStatus:
		return getBulkLoadStatus(db, req, tenant);
		break;
	default:
		throw unknown_api_request();
	}
}

ReadRangeApiResult createReadRangeApiResult(RangeResult rangeResult) {
	auto ret = ReadRangeApiResult::create();
	ret.arena().dependsOn(rangeResult.arena());
	auto data = ret.getPtr();
	static_assert(sizeof(FDBKeyValue) == sizeof(KeyValueRef));
	data->kv_arr = (FDBKeyValue*)rangeResult.begin();
	data->kv_count = rangeResult.size();
	data->more = rangeResult.more;
	return ret;
}
