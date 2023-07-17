/*
 * fdb_c_requests.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDB_C_REQUESTS_H
#define FDB_C_REQUESTS_H
#pragma once

#include "fdb_c_types.h"

#ifdef __cplusplus
extern "C" {
#endif

enum FDBApiRequestType {
	FDBApiRequest_Invalid = 0, //
	FDBApiRequest_ReadBGDescription = 1, //
	FDBApiRequest_StartBulkLoad = 2, //
	FDBApiRequest_BulkLoadAllFilesRegistered = 3, //
	FDBApiRequest_AbortBulkLoad = 4, //
	FDBApiRequest_CleanupBulkLoad = 5, //
	FDBApiRequest_WaitBulkLoadComplete = 6, //
	FDBApiRequest_GetBulkLoadStatus = 7, //
	FDBApiRequest_ListBulkLoads = 8, //
};

enum FDBApiResultType {
	FDBApiResult_Invalid = 0, //
	FDBApiResult_Error = -1, //
	FDBApiResult_ReadBGDescription = 1, //
	FDBApiResult_ReadRange = 2, //
	FDBApiResult_ReadBGMutations = 3, //
	FDBApiResult_BulkLoadID = 4, //
	FDBApiResult_Void = 5, //
	FDBApiResult_BulkLoadStatus = 4, //
	FDBApiResult_BulkLoadList = 4, //
};

typedef struct FDBRequestHeader_ FDBRequestHeader;
typedef struct FDBResultHeader_ FDBResultHeader;

typedef struct FDBReadBGDescriptionRequest_ {
	FDBRequestHeader* header;
	FDBKeyRange key_range;
	int64_t begin_version;
	int64_t read_version;
	int64_t mutation_byte_limit;
} FDBReadBGDescriptionRequest;

typedef struct FDBStartBulkLoadRequest_ {
	FDBRequestHeader* header;
	FDBKeyRange key_range;
} FDBStartBulkLoadRequest;

typedef struct FDBBulkLoadAllFilesRegisteredRequest_ {
	FDBRequestHeader* header;
	FDBBulkLoadID id;
} FDBBulkLoadAllFilesRegisteredRequest;

typedef struct FDBAbortBulkLoadRequest_ {
	FDBRequestHeader* header;
	FDBBulkLoadID id;
} FDBAbortBulkLoadRequest;

typedef struct FDBCleanupBulkLoadRequest_ {
	FDBRequestHeader* header;
	FDBBulkLoadID id;
} FDBCleanupBulkLoadRequest;

typedef struct FDBWaitBulkLoadCompleteRequest_ {
	FDBRequestHeader* header;
	FDBBulkLoadID id;
} FDBWaitBulkLoadCompleteRequest;

typedef struct FDBGetBulkLoadStatusRequest_ {
	FDBRequestHeader* header;
	FDBBulkLoadID id;
} FDBGetBulkLoadStatusRequest;

typedef struct FDBListBulkLoadsRequest_ {
	FDBRequestHeader* header;
} FDBListBulkLoadsRequest;

typedef struct FDBErrorResult_ {
	FDBResultHeader* header;
	fdb_error_t error;
} FDBErrorResult;

typedef struct FDBReadBGDescriptionResult_ {
	FDBResultHeader* header;
	FDBBGFileDescriptionV2** desc_arr;
	int desc_count;
	int64_t read_version;
} FDBReadBGDescriptionResult;

typedef struct FDBReadRangeResult_ {
	FDBResultHeader* header;
	FDBKeyValue* kv_arr;
	int kv_count;
	fdb_bool_t more;
} FDBReadRangeResult;

typedef struct FDBReadBGMutationsResult_ {
	FDBResultHeader* header;
	FDBBGMutation* mutation_arr;
	int mutation_count;
} FDBReadBGMutationsResult;

typedef struct FDBBulkLoadIDResult_ {
	FDBResultHeader* header;
	FDBBulkLoadID id;
} FDBBulkLoadIDResult;

typedef struct FDBVoidResult_ {
	FDBResultHeader* header;
} FDBVoidResult;

typedef struct FDBBulkLoadStatusResult_ {
	FDBResultHeader* header;
	FDBBulkLoadStatus status;
} FDBBulkLoadStatusResult;

typedef struct FDBBulkLoadListResult_ {
	FDBResultHeader* header;
	FDBBulkLoadID** ids_arr;
	int ids_count;
} FDBBulkLoadListResult;

#ifdef __cplusplus
}
#endif
#endif