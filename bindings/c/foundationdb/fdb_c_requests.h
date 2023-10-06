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
	FDBApiRequest_BulkLoadFilesToWrite = 7, //
	FDBApiRequest_BulkLoadFilesWritten = 8, //
	FDBApiRequest_ListBulkLoads = 9, //
	FDBApiRequest_GetBulkLoadStatus = 10
};

enum FDBApiResultType {
	FDBApiResult_Invalid = 0, //
	FDBApiResult_Error = -1, //
	FDBApiResult_ReadBGDescription = 1, //
	FDBApiResult_ReadRange = 2, //
	FDBApiResult_ReadBGMutations = 3, //
	FDBApiResult_BulkLoadID = 4, //
	FDBApiResult_Void = 5, //
	FDBApiResult_BulkLoadFilesToWrite = 6, //
	FDBApiResult_ListBulkLoads = 7, //
	FDBApiResult_GetBulkLoadStatus = 8
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

typedef struct FDBBulkLoadFilesToWriteRequest_ {
	FDBRequestHeader* header;
	FDBBulkLoadID id;
	int count;
} FDBBulkLoadFilesToWriteRequest;

typedef struct FDBBulkLoadFilesWrittenRequest_ {
	FDBRequestHeader* header;
	FDBBulkLoadID id;
	FDBBulkLoadWrittenFile** files_arr;
	int files_count;
} FDBBulkLoadFilesWrittenRequest;

typedef struct FDBListBulkLoadsRequest_ {
	FDBRequestHeader* header;
	FDBKeyRange key_range;
} FDBListBulkLoadsRequest;

typedef struct FDBGetBulkLoadStatusRequest_ {
	FDBRequestHeader* header;
	FDBBulkLoadID id;
} FDBGetBulkLoadStatusRequest;

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

typedef struct FDBBulkLoadFilesToWriteResult_ {
	FDBResultHeader* header;
	FDBBulkLoadFileToWrite** files_arr;
	int files_count;
} FDBBulkLoadFilesToWriteResult;

typedef struct FDBListBulkLoadsResult_ {
	FDBResultHeader* header;
	FDBBulkLoadID** ids_arr;
	int ids_count;
} FDBListBulkLoadsResult;

typedef struct FDBGetBulkLoadStatusResult_ {
	FDBResultHeader* header;
	FDBBulkLoadStatus status;
} FDBGetBulkLoadStatusResult;

#ifdef __cplusplus
}
#endif
#endif