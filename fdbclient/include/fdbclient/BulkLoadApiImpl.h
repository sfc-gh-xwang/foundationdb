/*
 * BulkLoadApiImpl.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef __FDBCLIENT_BULK_LOAD_API_IMPL_H__
#define __FDBCLIENT_BULK_LOAD_API_IMPL_H__
#pragma once

#include "fdbclient/IClientApi.h"
#include "fdbclient/ApiRequest.h"
#include "fdbclient/DatabaseContext.h"

Future<ApiResult> startBulkLoad(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant);
Future<ApiResult> bulkLoadAllFilesRegistered(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant);
Future<ApiResult> abortBulkLoad(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant);
Future<ApiResult> cleanupBulkLoad(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant);
Future<ApiResult> waitBulkLoadComplete(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant);

#endif