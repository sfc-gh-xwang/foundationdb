/*
 * ApiRequestHandler.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDBCLIENT_API_REQUEST_HANDLER_H
#define FDBCLIENT_API_REQUEST_HANDLER_H

#pragma once

#include "fdbclient/ApiRequest.h"
#include "fdbclient/ISingleThreadTransaction.h"
#include "fdbclient/FDBTypes.h"

Future<ApiResult> handleTransactionApiRequest(ISingleThreadTransaction* tr, ApiRequest req);
Future<ApiResult> handleDatabaseOrTenantApiRequest(DatabaseContext* db,
                                                   ApiRequest req,
                                                   Optional<Reference<Tenant>> tenant = {});

ReadRangeApiResult createReadRangeApiResult(RangeResult rangeResult);

#endif
