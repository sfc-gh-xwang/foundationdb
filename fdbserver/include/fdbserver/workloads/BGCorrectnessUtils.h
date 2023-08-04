/*
 * BGCorrectnessUtils.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDBSERVER_WORKLOAD_BGCORRECTNESSUTILS_H
#define FDBSERVER_WORKLOAD_BGCORRECTNESSUTILS_H
#pragma once

#include "flow/Arena.h"
#include "fdbclient/FDBTypes.h"

Value genVal(uint32_t val, uint16_t valLen);
bool validateValue(const Value& v, uint32_t val, uint16_t valLen);

#endif