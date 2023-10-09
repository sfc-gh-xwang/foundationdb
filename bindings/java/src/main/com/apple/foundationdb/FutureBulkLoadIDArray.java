/*
 * FutureBulkLoadIDArray.java
 *
 * Copyright (c) 2023 Snowflake Computing
 */

package com.apple.foundationdb;

import java.util.concurrent.Executor;

class FutureBulkLoadIDArray extends NativeFuture<KeyArrayResult> {
	FutureBulkLoadIDArray(long cPtr, Executor executor) {
		super(cPtr);
		registerMarshalCallback(executor);
	}

	@Override
	protected KeyArrayResult getIfDone_internal(long cPtr) throws FDBException {
		return FutureBulkLoadIDArray_get(cPtr);
	}

	private native KeyArrayResult FutureBulkLoadIDArray_get(long cPtr) throws FDBException;
}
