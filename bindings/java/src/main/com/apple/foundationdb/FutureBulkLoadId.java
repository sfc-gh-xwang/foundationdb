/*
 * FutureBulkLoadId.java
 *
 * Copyright (c) 2023 Snowflake Computing
 */

package com.apple.foundationdb;

import java.util.concurrent.Executor;

import com.apple.foundationdb.EventKeeper.Events;

class FutureBulkLoadId extends NativeFuture<byte[]> {
	FutureBulkLoadId(long cPtr, Executor executor) {
		super(cPtr);
		registerMarshalCallback(executor);
	}

	@Override
	protected byte[] getIfDone_internal(long cPtr) throws FDBException {
		return FutureBulkLoadId_get(cPtr);
	}

	private native byte[] FutureBulkLoadId_get(long cPtr) throws FDBException;
}
