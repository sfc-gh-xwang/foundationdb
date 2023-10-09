/*
 * FutureBulkLoadStatus.java
 *
 * Copyright (c) 2023 Snowflake Computing
 */

package com.apple.foundationdb;

import java.util.concurrent.Executor;

import com.apple.foundationdb.EventKeeper.Events;

class FutureBulkLoadStatus extends NativeFuture<BulkLoadStatus> {
	FutureBulkLoadStatus(long cPtr, Executor executor) {
		super(cPtr);
		registerMarshalCallback(executor);
	}

	@Override
	protected BulkLoadStatus getIfDone_internal(long cPtr) throws FDBException {
		return FutureBulkLoadStatus_get(cPtr);
	}

	private native BulkLoadStatus FutureBulkLoadStatus_get(long cPtr) throws FDBException;
}
