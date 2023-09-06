/**
 * FutureDouble.java
 */

package com.apple.foundationdb;

import java.util.concurrent.Executor;

class FutureDouble extends NativeFuture<Double> {
	FutureDouble(long cPtr, Executor executor) {
		super(cPtr);
		registerMarshalCallback(executor);
	}

	@Override
	protected Double getIfDone_internal(long cPtr) throws FDBException {
		return FutureDouble_get(cPtr);
	}

	private native double FutureDouble_get(long cPtr) throws FDBException;
}
