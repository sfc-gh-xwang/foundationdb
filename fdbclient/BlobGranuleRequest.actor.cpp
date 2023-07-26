/*
 * BlobGranuleRequest.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/BlobGranuleRequest.actor.h"

ACTOR Future<Standalone<VectorRef<KeyRangeRef>>> getBlobRanges(Transaction* tr, KeyRange range, int batchLimit) {
	state Standalone<VectorRef<KeyRangeRef>> blobRanges;
	state Key beginKey = range.begin;

	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

	loop {

		state RangeResult results =
		    wait(krmGetRangesUnaligned(tr, blobRangeKeys.begin, KeyRangeRef(beginKey, range.end), 2 * batchLimit + 2));

		blobRanges.arena().dependsOn(results.arena());
		for (int i = 0; i < results.size() - 1; i++) {
			if (isBlobRangeActive(results[i].value)) {
				blobRanges.push_back(blobRanges.arena(), KeyRangeRef(results[i].key, results[i + 1].key));
			}
			if (blobRanges.size() == batchLimit) {
				return blobRanges;
			}
		}

		if (!results.more) {
			return blobRanges;
		}
		beginKey = results.back().key;
	}
}