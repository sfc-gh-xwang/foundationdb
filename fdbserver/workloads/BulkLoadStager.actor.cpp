/*
 * BulkLoadStager.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include <memory>
#include <ostream>
#include <stdio.h>
#include <utility>

#include "fdbrpc/Locality.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobConnectionProvider.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobGranuleFiles.actor.h"
#include "fdbclient/BulkLoad.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/BulkLoadStager.actor.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // This must be the last #include.
namespace BulkLoadHelpers {
namespace {

// Returns physical size of written file.
ACTOR Future<int64_t> writeOneFileToBlob(Reference<BlobConnectionProvider> bstore,
                                         BulkLoadFileToWriteRef toWrite,
                                         Standalone<GranuleSnapshot>* snapshot) {
	Value serialized = wait(serializeChunkedSnapshot(
	    toWrite.fileName, snapshot, 16 * 1024, toWrite.compression, toWrite.cipherKeysCtx, true));
	std::string fname = toWrite.fileName.toString();
	state int64_t physicalSize = serialized.size();
	// filename and volume is already predetermined, so just access it instead of doing createForWrite
	Reference<BackupContainerFileSystem> blobConn = bstore->getForAccess(fname);
	state std::string fileContents = serialized.toString();
	wait(blobConn->writeEntireFile(fname, fileContents));
	return physicalSize;
}

} // namespace

class BulkLoadStagerActors {
public:
	ACTOR static Future<Void> finishBulkLoad(BulkLoadStager* self) {
		if (self->bulkLoadFinished) {
			return Void();
		}
		// Write out any pending snapshots, and mark any files as written.
		wait(writeSnapshot(self));
		wait(markFilesAsWritten(self));
		// maybeReopenTenant(cx); TODO : Do this sometimes?
		wait(self->cx->bulkLoadAllFilesRegistered(self->id, self->tenant));

		try {
			wait(self->cx->waitBulkLoadComplete(self->id, self->tenant));
		} catch (Error& e) {
			// TODO: Remove this once the bulk load API is fully implemented.
			if (e.code() != error_code_api_not_implemented) {
				throw e;
			}
		}

		wait(self->cx->cleanupBulkLoad(self->id, self->tenant));

		self->bulkLoadFinished = true;
		return Void();
	}

	ACTOR static Future<Void> addRow(BulkLoadStager* self, KeyValue row, bool allowSplit) {
		state KeyValue prefixedRow = row;
		if (self->tenant.present()) {
			prefixedRow = KeyValueRef(self->tenant.get()->prefix().withSuffix(row.key), row.value);
		}
		ASSERT(self->keyRange.contains(prefixedRow.key));

		self->currentSnapshot.push_back_deep(self->currentSnapshot.arena(), prefixedRow);
		self->currentSnapshotSize += prefixedRow.key.size() + prefixedRow.value.size();
		int64_t targetSize = wait(getCurrentTargetSize(self));
		if (allowSplit && self->currentSnapshotSize >= targetSize) {
			// Write out a granule file
			// TODO: Rather than block on this, make this async so that future addRows can continue
			// without waiting on this (expensive) blob write.
			wait(writeSnapshot(self));
		}
		return Void();
	}

	ACTOR static Future<int64_t> getCurrentTargetSize(BulkLoadStager* self) {
		// Fetch more file refs if we ran out.
		if (self->availableFileRefs.empty()) {
			wait(fetchMoreFilesToWrite(self));
		}

		return self->availableFileRefs.front().targetSize;
	}

private:
	// Request more BulkLoadFileToWriteRefs. This may randomly throw away a few of the
	// fetched references to make the list of used file refs non contiguous.
	ACTOR static Future<Void> fetchMoreFilesToWrite(BulkLoadStager* self) {
		state int count = deterministicRandom()->randomInt(1, 6);

		// Sometimes throw away a bunch of file refs to make written files non-contiguous.
		if (BUGGIFY_WITH_PROB(0.2)) {
			Standalone<VectorRef<BulkLoadFileToWriteRef>> throwAwayFilesToWrite =
			    wait(self->cx->getBulkLoadFilesToWrite(self->id, count, self->tenant));
		}
		Standalone<VectorRef<BulkLoadFileToWriteRef>> filesToWrite =
		    wait(self->cx->getBulkLoadFilesToWrite(self->id, count, self->tenant));
		for (int i = 0; i < filesToWrite.size(); ++i) {
			Standalone<BulkLoadFileToWriteRef> fileToWrite(filesToWrite[i]);
			fileToWrite.arena().dependsOn(filesToWrite.arena());
			self->availableFileRefs.push_back(fileToWrite);
		}
		return Void();
	}

	ACTOR static Future<Void> writeSnapshot(BulkLoadStager* self) {
		if (self->currentSnapshot.empty()) {
			return Void();
		}
		// Fetch more file refs if we ran out.
		if (self->availableFileRefs.empty()) {
			wait(fetchMoreFilesToWrite(self));
		}
		state Standalone<BulkLoadFileToWriteRef> fileToWrite = self->availableFileRefs.front();
		self->availableFileRefs.pop_front();
		ASSERT_EQ(fileToWrite.tenantPrefix.present(), self->tenant.present());
		if (self->tenant.present()) {
			ASSERT_EQ(fileToWrite.tenantPrefix.get(), self->tenant.get()->prefix());
		}
		int64_t physicalSize = wait(writeOneFileToBlob(self->bstore, fileToWrite, &self->currentSnapshot));

		Standalone<BulkLoadWrittenFileRef> writtenFile;
		writtenFile.arena().dependsOn(fileToWrite.arena());
		writtenFile.fileId = fileToWrite.fileId;

		Key startKey(self->currentSnapshot.front().key);
		Key endKey(self->currentSnapshot.back().key);
		if (self->tenant.has_value()) {
			// The keyRange in BulkLoadWrittenFileRef does not include the tenant prefix
			startKey = startKey.removePrefix(self->tenant.get()->prefix());
			endKey = endKey.removePrefix(self->tenant.get()->prefix());
		}
		startKey = BlobGranuleKeyHelpers::truncateKey(
		    startKey, SERVER_KNOBS->BG_KEY_TUPLE_TRUNCATE_OFFSET, SERVER_KNOBS->BG_KEY_SNOWTRAM_AWARE);
		KeyRange keyRange(KeyRangeRef(startKey, keyAfter(endKey)));
		writtenFile.arena().dependsOn(keyRange.arena());
		writtenFile.keyRange = keyRange;
		writtenFile.physicalSize = physicalSize;
		writtenFile.logicalSize = 0;
		for (int i = 0; i < self->currentSnapshot.size(); ++i) {
			writtenFile.logicalSize += self->currentSnapshot[i].key.size() + self->currentSnapshot[i].value.size();
		}
		self->filesWritten.push_back(writtenFile);

		// maybeReopenTenant(cx); TODO: Do this sometimes?

		// Flip a coin and mark files as written.
		if (deterministicRandom()->coinflip()) {
			wait(markFilesAsWritten(self));
		}

		self->currentSnapshot.clear();
		self->currentSnapshotSize = 0;
		return Void();
	}

	ACTOR static Future<Void> markFilesAsWritten(BulkLoadStager* self) {
		if (self->filesWritten.empty()) {
			return Void();
		}

		state Standalone<VectorRef<BulkLoadWrittenFileRef>> writtenFiles;
		for (int i = 0; i < self->filesWritten.size(); ++i) {
			writtenFiles.arena().dependsOn(self->filesWritten[i].arena());
			writtenFiles.push_back(writtenFiles.arena(), self->filesWritten[i]);
		}
		wait(self->cx->markBulkLoadFilesWritten(self->id, writtenFiles, self->tenant));
		// Sometimes re-register the files again. This should be idempotent.
		if (BUGGIFY_WITH_PROB(0.1)) {
			wait(self->cx->markBulkLoadFilesWritten(self->id, writtenFiles, self->tenant));
		}
		self->filesWritten.clear();

		return Void();
	}
};

Future<Void> BulkLoadStager::addRow(KeyValue row) {
	bool allowSplit = true;
	if (previousKey.has_value()) {
		// Rows must be added in strictly increasing order.
		// Do this check before making the call into the ACTOR to ensure
		// this is always serialized and ordered with other calls to `addRow`.
		ASSERT_GT(row.key, previousKey.get());
		allowSplit = BlobGranuleKeyHelpers::canSplit(previousKey.get(),
		                                             row.key,
		                                             SERVER_KNOBS->BG_KEY_TUPLE_TRUNCATE_OFFSET,
		                                             SERVER_KNOBS->BG_KEY_SNOWTRAM_AWARE);
	}
	previousKey = row.key;
	return BulkLoadStagerActors::addRow(this, row, allowSplit);
}

Future<Void> BulkLoadStager::finishBulkLoad() {
	return BulkLoadStagerActors::finishBulkLoad(this);
}

Future<int64_t> BulkLoadStager::getCurrentTargetSizeBytes() {
	return BulkLoadStagerActors::getCurrentTargetSize(this);
}

} // namespace BulkLoadHelpers
