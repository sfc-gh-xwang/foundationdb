/*
 * BulkLoadStager.actor.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include <deque>
#include <memory>
#include <stdio.h>
#include <utility>

#include "fdbserver/workloads/BGCorrectnessUtils.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobConnectionProvider.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobGranuleFiles.actor.h"
#include "fdbclient/BulkLoad.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/genericactors.actor.h"

#pragma once

namespace BulkLoadHelpers {

class BulkLoadStager : public ReferenceCounted<BulkLoadStager> {
public:
	BulkLoadStager(Database database,
	               Reference<BlobConnectionProvider> bstore,
	               BulkLoadID bulkLoadId,
	               KeyRange keyRange,
	               Optional<Reference<Tenant>> tenant)
	  : cx(database),
	    bstore(bstore),
	    id(bulkLoadId),
	    keyRange(keyRange),
	    tenant(tenant){};

	~BulkLoadStager() = default;

	// Adds a new row to the bulk load. The new key must be strictly greater than the previous key
	// added. Adding a row *may* cause a new blob file to be written out (e.g. if the rows added so far)
	// are at the granule threshold.
	Future<Void> addRow(KeyValue row);

	// Complete the bulk load by writing out any pending blob files and marking all files as written.
	// This method *must* be called before this BulkLoadStager is destroyed.
	Future<Void> finishBulkLoad();

	// Returns the target size in bytes for the next granule that will be written to blob storage.
	Future<int64_t> getCurrentTargetSizeBytes();

	void addref() { ::ReferenceCounted<BulkLoadStager>::addref(); }
	void delref() { ::ReferenceCounted<BulkLoadStager>::delref(); }

private:
	friend class BulkLoadStagerActors;

	Database cx;
	Reference<BlobConnectionProvider> bstore;
	const BulkLoadID id;
	const KeyRange keyRange;
	const Optional<Reference<Tenant>> tenant;
	FlowMutex mutex;

	Optional<Key> previousKey;
	Standalone<GranuleSnapshot> currentSnapshot;
	int64_t currentSnapshotSize = 0;
	bool bulkLoadFinished = false;
	// These are vectors of Standalones instead of Standalone<VectorRef> so that
	// we can depend on the arena(s) of individual elements instead of the entire VectorRef
	std::deque<Standalone<BulkLoadFileToWriteRef>> availableFileRefs;
	std::deque<Standalone<BulkLoadWrittenFileRef>> filesWritten;

	std::deque<Future<Void>> pendingSnapshotWrites;
};

} // namespace BulkLoadHelpers
