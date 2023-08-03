/*
 * BulkLoader.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDBSERVER_BULKLOADER_H
#define FDBSERVER_BULKLOADER_H
#pragma once

#include "fdbclient/BulkLoaderInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/BlobWorker.h"

// TODO: eventually not shared state?

struct BulkLoaderStats {
	CounterCollection cc;
	Future<Void> logger;

	Counter filesToWriteRequests;
	Counter filesWrittenRequests;
	Counter filesToWriteCreated;
	Counter filesWritten;
	Counter startRequests;
	Counter stateChangeRequests;
	Counter waitCompleteRequests;

	Counter errors;

	int activeRequests;

	explicit BulkLoaderStats(UID id, double interval)
	  : cc("BulkLoaderStats", id.toString()),
	    filesToWriteRequests("FilesToWriteRequests", cc),
	    filesWrittenRequests("FilesWrittenRequests", cc),
	    filesToWriteCreated("FilesToWriteCreated", cc),
	    filesWritten("FilesWritten", cc),
	    startRequests("StartRequests", cc),
	    stateChangeRequests("StateChangeRequests", cc),
	    waitCompleteRequests("WaitCompleteRequests", cc),
	    errors("Errors", cc),
	    activeRequests(0) {
		specialCounter(cc, "ActiveRequests", [this]() { return this->activeRequests; });
		logger = cc.traceCounters("BulkLoaderMetrics", id, interval, "BulkLoaderMetrics");
	}
};

struct BulkLoaderData : NonCopyable, ReferenceCounted<BulkLoaderData> {
	UID id;
	Database db;
	PromiseStream<Future<Void>> addActor;
	BulkLoaderStats stats;

	BulkLoaderData(UID id, Database db) : id(id), db(db), stats(id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL) {}
};

Future<Void> bulkLoader(const BulkLoaderInterface& self,
                        const Reference<BulkLoaderData>& blData,
                        const Reference<BlobWorkerData>& bwData);

#endif