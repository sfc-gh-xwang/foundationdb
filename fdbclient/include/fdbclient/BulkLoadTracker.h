/*
 * BulkLoadTracker.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once
#ifndef FDBCLIENT_BULKLOADTRACKER_H
#define FDBCLIENT_BULKLOADTRACKER_H

#include "fdbclient/BulkLoad.h"
#include "fdbclient/DatabaseContext.h"

// used for fdb components to track active bulk loads and potentially perform work for different stages of each active
// bulk load
// FIXME: auto-abort long-running bulk loads that are still stuck in staging
// FIXME: clean up successfully completed/aborted bulk load metadata after a while (need to keep around for a bit so
// caller can know for sure whether it was successful or not)
//        and/or allow client to explicitly clean up bulk loads when it knows for sure the load is complete
struct BulkLoadTracker {
	typedef std::function<Future<Void>(const BulkLoadIDKey&, const BulkLoadStatus::State&)> Handler;

	Database db;
	Handler handler;
	PromiseStream<Future<Void>> addActor;
	bool started;

	// FIXME: define hash function and do unordered set
	std::set<BulkLoadIDKey> activeBulkLoads;

	BulkLoadTracker() {}

	BulkLoadTracker(Database db, Handler handler) : db(db), handler(handler), started(false) {}

	Future<Void> trackNewBulkLoads();

	Future<Void> start() {
		ASSERT(!started);
		started = true;
		Future<Void> collection = actorCollection(addActor.getFuture());
		addActor.send(trackNewBulkLoads());
		return collection;
	}
};

#endif