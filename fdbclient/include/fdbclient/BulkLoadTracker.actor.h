/*
 * BulkLoadTracker.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_BULK_LOAD_TRACKER_ACTOR_G_H)
#define FDBCLIENT_BULK_LOAD_TRACKER_ACTOR_G_H
#include "fdbclient/BulkLoadTracker.actor.g.h"
#elif !defined(FDBCLIENT_BULK_LOAD_TRACKER_ACTOR_H)
#define FDBCLIENT_BULK_LOAD_TRACKER_ACTOR_H

#include "fdbclient/BulkLoad.h"
#include "fdbclient/BulkLoadMetadata.h"
#include "fdbclient/DatabaseContext.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// used for fdb components to track active bulk loads and potentially perform work for different stages of each active
// bulk load
// FIXME: auto-abort long-running bulk loads that are still stuck in staging
// FIXME: clean up successfully completed/aborted bulk load metadata after a while (need to keep around for a bit so
// caller can know for sure whether it was successful or not)
//        and/or allow client to explicitly clean up bulk loads when it knows for sure the load is complete
struct BulkLoadTracker {
	typedef std::function<Future<Void>(const BulkLoadID&, const BulkLoadState&)> Handler;

	Database db;
	Handler handler;
	PromiseStream<Future<Void>> addActor;
	bool started;

	std::unordered_set<BulkLoadID> activeBulkLoads;

	BulkLoadTracker() {}

	BulkLoadTracker(Database db, Handler handler, PromiseStream<Future<Void>> addActor)
	  : db(db), handler(handler), addActor(addActor), started(false) {}

	void start() {
		ASSERT(!started);
		started = true;
		addActor.send(trackNewBulkLoads(this));
	}

	// FIXME: once we add change logs to key-backed objects, switch to that instead of doing explicit diffs
	ACTOR Future<Void> trackNewBulkLoads(BulkLoadTracker* self) {
		loop {
			state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);
			state Future<Void> waitFuture;
			loop {
				try {
					SystemDBWriteLockedNow(self->db.getReference())->setOptions(tr);
					// FIXME: this could use the client api to list bulk loads, if it didn't also need to
					// transactionally start a watch at the same time
					BulkLoadMetadata::StatusMap::RangeResultType result =
					    wait(BulkLoadMetadata::statusMap().getRange(tr, {}, {}, CLIENT_KNOBS->TOO_MANY));
					ASSERT(!result.more);
					std::unordered_set<BulkLoadID> newActiveBulkLoads;
					for (auto& it : result.results) {
						newActiveBulkLoads.insert(it.first);
						if (!self->activeBulkLoads.count(it.first)) {
							self->addActor.send(self->handler(it.first, it.second.state));
						}
					}
					self->activeBulkLoads = newActiveBulkLoads;

					waitFuture = BulkLoadMetadata::onChange().watch(tr);
					wait(tr->commit());
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}

			wait(waitFuture);
		}
	}
};

#include "flow/unactorcompiler.h"

#endif