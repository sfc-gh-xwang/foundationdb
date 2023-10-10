/*
 * BulkLoadTracker.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/BulkLoadTracker.h"
#include "fdbclient/BulkLoadMetadata.h"

ACTOR Future<Void> trackNewBulkLoadsActor(BulkLoadTracker* self) {
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
				std::set<BulkLoadIDKey> newActiveBulkLoads;
				for (auto& it : result.results) {
					newActiveBulkLoads.insert(it.first);
					if (!self->activeBulkLoads.count(it.first)) {
						self->addActor.send(self->handler(it.first, it.second.stateCode));
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

Future<Void> BulkLoadTracker::trackNewBulkLoads() {
	return trackNewBulkLoadsActor(this);
}