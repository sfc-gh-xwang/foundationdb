/**
 * UpdateStream.h
 */

#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "flow/FastRef.h"

class UpdateStream : public ReferenceCounted<UpdateStream> {
public:
	struct Update {
		Key key;
		Optional<Value> value;
		uint64_t index;
		Update(Key const& key, Optional<Value> const& value, uint64_t index);
	};

private:
	friend class UpdateStreamImpl;

	Future<Void> updateStreamFuture;
	Key metadataPrefix;
	int batchSize;
	UID leaseId;
	Optional<uint64_t> lastUpdateIndex;
	PromiseStream<Update> results;

	Key getLeaseKey() const;

public:
	UpdateStream(Key metadataPrefix, int batchSize);

	// Call this before reading a snapshot of the watched data.
	// All updates after calling start will be visible made visible
	// through getUpdates.
	//
	// Returns the current index number.
	Future<Optional<uint64_t>> start(Database);

	// After a snapshot of watched data has been read, call
	// getUpdates and listen to the returned FutureStream
	// to consume updates to the watched data.
	FutureStream<Update> getUpdates(Database);

	// TODO: Comment
	// Does not commit!
	static Future<Void> update(Reference<ReadYourWritesTransaction>,
	                           Key const&,
	                           Optional<Value> const&,
	                           Key metadataPrefix);

	UID getLeaseId() const;
};
