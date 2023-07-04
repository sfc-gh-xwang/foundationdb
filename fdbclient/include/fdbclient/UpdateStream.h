/**
 * UpdateStream.h
 */

#pragma once

#include "fdbclient/IClientApi.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/FastRef.h"

/**
 * The UpdateStream class provides a way to consume a stream of updates
 * to many keys while only using 2 watches. It is optimized for the use case
 * of a single consumer monitoring a large number of keys that are infrequently
 * updated.
 *
 * Each UpdateStream object watches for updates in a keyrange specified
 * by the metadataPrefix provided in the constructor. Within this metadata keyrange,
 * the following data is stored:
 *
 *   - A persistent update queue: Update keys have form
 *     <metadataPrefix>/updates/<updateIndex>. The value stored at an update
 *     key is the key of the data that was updated.
 *   - A leaseId: The lease key has form <metadataPrefix>/lease. The lease
 *     prevents multiple consumers from concurrently consuming from the update queue.
 *     It is safe to attempt to run multiple concurrent consumers, but performance
 *     will suffer.
 *
 * When an UpdateStream is created, it must first call the start method to
 * learn about the last known update in the update queue. Then it is the
 * responisibility of the consumer to read a (possibly inconsistent) snapshot
 * of whatever data they are interested in (the UpdateStream class does not
 * provide snapshotting functionality). Once the snapshot has been read,
 * the consumer calls the getUpdates method to access a FutureStream of
 * all updates (including those that may have been missed while reading
 * a snapshot).
 *
 * Producers can use the static UpdateStream::update method to register
 * their updates in the persistent update queue, guaranteeing that if there
 * is a live consumer, the consumer will be notified of these updates.
 * Producers only need to share a common knowledge of the metadataPrefix
 * with consumers. Only set and clear updates are supported, so all updates
 * are idempotent.
 *
 * Consumers are guaranteed to eventually see all updates exactly once in
 * the FutureStream returned by getUpdates. If there is a risk that an update
 * may be missed or delivered twice (either because a transaction to consume from the
 * persistent update queue received a commit_unknown_result error or because another
 * consumer has started consuming from the same queue), a restart_update_stream
 * error is sent to the FutureStream from getUpdates. When this error is received,
 * the consumer should again:
 *
 *   - Create a new UpdateStream object
 *   - Call UpdateStream::start
 *   - Read a snapshot of the tracked data
 *   - Call UpdateStream::getUpdates
 *
 * commit_unknown_result errors are rare and it is not recommended to run concurrent
 * consumers, so the restart_update_stream error should be rare if UpdateStream is
 * used as intended.
 *
 * NOTE: Because updates are delivered exactly once and are guaranteed to be idempotent,
 * there is potential to either:
 *
 *   - Support non-idempotent updates or
 *   - Handle commit_unknown_result more elegantly and avoid restarting
 *     by potentially consuming some updates multiple times.
 *
 * As of the time of writing this class, neither usecase is necessary, but
 * future usecases may require one of the above enhancements.
 */

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
	bool accessSystemKeys;
	bool immediatePriority;

	Key getLeaseKey() const;
	void applyTransactionOptions(Reference<ReadYourWritesTransaction>) const;

public:
	UpdateStream(Key metadataPrefix, int batchSize, bool accessSystemKeys, bool immediatePriority);

	// Call this before reading a snapshot of the watched data.
	// All updates after calling start will be made visible
	// through getUpdates.
	//
	// Returns the current index number.
	Future<Optional<uint64_t>> start(Database);

	// After a snapshot of watched data has been read, call
	// getUpdates and listen to the returned FutureStream
	// to consume updates to the watched data.
	FutureStream<Update> getUpdates(Database);

	// Called by producers to register updates in the
	// persistent update queue
	static Future<Void> update(Reference<ReadYourWritesTransaction>,
	                           Key const&,
	                           Optional<Value> const&,
	                           Key metadataPrefix);
	static Future<Void> update(Reference<ITransaction>, Key const&, Optional<Value> const&, Key metadataPrefix);

	UID getLeaseId() const;
};
