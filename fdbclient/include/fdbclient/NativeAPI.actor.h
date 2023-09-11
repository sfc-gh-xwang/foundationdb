/*
 * NativeAPI.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "flow/IRandom.h"
#include "fdbclient/Tracing.h"
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_NATIVEAPI_ACTOR_G_H)
#define FDBCLIENT_NATIVEAPI_ACTOR_G_H
#include "fdbclient/NativeAPI.actor.g.h"
#elif !defined(FDBCLIENT_NATIVEAPI_ACTOR_H)
#define FDBCLIENT_NATIVEAPI_ACTOR_H

#include "flow/BooleanParam.h"
#include "flow/flow.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClusterConnectionRecord.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantLookupInfo.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/StorageCheckpoint.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/TenantName.h"
#include "flow/actorcompiler.h" // has to be last include

// CLIENT_BUGGIFY should be used to randomly introduce failures at run time (like BUGGIFY but for client side testing)
// Unlike BUGGIFY, CLIENT_BUGGIFY can be enabled and disabled at runtime.
#define CLIENT_BUGGIFY_WITH_PROB(x)                                                                                    \
	(getSBVar(__FILE__, __LINE__, BuggifyType::Client) && deterministicRandom()->random01() < (x))
#define CLIENT_BUGGIFY CLIENT_BUGGIFY_WITH_PROB(P_BUGGIFIED_SECTION_FIRES[int(BuggifyType::Client)])

FDB_BOOLEAN_PARAM(UseProvisionalProxies);
FDB_BOOLEAN_PARAM(UseRawAccess);

// Incomplete types that are reference counted
class DatabaseContext;
template <>
void addref(DatabaseContext* ptr);
template <>
void delref(DatabaseContext* ptr);

void validateOptionValuePresent(Optional<StringRef> value);
void validateOptionValueNotPresent(Optional<StringRef> value);

void enableClientInfoLogging();

class Database {
public:
	// Creates a database object that represents a connection to a cluster
	// This constructor uses a preallocated DatabaseContext that may have been created
	// on another thread
	static Database createDatabase(Reference<IClusterConnectionRecord> connRecord,
	                               int apiVersion,
	                               IsInternal internal = IsInternal::True,
	                               LocalityData const& clientLocality = LocalityData(),
	                               DatabaseContext* preallocatedDb = nullptr);

	static Database createDatabase(std::string connFileName,
	                               int apiVersion,
	                               IsInternal internal = IsInternal::True,
	                               LocalityData const& clientLocality = LocalityData());

	static Database createSimulatedExtraDatabase(std::string connectionString,
	                                             Optional<TenantName> defaultTenant = Optional<TenantName>());

	Database() {} // an uninitialized database can be destructed or reassigned safely; that's it
	void operator=(Database const& rhs) { db = rhs.db; }
	Database(Database const& rhs) : db(rhs.db) {}
	Database(Database&& r) noexcept : db(std::move(r.db)) {}
	void operator=(Database&& r) noexcept { db = std::move(r.db); }

	// For internal use by the native client:
	explicit Database(Reference<DatabaseContext> cx) : db(cx) {}
	explicit Database(DatabaseContext* cx) : db(cx) {}
	inline DatabaseContext* getPtr() const { return db.getPtr(); }
	inline DatabaseContext* extractPtr() { return db.extractPtr(); }
	DatabaseContext* operator->() const { return db.getPtr(); }
	Reference<DatabaseContext> getReference() const { return db; }

	const UniqueOrderedOptionList<FDBTransactionOptions>& getTransactionDefaults() const;

private:
	Reference<DatabaseContext> db;
};

void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>());

// Configures the global networking machinery
void setupNetwork(uint64_t transportId = 0, UseMetrics = UseMetrics::False);

// This call blocks while the network is running.  To use the API in a single-threaded
//  environment, the calling program must have ACTORs already launched that are waiting
//  to use the network.  In this case, the program can terminate by calling stopNetwork()
//  from a callback, thereby releasing this call to return.  In a multithreaded setup
//  this call can be called from a dedicated "networking" thread.  All the network-based
//  callbacks will happen on this second thread.  When a program is finished, the
//  call stopNetwork (from a non-networking thread) can cause the runNetwork() call to
//  return.
//
// Throws network_already_setup if g_network has already been initialized
void runNetwork();

// See above.  Can be called from a thread that is not the "networking thread"
//
// Throws network_not_setup if g_network has not been initialized
void stopNetwork();

struct StorageMetrics;

class ReadYourWritesTransaction; // workaround cyclic dependency

struct Watch : public ReferenceCounted<Watch>, NonCopyable {
	Key key;
	Optional<Value> value;
	bool valuePresent;
	Optional<Value> setValue;
	bool setPresent;
	Promise<Void> onChangeTrigger;
	Promise<Void> onSetWatchTrigger;
	Future<Void> watchFuture;
	Optional<ReadOptions> readOptions;

	Watch() : valuePresent(false), setPresent(false), watchFuture(Never()) {}
	Watch(Key key) : key(key), valuePresent(false), setPresent(false), watchFuture(Never()) {}
	Watch(Key key, Optional<Value> val)
	  : key(key),
	    value(val),
	    valuePresent(true),
	    setPresent(false),
	    watchFuture(Never()) {}

	void setWatch(Future<Void> watchFuture);
};

class Tenant : public ReferenceCounted<Tenant>, public FastAllocated<Tenant>, NonCopyable {
public:
	Tenant(Database cx, TenantName name);
	explicit Tenant(Database cx, int64_t id);
	Tenant(Database cx, Future<TenantLookupInfo> tenantLookupFuture, Optional<TenantName> name);
	Tenant(Database cx, TenantMapEntry entry);

	static Tenant* allocateOnForeignThread() { return (Tenant*)Tenant::operator new(sizeof(Tenant)); }

	Future<Void> ready() const { return success(lookupFuture) && success(dbFuture); }
	int64_t id() const;
	Database database() const;
	Optional<TenantGroupName> tenantGroup() const;
	Future<TenantLookupInfo> getLookupFuture() const;
	KeyRef prefix() const;
	void setOption(FDBTenantOptions::Option option, Optional<StringRef> value = Optional<StringRef>());
	std::string description() const;

	Optional<TenantName> name;

	const UniqueOrderedSensitiveOptionList<FDBTransactionOptions>& getSensitiveTransactionDefaults() const {
		return sensitiveTransactionDefaults;
	}

private:
	mutable int64_t bigEndianId = -1;
	Future<TenantLookupInfo> lookupFuture;
	Future<Database> dbFuture;

	UniqueOrderedSensitiveOptionList<FDBTransactionOptions> sensitiveTransactionDefaults;
};

template <>
struct Traceable<Tenant> : std::true_type {
	static std::string toString(const Tenant& tenant) { return printable(tenant.description()); }
};

FDB_BOOLEAN_PARAM(AllowInvalidTenantID);
FDB_BOOLEAN_PARAM(ResolveDefaultTenant);

struct TransactionState;
template <>
void addref(TransactionState* ptr);
template <>
void delref(TransactionState* ptr);

class Transaction : NonCopyable {
public:
	explicit Transaction(Database const& cx);
	explicit Transaction(Reference<Tenant> const& tenant);
	explicit Transaction(Transaction&& tr);
	~Transaction();

	static Transaction fromTenantOrDefault(Optional<Reference<Tenant>> const& tenant, Database const& cx);
	void setVersion(Version v);
	Future<Version> getReadVersion();
	Future<Version> getRawReadVersion();
	Optional<Version> getCachedReadVersion() const;

	[[nodiscard]] Future<ValueReadResult> get(const Key& key, Snapshot = Snapshot::False);
	[[nodiscard]] Future<Void> watch(Reference<Watch> watch);
	[[nodiscard]] Future<KeyReadResult> getKey(const KeySelector& key, Snapshot = Snapshot::False);
	// Future< Optional<KeyValue> > get( const KeySelectorRef& key );
	[[nodiscard]] Future<RangeReadResult> getRange(const KeySelector& begin,
	                                               const KeySelector& end,
	                                               int limit,
	                                               Snapshot = Snapshot::False,
	                                               Reverse = Reverse::False);
	[[nodiscard]] Future<RangeReadResult> getRange(const KeySelector& begin,
	                                               const KeySelector& end,
	                                               GetRangeLimits limits,
	                                               Snapshot = Snapshot::False,
	                                               Reverse = Reverse::False);
	[[nodiscard]] Future<RangeReadResult> getRange(const KeyRange& keys,
	                                               int limit,
	                                               Snapshot snapshot = Snapshot::False,
	                                               Reverse reverse = Reverse::False) {
		return getRange(KeySelector(firstGreaterOrEqual(keys.begin), keys.arena()),
		                KeySelector(firstGreaterOrEqual(keys.end), keys.arena()),
		                limit,
		                snapshot,
		                reverse);
	}
	[[nodiscard]] Future<RangeReadResult> getRange(const KeyRange& keys,
	                                               GetRangeLimits limits,
	                                               Snapshot snapshot = Snapshot::False,
	                                               Reverse reverse = Reverse::False) {
		return getRange(KeySelector(firstGreaterOrEqual(keys.begin), keys.arena()),
		                KeySelector(firstGreaterOrEqual(keys.end), keys.arena()),
		                limits,
		                snapshot,
		                reverse);
	}

	[[nodiscard]] Future<MappedRangeReadResult> getMappedRange(const KeySelector& begin,
	                                                           const KeySelector& end,
	                                                           const Key& mapper,
	                                                           GetRangeLimits limits,
	                                                           int matchIndex = MATCH_INDEX_ALL,
	                                                           Snapshot = Snapshot::False,
	                                                           Reverse = Reverse::False);

private:
	template <class GetKeyValuesFamilyRequest, class GetKeyValuesFamilyReply, class RangeReadResultFamily>
	Future<RangeReadResultFamily> getRangeInternal(const KeySelector& begin,
	                                               const KeySelector& end,
	                                               const Key& mapper,
	                                               GetRangeLimits limits,
	                                               int matchIndex,
	                                               Snapshot snapshot,
	                                               Reverse reverse);

public:
	// A method for streaming data from the storage server that is more efficient than getRange when reading large
	// amounts of data
	[[nodiscard]] Future<Void> getRangeStream(PromiseStream<RangeReadResult>& results,
	                                          const KeySelector& begin,
	                                          const KeySelector& end,
	                                          int limit,
	                                          Snapshot = Snapshot::False,
	                                          Reverse = Reverse::False);
	[[nodiscard]] Future<Void> getRangeStream(PromiseStream<RangeReadResult>& results,
	                                          const KeySelector& begin,
	                                          const KeySelector& end,
	                                          GetRangeLimits limits,
	                                          Snapshot = Snapshot::False,
	                                          Reverse = Reverse::False);
	[[nodiscard]] Future<Void> getRangeStream(PromiseStream<RangeReadResult>& results,
	                                          const KeyRange& keys,
	                                          int limit,
	                                          Snapshot snapshot = Snapshot::False,
	                                          Reverse reverse = Reverse::False) {
		return getRangeStream(results,
		                      KeySelector(firstGreaterOrEqual(keys.begin), keys.arena()),
		                      KeySelector(firstGreaterOrEqual(keys.end), keys.arena()),
		                      limit,
		                      snapshot,
		                      reverse);
	}
	[[nodiscard]] Future<Void> getRangeStream(PromiseStream<RangeReadResult>& results,
	                                          const KeyRange& keys,
	                                          GetRangeLimits limits,
	                                          Snapshot snapshot = Snapshot::False,
	                                          Reverse reverse = Reverse::False) {
		return getRangeStream(results,
		                      KeySelector(firstGreaterOrEqual(keys.begin), keys.arena()),
		                      KeySelector(firstGreaterOrEqual(keys.end), keys.arena()),
		                      limits,
		                      snapshot,
		                      reverse);
	}

	[[nodiscard]] Future<Standalone<VectorRef<const char*>>> getAddressesForKey(const Key& key);

	void enableCheckWrites();
	void addReadConflictRange(KeyRangeRef const& keys);
	void addWriteConflictRange(KeyRangeRef const& keys);
	void makeSelfConflicting();

	Future<Void> warmRange(KeyRange keys);

	// Try to split the given range into equally sized chunks based on estimated size.
	// The returned list would still be in form of [keys.begin, splitPoint1, splitPoint2, ... , keys.end]
	Future<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(KeyRange const& keys, int64_t chunkSize);

	Future<Standalone<VectorRef<KeyRangeRef>>> getBlobGranuleRanges(const KeyRange& range, int rangeLimit);
	Future<Standalone<VectorRef<BlobGranuleChunkRef>>> readBlobGranules(
	    const KeyRange& range,
	    Version begin,
	    Optional<Version> readVersion,
	    Version* readVersionOut = nullptr,
	    int64_t mutationByteLimit = std::numeric_limits<int64_t>::max());

	Future<Standalone<VectorRef<BlobGranuleSummaryRef>>> summarizeBlobGranules(const KeyRange& range,
	                                                                           Optional<Version> summaryVersion,
	                                                                           int rangeLimit);

	void addGranuleMaterializeStats(const GranuleMaterializeStats& stats);

	// If checkWriteConflictRanges is true, existing write conflict ranges will be searched for this key
	void set(const KeyRef& key, const ValueRef& value, AddConflictRange = AddConflictRange::True);
	void atomicOp(const KeyRef& key,
	              const ValueRef& value,
	              MutationRef::Type operationType,
	              AddConflictRange = AddConflictRange::True);
	void clear(const KeyRangeRef& range, AddConflictRange = AddConflictRange::True);
	void clear(const KeyRef& key, AddConflictRange = AddConflictRange::True);

	// Throws not_committed or commit_unknown_result errors in normal operation
	[[nodiscard]] Future<Void> commit();

	void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>());

	// May be called only after commit() returns success
	Version getCommittedVersion() const;

	int64_t getTotalCost() const;

	void setExpectedCost(uint64_t bytes);

	double getTagThrottledDuration() const;

	// Will be fulfilled only after commit() returns success
	[[nodiscard]] Future<Standalone<StringRef>> getVersionstamp();

	Future<uint64_t> getProtocolVersion();

	uint32_t getSize();
	[[nodiscard]] Future<Void> onError(Error const& e);

	// These are to permit use as state variables in actors:
	Transaction();
	void operator=(Transaction&& r) noexcept;

	void reset();
	void fullReset();
	double getBackoff(int errCode);

	void debugTransaction(UID dID);
	VersionVector getVersionVector() const;
	SpanContext getSpanContext() const;

	Future<Void> commitMutations();
	void setupWatches();

	int apiVersionAtLeast(int minVersion) const;
	void checkDeferredError() const;

	Database getDatabase() const;

	Transaction& getTransaction() { return *this; }
	void setTransactionID(UID id);
	void setToken(uint64_t token);

	const std::vector<Future<std::pair<Key, Key>>>& getExtraReadConflictRanges() const;
	Standalone<VectorRef<KeyRangeRef>> readConflictRanges() const;
	Standalone<VectorRef<KeyRangeRef>> writeConflictRanges() const;

	Optional<Reference<Tenant>> getTenant();

	Reference<TransactionState> trState;

	Optional<TransactionTag> const& getTag() const&;
	Span span;

	template <class Type>
	using FutureT = Future<Type>;

private:
	template <class GetKeyValuesFamilyRequest, class GetKeyValuesFamilyReply>
	Future<RangeReadResult> getRangeInternal(const KeySelector& begin,
	                                         const KeySelector& end,
	                                         const Key& mapper,
	                                         GetRangeLimits limits,
	                                         Snapshot snapshot,
	                                         Reverse reverse);
};

ACTOR Future<Version> waitForCommittedVersion(Database cx, Version version, SpanContext spanContext);
ACTOR Future<Standalone<VectorRef<DDMetricsRef>>> waitDataDistributionMetricsList(Database cx,
                                                                                  KeyRange keys,
                                                                                  int shardLimit);

Optional<std::string> tryUnprintable(std::string const&);
std::string unprintable(std::string const&);

int64_t extractIntOption(Optional<StringRef> value,
                         int64_t minValue = std::numeric_limits<int64_t>::min(),
                         int64_t maxValue = std::numeric_limits<int64_t>::max());

// Takes a snapshot of the cluster, specifically the following persistent
// states: coordinator, TLog and storage state
ACTOR Future<Void> snapCreate(Database cx, Standalone<StringRef> snapCmd, UID snapUID);

// Adds necessary mutation(s) to the transaction, so that *one* checkpoint will be created for
// each and every shards overlapping with `ranges`.
// All checkpoint(s) will be created at the transaction's commit version.
Future<Void> createCheckpoint(Transaction* tr,
                              const std::vector<KeyRange>& ranges,
                              CheckpointFormat format,
                              Optional<UID> dataMoveId = Optional<UID>());

// Same as above.
Future<Void> createCheckpoint(Reference<ReadYourWritesTransaction> tr,
                              const std::vector<KeyRange>& ranges,
                              CheckpointFormat format,
                              Optional<UID> dataMoveId = Optional<UID>());

// Gets checkpoint metadata for `ranges` at the specific version, with the particular format.
// Returns a list of [range, checkpoint], where the `checkpoint` has data over `range`.
// checkpoint_not_found() error will be returned if the specific checkpoint cannot be found.
ACTOR Future<std::vector<std::pair<KeyRange, CheckpointMetaData>>> getCheckpointMetaData(
    Database cx,
    std::vector<KeyRange> ranges,
    Version version,
    CheckpointFormat format,
    Optional<UID> dataMoveId = Optional<UID>(),
    double timeout = 5.0);

// Checks with Data Distributor that it is safe to mark all servers in exclusions as failed
ACTOR Future<bool> checkSafeExclusions(Database cx, std::vector<AddressExclusion> exclusions);

// Measured in bytes, rounded up to the nearest page size. Multiply by fungibility ratio
// because writes are more expensive than reads.
inline uint64_t getWriteOperationCost(uint64_t bytes) {
	if (bytes == 0) {
		return CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	} else {
		return CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE *
		       ((bytes - 1) / CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE + 1);
	}
}

// Measured in bytes, rounded up to the nearest page size.
inline uint64_t getReadOperationCost(uint64_t bytes) {
	if (bytes == 0) {
		return CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	} else {
		return ((bytes - 1) / CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE + 1) * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	}
}

// Create a transaction to set the value of system key \xff/conf/perpetual_storage_wiggle. If enable == true, the value
// will be 1. Otherwise, the value will be 0. The caller should take care of the reset of StorageWiggleMetrics if
// necessary. Returns the FDB version at which the transaction was committed.
ACTOR Future<Version> setPerpetualStorageWiggle(Database cx, bool enable, LockAware lockAware = LockAware::False);

ACTOR Future<std::vector<std::pair<UID, StorageWiggleValue>>> readStorageWiggleValues(Database cx,
                                                                                      bool primary,
                                                                                      bool use_system_priority);

// Returns the maximum legal size of a key. This size will be determined by the prefix of the passed in key
// (system keys have a larger maximum size). This should be used for generic max key size requests.
int64_t getMaxKeySize(KeyRef const& key);

// Returns the maximum legal size of a key that can be read. Keys larger than this will be assumed not to exist.
int64_t getMaxReadKeySize(KeyRef const& key);

// Returns the maximum legal size of a key that can be written. If using raw access, writes to normal keys will
// be allowed to be slightly larger to accommodate the prefix.
int64_t getMaxWriteKeySize(KeyRef const& key, bool hasRawAccess);

// Returns the maximum legal size of a key that can be cleared. Keys larger than this will be assumed not to exist.
int64_t getMaxClearKeySize(KeyRef const& key);

struct KeyRangeLocationInfo;
// Return the aggregated StorageMetrics of range keys to the caller. The locations tell which interface should
// serve the request. The final result is within (min-permittedError/2, max + permittedError/2) if valid.
ACTOR Future<Optional<StorageMetrics>> waitStorageMetricsWithLocation(TenantInfo tenantInfo,
                                                                      Version version,
                                                                      KeyRange keys,
                                                                      std::vector<KeyRangeLocationInfo>* locations,
                                                                      StorageMetrics min,
                                                                      StorageMetrics max,
                                                                      StorageMetrics permittedError,
                                                                      bool* invalidateCache);

// Return the suggested split points from storage server.The locations tell which interface should
// serve the request. `limit` is the current estimated storage metrics of `keys`.The returned points, if present,
// guarantee the metrics of split result is within limit.
ACTOR Future<Optional<Standalone<VectorRef<KeyRef>>>> splitStorageMetricsWithLocations(
    std::vector<KeyRangeLocationInfo> locations,
    KeyRange keys,
    StorageMetrics limit,
    StorageMetrics estimated,
    Optional<int> minSplitBytes);

ACTOR Future<RangeReadResult> getWorkerInterfaces(Reference<IClusterConnectionRecord> connRecord);

ACTOR Future<Standalone<VectorRef<ReadHotRangeWithMetrics>>> getReadHotRangesWithLocations(
    KeyRange keys,
    std::vector<KeyRangeLocationInfo>* locations,
    bool* invalidateLocationCache);

namespace NativeAPI {

ACTOR Future<std::vector<std::pair<StorageServerInterface, ProcessClass>>> getServerListAndProcessClasses(
    Transaction* tr);

const TLSConfig& getTLSConfig();
} // namespace NativeAPI
#include "flow/unactorcompiler.h"
#endif
