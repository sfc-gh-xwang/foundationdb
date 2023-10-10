/*
 * TransactionState.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDBCLIENT_TRANSACTIONSTATE_H
#define FDBCLIENT_TRANSACTIONSTATE_H
#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ClientLogEvents.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/KeyRangeMap.h"
#include "flow/WipedString.h"

struct TransactionOptions {
	double maxBackoff;
	uint32_t getReadVersionFlags;
	uint32_t sizeLimit;
	int maxTransactionLoggingFieldLength;
	bool checkWritesEnabled : 1;
	bool causalWriteRisky : 1;
	bool commitOnFirstProxy : 1;
	bool debugDump : 1;
	bool lockAware : 1;
	bool readOnly : 1;
	bool firstInBatch : 1;
	bool includePort : 1;
	bool reportConflictingKeys : 1;
	bool expensiveClearCostEstimation : 1;
	bool useGrvCache : 1;
	bool skipGrvCache : 1;
	bool rawAccess : 1;
	bool bypassStorageQuota : 1;
	bool preferLocalStorageServer : 1;
	bool disable_range_fragments : 1;

	TransactionPriority priority;

	Optional<TransactionTag> throttlingTag;

	// update clear function if you add a new field

	TransactionOptions(Database const& cx);
	TransactionOptions();

	void reset(Database const& cx);

private:
	void clear();
};

struct TransactionLogInfo : public ReferenceCounted<TransactionLogInfo>, NonCopyable {
	enum LoggingLocation { DONT_LOG = 0, TRACE_LOG = 1, DATABASE = 2 };

	TransactionLogInfo() : logLocation(DONT_LOG), maxFieldLength(0) {}
	TransactionLogInfo(LoggingLocation location) : logLocation(location), maxFieldLength(0) {}
	TransactionLogInfo(std::string id, LoggingLocation location)
	  : logLocation(location),
	    maxFieldLength(0),
	    identifier(id) {}

	void setIdentifier(std::string id) { identifier = id; }
	void logTo(LoggingLocation loc) { logLocation = logLocation | loc; }

	template <typename T>
	void addLog(const T& event) {
		if (logLocation & TRACE_LOG) {
			ASSERT(!identifier.empty());
			event.logEvent(identifier, maxFieldLength);
		}

		if (flushed) {
			return;
		}

		if (logLocation & DATABASE) {
			logsAdded = true;
			static_assert(std::is_base_of<FdbClientLogEvents::Event, T>::value,
			              "Event should be derived class of FdbClientLogEvents::Event");
			trLogWriter << event;
		}
	}

	BinaryWriter trLogWriter{ IncludeVersion() };
	bool logsAdded{ false };
	bool flushed{ false };
	int logLocation;
	int maxFieldLength;
	std::string identifier;
};

struct TransactionState : ReferenceCounted<TransactionState> {
	Database cx;
	Future<Version> readVersionFuture;
	Promise<Optional<Value>> metadataVersion;
	Optional<WipedString> authToken;
	Reference<TransactionLogInfo> trLogInfo;
	TransactionOptions options;
	Optional<ReadOptions> readOptions;
	Optional<HybridReadOptions> hybridReadOptions;

	TaskPriority taskID;
	SpanContext spanContext;
	UseProvisionalProxies useProvisionalProxies = UseProvisionalProxies::False;
	bool readVersionObtainedFromGrvProxy;
	double proxyTagThrottledDuration = 0.0;

	// Special flag to skip prepending tenant prefix to mutations and conflict ranges
	// when a dummy, internal transaction gets committed. The sole purpose of commitDummyTransaction() is to
	// resolve the state of earlier transaction that returned commit_unknown_result or request_maybe_delivered.
	// Therefore, the dummy transaction can simply reuse one conflict range of the earlier commit, if it already has
	// been prefixed.
	bool skipApplyTenantPrefix = false;

	int numErrors = 0;
	double startTime = 0;
	Promise<Standalone<StringRef>> versionstampPromise;

	Version committedVersion{ invalidVersion };

	// Used to save conflicting keys if FDBTransactionOptions::REPORT_CONFLICTING_KEYS is enabled
	// prefix/<key1> : '1' - any keys equal or larger than this key are (probably) conflicting keys
	// prefix/<key2> : '0' - any keys equal or larger than this key are (definitely) not conflicting keys
	std::shared_ptr<CoalescedKeyRangeMap<Value>> conflictingKeys;

	bool automaticIdempotency = false;

	Future<Void> startFuture;

	std::vector<Reference<Watch>> watches;
	double backoff;
	CommitTransactionRequest tr;
	std::vector<Future<std::pair<Key, Key>>> extraConflictRanges;
	Promise<Void> commitResult;
	Future<Void> committing;

	// Only available so that Transaction can have a default constructor, for use in state variables
	TransactionState(TaskPriority taskID, SpanContext spanContext)
	  : taskID(taskID),
	    spanContext(spanContext),
	    tenantSet(false) {}

	TransactionState(Database cx,
	                 Optional<Reference<Tenant>> tenant,
	                 TaskPriority taskID,
	                 SpanContext spanContext,
	                 Reference<TransactionLogInfo> trLogInfo);

	~TransactionState();

	Reference<TransactionState> cloneAndReset(bool fullReset);
	void cancel();
	void cancelWatches(Error const& e = transaction_cancelled());
	void flushTrLogsIfEnabled();

	Version readVersion() {
		ASSERT(readVersionFuture.isValid() && readVersionFuture.isReady());
		return readVersionFuture.get();
	}

	TenantInfo getTenantInfo(AllowInvalidTenantID allowInvalidTenantId = AllowInvalidTenantID::False);

	Optional<Reference<Tenant>> const& tenant();
	bool hasTenant(ResolveDefaultTenant ResolveDefaultTenant = ResolveDefaultTenant::True);
	int64_t tenantId() const { return tenant_.present() ? tenant_.get()->id() : TenantInfo::INVALID_TENANT; }

	Future<Void> startTransaction(uint32_t readVersionFlags = 0);
	Future<Version> getReadVersion(uint32_t flags);

	bool canUseGrvCache();

	void addReadCost(uint64_t bytes);
	void addWriteCost(uint64_t bytes);
	void addClearCost();
	void flushWriteCost();
	int64_t getTotalCost() const;

	Optional<ThrottlingId> getThrottlingId();

	void setExpectedCost(uint64_t bytes);
	Optional<uint64_t> getExpectedTransactionCost() const;

private:
	Optional<Reference<Tenant>> tenant_;
	bool tenantSet;

	// Measured by summing the bytes accessed by each read operation
	// after rounding up to the nearest page size
	int64_t readCost = 0;

	// Measured by summing the bytes accessed by each write operation
	// after rounding up to the nearest page size and applying a write penalty
	int64_t writeCost = 0;
	bool flushedWriteCost = false;

	// Expected transaction cost provided by the client
	Optional<uint64_t> expectedTransactionCost;
};

#endif