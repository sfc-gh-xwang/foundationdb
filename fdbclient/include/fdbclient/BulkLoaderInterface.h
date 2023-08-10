/*
 * BulkLoaderInterface.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDBCLIENT_BULKLOADERINTERFACE_H
#define FDBCLIENT_BULKLOADERINTERFACE_H
#pragma once

#include "fdbclient/BulkLoad.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/TenantInfo.h"
#include "fdbrpc/TimedRequest.h"

struct BulkLoaderInterface {
	constexpr static FileIdentifier file_identifier = 7553637;
	RequestStream<ReplyPromise<Void>> waitFailure;
	PublicRequestStream<struct BulkLoadFilesToWriteRequest> filesToWriteRequest;
	PublicRequestStream<struct BulkLoadFilesWrittenRequest> filesWrittenRequest;
	PublicRequestStream<struct BulkLoadStartRequest> startRequest;
	PublicRequestStream<struct BulkLoadStateChangeRequest> stateChangeRequest;
	PublicRequestStream<struct BulkLoadWaitCompleteRequest> waitCompleteRequest;

	struct LocalityData locality;
	UID myId;

	BulkLoaderInterface() {}
	explicit BulkLoaderInterface(const struct LocalityData& l, UID id) : locality(l), myId(id) {}

	void initEndpoints() {
		// TODO: specify endpoint priorities?
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(filesToWriteRequest.getReceiver());
		streams.push_back(filesWrittenRequest.getReceiver());
		streams.push_back(startRequest.getReceiver());
		streams.push_back(stateChangeRequest.getReceiver());
		streams.push_back(waitCompleteRequest.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
	UID id() const { return myId; }
	NetworkAddress address() const { return filesToWriteRequest.getEndpoint().getPrimaryAddress(); }
	NetworkAddress stableAddress() const { return filesToWriteRequest.getEndpoint().getStableAddress(); }
	bool operator==(const BulkLoaderInterface& r) const { return id() == r.id(); }
	bool operator!=(const BulkLoaderInterface& r) const { return !(*this == r); }
	std::string toString() const { return id().shortString(); }

	template <class Archive>
	void serialize(Archive& ar) {
		// use adjusted endpoints
		serializer(ar, myId, locality, waitFailure);
		if (Archive::isDeserializing) {
			filesToWriteRequest = PublicRequestStream<struct BulkLoadFilesToWriteRequest>(
			    waitFailure.getEndpoint().getAdjustedEndpoint(1));
			filesWrittenRequest = PublicRequestStream<struct BulkLoadFilesWrittenRequest>(
			    waitFailure.getEndpoint().getAdjustedEndpoint(2));
			startRequest =
			    PublicRequestStream<struct BulkLoadStartRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(3));
			stateChangeRequest = PublicRequestStream<struct BulkLoadStateChangeRequest>(
			    waitFailure.getEndpoint().getAdjustedEndpoint(4));
			waitCompleteRequest = PublicRequestStream<struct BulkLoadWaitCompleteRequest>(
			    waitFailure.getEndpoint().getAdjustedEndpoint(5));
		}
	}
};

struct BulkLoadFilesToWriteReply {
	constexpr static FileIdentifier file_identifier = 9176412;
	Arena arena;
	VectorRef<BulkLoadFileToWriteRef> filePointers;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, filePointers, arena);
	}
};

struct BulkLoadFilesToWriteRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 8267725;

	TenantInfo tenantInfo;
	Arena arena;
	BulkLoadIDRef id;
	int count;

	ReplyPromise<BulkLoadFilesToWriteReply> reply;

	BulkLoadFilesToWriteRequest() {}

	bool verify() const noexcept { return tenantInfo.isAuthorized(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantInfo, id, count, reply, arena);
	}
};

struct BulkLoadFilesWrittenRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 2159325;

	TenantInfo tenantInfo;
	Arena arena;
	BulkLoadIDRef id;
	VectorRef<BulkLoadWrittenFileRef> filesWritten;

	// just an acknowledgement or error
	ReplyPromise<Void> reply;

	BulkLoadFilesWrittenRequest() {}

	bool verify() const noexcept { return tenantInfo.isAuthorized(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantInfo, id, filesWritten, reply, arena);
	}
};

/*
 * All of these requests to the bulk loader mutate state on disk, and for correctness need to be idempotent.
 * Enforcing this for start and cleanup is particularly difficult, as they transition from (no state) -> state, and
 * state -> (no state), respectively. For example, consider the case of duplicating a start request. This could happen
 * due to retransmits, retries, or duplication from load balance. If 2 requests get sent, one completes and unblocks the
 * client, then that client can proceed with the rest of the bulk load, eventually cleaning it up. If the duplicated
 * request begins after the cleanup, it would see that there was no existing load for that id or that range, and
 * re-animate the previously completed load. The solution to this is to use a version fencing procotol to guarantee no
 * start request can begin a transaction after the cleanup request has been committed. Before starting the bulk load,
 * the client gets a read version RVstart. We then want to enforce that no start load request is allowed to complete
 * with its transaction's read version higher than RVstart + SW (start window). This allows us to ensure there are no
 * races in cleanup by forcing the cleanup transaction to wait until its read version is higher than RVStart + SW. To
 * implement this we sent RVstart as part of the bulk load request, and then in the cleanup phase we delay the read
 * version until it is large enough. In practice, the version we use on cleanup instead of persisting RVstart is the
 * commit version of the original start transaction, CVstart. Because CVStart > RVStart, ensuring the RVcleanup >=
 * CVStart + SW still satisfies the invariant of RVcleanup >= RVstart + SW. Note that in practice this is very rare, a
 * bulk load entirely completing in several seconds likely means that the overall data size was tiny, in which case the
 * feature was not being used efficiently anyway. But in simulation, where we intentionally stress test the feature with
 * many small concurrent loads and adverse conditions, this happens frequently.
 */
struct BulkLoadStartRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 7208020;

	TenantInfo tenantInfo;
	Arena arena;
	BulkLoadIDRef id;
	KeyRangeRef keyRange;
	Version startReadVersion; // bound retries/retransmits

	// just an acknowledgement or error
	ReplyPromise<Void> reply;

	BulkLoadStartRequest() {}

	bool verify() const noexcept { return tenantInfo.isAuthorized(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantInfo, id, keyRange, startReadVersion, reply, arena);
	}
};

struct BulkLoadStateChangeRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 1861282;

	TenantInfo tenantInfo;
	Arena arena;
	BulkLoadIDRef id;
	BulkLoadStatus::State desired;

	// just an acknowledgement or error
	ReplyPromise<Void> reply;

	BulkLoadStateChangeRequest() {}

	bool verify() const noexcept { return tenantInfo.isAuthorized(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantInfo, id, desired, reply, arena);
	}
};

struct BulkLoadWaitCompleteRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 3425035;

	TenantInfo tenantInfo;
	Arena arena;
	BulkLoadIDRef id;

	// just an acknowledgement or error
	ReplyPromise<Void> reply;

	BulkLoadWaitCompleteRequest() {}

	bool verify() const noexcept { return tenantInfo.isAuthorized(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantInfo, id, reply, arena);
	}
};

#endif
