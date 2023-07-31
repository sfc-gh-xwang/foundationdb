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

	// FIXME: add authorization once we can get tenant info from tenant instead of transaction!!
	// bool verify() const noexcept { return tenantInfo.isAuthorized(); }
	bool verify() const noexcept { return true; }

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

	// FIXME: add authorization once we can get tenant info from tenant!!
	// bool verify() const noexcept { return tenantInfo.isAuthorized(); }
	bool verify() const noexcept { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantInfo, id, filesWritten, reply, arena);
	}
};

struct BulkLoadStartRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 7208020;

	TenantInfo tenantInfo;
	Arena arena;
	BulkLoadIDRef id;
	KeyRangeRef keyRange;

	// just an acknowledgement or error
	ReplyPromise<Void> reply;

	BulkLoadStartRequest() {}

	// FIXME: add authorization once we can get tenant info from tenant!!
	// bool verify() const noexcept { return tenantInfo.isAuthorized(); }
	bool verify() const noexcept { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantInfo, id, keyRange, reply, arena);
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

	// FIXME: add authorization once we can get tenant info from tenant!!
	// bool verify() const noexcept { return tenantInfo.isAuthorized(); }
	bool verify() const noexcept { return true; }

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

	// FIXME: add authorization once we can get tenant info from tenant!!
	// bool verify() const noexcept { return tenantInfo.isAuthorized(); }
	bool verify() const noexcept { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantInfo, id, reply, arena);
	}
};

#endif
