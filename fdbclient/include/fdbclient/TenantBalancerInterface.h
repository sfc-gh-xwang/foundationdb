/*
 * TenantBalancerInterface.h
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDBCLIENT_TENANTBALANCERINTERFACE_H
#define FDBCLIENT_TENANTBALANCERINTERFACE_H

#include <stdbool.h>
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"

enum class MovementState { INITIALIZING, STARTED, READY_FOR_SWITCH, SWITCHING, COMPLETED, ERROR };
enum class AbortState { UNKNOWN, ROLLED_BACK, COMPLETED };

struct TenantMovementInfo {
	constexpr static FileIdentifier file_identifier = 16510400;
	Arena arena;

	UID movementId;
	Key tenantId;

	explicit TenantMovementInfo() = default;
	explicit TenantMovementInfo(UID moveId, Key tid) : movementId(moveId), tenantId(tid) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, movementId, tenantId, arena);
	}
};

struct TenantMovementStatus {
	constexpr static FileIdentifier file_identifier = 5103586;
	Arena arena;

	TenantMovementInfo tenantMovementInfo;
	Optional<double> databaseVersionLag;
	Optional<double> mutationLag;
	Optional<Version> switchVersion;

	explicit TenantMovementStatus() = default;
	std::string toJson() const;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantMovementInfo, databaseVersionLag, mutationLag, switchVersion, arena);
	}
};

struct GetMovementStatusReply {
	constexpr static FileIdentifier file_identifier = 4693499;

	TenantMovementStatus movementStatus;

	GetMovementStatusReply() = default;
	GetMovementStatusReply(TenantMovementStatus moveStatus) : movementStatus(moveStatus) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, movementStatus);
	}
};

struct GetMovementStatusRequest {
	constexpr static FileIdentifier file_identifier = 11494877;
	Arena arena;

	Key tenantName;
	ReplyPromise<GetMovementStatusReply> reply;

	GetMovementStatusRequest() = default;
	GetMovementStatusRequest(Key name) : tenantName(name) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantName, reply, arena);
	}
};

struct MoveTenantsToClusterReply {
	constexpr static FileIdentifier file_identifier = 3708530;

	UID movementId;

	MoveTenantsToClusterReply() = default;
	explicit MoveTenantsToClusterReply(UID id) : movementId(id) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, movementId);
	}
};

struct MoveTenantsToClusterRequest {
	constexpr static FileIdentifier file_identifier = 3571712;
	Arena arena;

	KeyRef tenantName;
	KeyRef dstCluster;

	ReplyPromise<MoveTenantsToClusterReply> reply;

	MoveTenantsToClusterRequest() = default;
	explicit MoveTenantsToClusterRequest(KeyRef tenantName, KeyRef name)
	  : tenantName(arena, tenantName), dstCluster(arena, name) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantName, dstCluster, reply, arena);
	}
};

struct GetActiveMovementsReply {
	constexpr static FileIdentifier file_identifier = 2320458;
	Arena arena;

	std::vector<TenantMovementInfo> activeMovements;

	GetActiveMovementsReply() = default;
	explicit GetActiveMovementsReply(std::vector<TenantMovementInfo> movements)
	  : activeMovements(std::move(movements)) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, activeMovements, arena);
	}
};

struct GetActiveMovementsRequest {
	constexpr static FileIdentifier file_identifier = 11980148;

	ReplyPromise<GetActiveMovementsReply> reply;

	GetActiveMovementsRequest() = default;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct AbortMovementReply {
	constexpr static FileIdentifier file_identifier = 14761140;

	AbortState abortResult;

	AbortMovementReply() {}
	AbortMovementReply(AbortState abortResult) : abortResult(abortResult) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, abortResult);
	}
};

struct AbortMovementRequest {
	constexpr static FileIdentifier file_identifier = 14058403;
	Arena arena;

	Key tenantName;

	ReplyPromise<AbortMovementReply> reply;

	AbortMovementRequest() = default;
	AbortMovementRequest(Key tenantName) : tenantName(tenantName) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantName, reply, arena);
	}
};

struct PersistInterfaceRequest {
	constexpr static FileIdentifier file_identifier = 3278033;
	ReplyPromise<Void> reply;

	PersistInterfaceRequest() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct HaltTenantBalancerRequest {
	constexpr static FileIdentifier file_identifier = 15769279;

	UID requesterID;
	ReplyPromise<Void> reply;

	explicit HaltTenantBalancerRequest() = default;
	explicit HaltTenantBalancerRequest(UID uid) : requesterID(uid) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, reply);
	}
};

struct TenantBalancerInterface {
	constexpr static FileIdentifier file_identifier = 6185894;
	Arena arena;

	LocalityData locality;
	UID uniqueId;

	RequestStream<HaltTenantBalancerRequest> haltTenantBalancer;
	RequestStream<GetMovementStatusRequest> getMovementStatus;
	RequestStream<GetActiveMovementsRequest> getActiveMovements;
	RequestStream<MoveTenantsToClusterRequest> moveTenantsToCluster;
	RequestStream<AbortMovementRequest> abortMovement;
	RequestStream<PersistInterfaceRequest> persistInterface;
	RequestStream<ReplyPromise<Void>> waitFailure;

	NetworkAddress address() const { return haltTenantBalancer.getEndpoint().getPrimaryAddress(); }
	NetworkAddress stableAddress() const { return haltTenantBalancer.getEndpoint().getStableAddress(); }
	Optional<NetworkAddress> secondaryAddress() const {
		return haltTenantBalancer.getEndpoint().addresses.secondaryAddress;
	}

	UID id() const { return uniqueId; }
	std::string toString() const { return id().shortString(); }

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           locality,
		           uniqueId,
		           haltTenantBalancer,
		           getMovementStatus,
		           getActiveMovements,
		           moveTenantsToCluster,
		           abortMovement,
		           persistInterface,
		           waitFailure,
		           arena);
	}

	TenantBalancerInterface() : uniqueId(deterministicRandom()->randomUniqueID()) {}
	explicit TenantBalancerInterface(const LocalityData& l, UID uid) : locality(l), uniqueId(uid) {}

	bool operator==(TenantBalancerInterface const& s) const { return uniqueId == s.uniqueId; }
	bool operator<(TenantBalancerInterface const& s) const { return uniqueId < s.uniqueId; }

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(haltTenantBalancer.getReceiver());
		streams.push_back(getMovementStatus.getReceiver());
		streams.push_back(getActiveMovements.getReceiver());
		streams.push_back(moveTenantsToCluster.getReceiver());
		streams.push_back(abortMovement.getReceiver());
		streams.push_back(persistInterface.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
};

#endif // FDBCLIENT_TENANTBALANCERINTERFACE_H
