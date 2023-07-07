/*
 * TenantMovement.actor.h
 * Copyright (c) 2023 Snowflake Computing
 */
#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_TENANT_MOVEMENT_ACTOR_G_H)
#define FDBCLIENT_TENANT_MOVEMENT_ACTOR_G_H
#include "fdbclient/TenantMovement.actor.g.h"
#elif !defined(FDBCLIENT_TENANT_MOVEMENT_ACTOR_H)
#define FDBCLIENT_TENANT_MOVEMENT_ACTOR_H

#include "fdbclient/IClientApi.h"
#include "fdbclient/TenantBalancer.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "flow/actorcompiler.h" // has to be last include

ACTOR
template <typename IDatabase>
static Future<Future<Void>> getTbiChangedFuture(Reference<IDatabase> db) {
	state Reference<ITransaction> tr = db->createTransaction();
	state Future<Void> future = Never();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state ThreadFuture<Void> threadFuture = TenantBalancerMetadata::getTenantBalancerInterface().watch(tr);
			wait(safeThreadFutureToFuture(tr->commit()));
			future = safeThreadFutureToFuture(threadFuture);
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
	return future;
}

ACTOR
template <typename IDatabase>
static Future<Optional<TenantBalancerInterface>> getTenantBalancerInterface(Reference<IDatabase> db) {
	state Reference<ITransaction> tr = db->createTransaction();
	state TenantBalancerInterface interf;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			TenantBalancerInterface _interf = wait(TenantBalancerMetadata::getTenantBalancerInterface().getD(tr));
			interf = _interf;
			break;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
	return interf;
}

namespace MetaclusterAPI {

ACTOR
template <typename IDatabase>
static Future<Void> moveTenant(IDatabase managementDb, TenantName tenant, ClusterName dstClusterName) {
	state MoveTenantsToClusterRequest req(tenant, dstClusterName);
	state Future<Void> init = Void();
	state Future<ErrorOr<MoveTenantsToClusterReply>> replyFuture = Never();
	try {
		state Future<Void> onTbiChanged = wait(getTbiChangedFuture(managementDb));
		loop choose {
			when(ErrorOr<MoveTenantsToClusterReply> reply =
			         wait(timeoutError(replyFuture, CLIENT_KNOBS->TENANT_BALANCER_REQUEST_TIMEOUT))) {
				if (reply.isError()) {
					throw reply.getError();
				}
				break;
			}
			when(wait(onTbiChanged || init)) {
				init = Never();
				state Optional<TenantBalancerInterface> tbi = wait(getTenantBalancerInterface(managementDb));
				replyFuture = tbi.present() ? tbi.get().moveTenantsToCluster.tryGetReply(req) : Never();
				Future<Void> _onTbiChanged = wait(getTbiChangedFuture(managementDb));
				onTbiChanged = _onTbiChanged;
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
	}
	return Void();
}

ACTOR
template <typename IDatabase>
static Future<TenantMovementStatus> getTenantMovementStatus(IDatabase managementDb, TenantName tenant) {
	state GetMovementStatusRequest req(tenant);
	state Future<Void> init = Void();
	state Future<ErrorOr<GetMovementStatusReply>> replyFuture = Never();
	state TenantMovementStatus status;
	try {
		state Future<Void> onTbiChanged = wait(getTbiChangedFuture(managementDb));
		loop choose {
			when(ErrorOr<GetMovementStatusReply> reply =
			         wait(timeoutError(replyFuture, CLIENT_KNOBS->TENANT_BALANCER_REQUEST_TIMEOUT))) {
				if (reply.isError()) {
					throw reply.getError();
				}
				status = reply.get().movementStatus;
				break;
			}
			when(wait(onTbiChanged || init)) {
				init = Never();
				state Optional<TenantBalancerInterface> tbi = wait(getTenantBalancerInterface(managementDb));
				replyFuture = tbi.present() ? tbi.get().getMovementStatus.tryGetReply(req) : Never();
				Future<Void> _onTbiChanged = wait(getTbiChangedFuture(managementDb));
				onTbiChanged = _onTbiChanged;
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
	}
	return status;
}

ACTOR
template <typename IDatabase>
static Future<std::vector<TenantMovementInfo>> getActiveMovements(IDatabase managementDb) {
	state GetActiveMovementsRequest req;
	state Future<Void> init = Void();
	state Future<ErrorOr<GetActiveMovementsReply>> replyFuture = Never();
	state std::vector<TenantMovementInfo> tenantMovementInfo;
	try {
		state Future<Void> onTbiChanged = wait(getTbiChangedFuture(managementDb));
		loop choose {
			when(ErrorOr<GetActiveMovementsReply> reply =
			         wait(timeoutError(replyFuture, CLIENT_KNOBS->TENANT_BALANCER_REQUEST_TIMEOUT))) {
				if (reply.isError()) {
					throw reply.getError();
				}
				tenantMovementInfo = reply.get().activeMovements;
				break;
			}
			when(wait(onTbiChanged || init)) {
				init = Never();
				state Optional<TenantBalancerInterface> tbi = wait(getTenantBalancerInterface(managementDb));
				replyFuture = tbi.present() ? tbi.get().getActiveMovements.tryGetReply(req) : Never();
				Future<Void> _onTbiChanged = wait(getTbiChangedFuture(managementDb));
				onTbiChanged = _onTbiChanged;
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
	}
	return tenantMovementInfo;
}

ACTOR
template <typename IDatabase>
static Future<Void> abortTenantMovement(IDatabase managementDb, TenantName tenant) {
	state AbortMovementRequest req;
	state Future<Void> init = Void();
	state Future<ErrorOr<AbortMovementReply>> replyFuture = Never();
	try {
		state Future<Void> onTbiChanged = wait(getTbiChangedFuture(managementDb));
		loop choose {
			when(ErrorOr<AbortMovementReply> reply =
			         wait(timeoutError(replyFuture, CLIENT_KNOBS->TENANT_BALANCER_REQUEST_TIMEOUT))) {
				if (reply.isError()) {
					throw reply.getError();
				}
				break;
			}
			when(wait(onTbiChanged || init)) {
				init = Never();
				state Optional<TenantBalancerInterface> tbi = wait(getTenantBalancerInterface(managementDb));
				replyFuture = tbi.present() ? tbi.get().abortMovement.tryGetReply(req) : Never();
				Future<Void> _onTbiChanged = wait(getTbiChangedFuture(managementDb));
				onTbiChanged = _onTbiChanged;
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
	}
	return Void();
}

} // namespace MetaclusterAPI

#include "flow/unactorcompiler.h"
#endif