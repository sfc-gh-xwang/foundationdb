/*
 * TenantBalancer.actor.cpp
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/TenantBalancer.actor.h"
#include "fdbserver/WaitFailure.h"
#include "flow/ApiVersion.h"
#include "flow/TaskPriority.h"
#include "metacluster/Metacluster.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct TenantBalancer {
	explicit TenantBalancer(TenantBalancerInterface tbi,
	                        Reference<AsyncVar<ServerDBInfo> const> dbInfo,
	                        Reference<IClusterConnectionRecord> connRecord)
	  : tbi(tbi), dbInfo(dbInfo), connRecord(connRecord), tenantBalancerMetrics("TenantBalancer", tbi.id().toString()),
	    getMovementStatusRequests("GetMovementStatusRequests", tenantBalancerMetrics),
	    getActiveMovementsRequests("GetActiveMovementsRequests", tenantBalancerMetrics),
	    moveTenantsToClusterRequests("MoveTenantsToClusterRequests", tenantBalancerMetrics),
	    abortMovementRequests("AbortMovementRequests", tenantBalancerMetrics) {
		managementDb =
		    openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::False, EnableLocalityLoadBalance::True);
	}

	TenantBalancerInterface tbi;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Reference<IClusterConnectionRecord> connRecord;
	Database managementDb;

	ActorCollection actors;

	CounterCollection tenantBalancerMetrics;
	Counter getMovementStatusRequests;
	Counter getActiveMovementsRequests;
	Counter moveTenantsToClusterRequests;
	Counter abortMovementRequests;
};

ACTOR Future<Void> getMovementStatus(TenantBalancer* self, GetMovementStatusRequest req) {
	++self->getMovementStatusRequests;
	TraceEvent(SevDebug, "TenantBalancerGetMovementStatus", self->tbi.id()).detail("Tenant", req.tenantName);

	try {
		GetMovementStatusReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerGetMovementStatusError", self->tbi.id())
		    .error(e)
		    .detail("Tenant", req.tenantName);
		req.reply.sendError(e);
	}
	return Void();
}

ACTOR Future<Void> getActiveMovements(TenantBalancer* self, GetActiveMovementsRequest req) {
	++self->getActiveMovementsRequests;
	TraceEvent(SevDebug, "TenantBalancerGetActiveMovements", self->tbi.id()).log();

	try {
		GetActiveMovementsReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerGetActiveMovementsError", self->tbi.id()).error(e);
		req.reply.sendError(e);
	}
	return Void();
}

ACTOR Future<Void> moveTenantsToCluster(TenantBalancer* self, MoveTenantsToClusterRequest req) {
	++self->moveTenantsToClusterRequests;
	TraceEvent("TenantBalancerMoveTenantsToCluster", self->tbi.id()).log();
	wait(delay(0.1));
	MoveTenantsToClusterReply reply;
	req.reply.send(reply);
	return Void();
}

ACTOR Future<Void> abortMovement(TenantBalancer* self, AbortMovementRequest req) {
	++self->abortMovementRequests;
	TraceEvent(SevDebug, "TenantBalancerAbortMovement", self->tbi.id()).detail("Tenant", req.tenantName);

	try {
		AbortMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerAbortMovementError", self->tbi.id())
		    .error(e)
		    .detail("Tenant", req.tenantName);
		req.reply.sendError(e);
	}
	return Void();
}

ACTOR Future<Void> tenantBalancerCore(TenantBalancer* self) {
	TraceEvent("TenantBalancerStarting", self->tbi.id());
	self->actors.add(waitFailureServer(self->tbi.waitFailure.getFuture()));
	loop choose {
		when(HaltTenantBalancerRequest req = waitNext(self->tbi.haltTenantBalancer.getFuture())) {
			req.reply.send(Void());
			TraceEvent("TenantBalancerHalted", self->tbi.id()).detail("ReqID", req.requesterID);
			break;
		}
		when(GetMovementStatusRequest req = waitNext(self->tbi.getMovementStatus.getFuture())) {
			self->actors.add(getMovementStatus(self, req));
		}
		when(GetActiveMovementsRequest req = waitNext(self->tbi.getActiveMovements.getFuture())) {
			self->actors.add(getActiveMovements(self, req));
		}
		when(MoveTenantsToClusterRequest req = waitNext(self->tbi.moveTenantsToCluster.getFuture())) {
			self->actors.add(moveTenantsToCluster(self, req));
		}
		when(AbortMovementRequest req = waitNext(self->tbi.abortMovement.getFuture())) {
			self->actors.add(abortMovement(self, req));
		}
		when(wait(self->actors.getResult())) {}
	}
	return Void();
}

ACTOR Future<Void> tenantBalancer(TenantBalancerInterface tbi,
                                  Reference<AsyncVar<ServerDBInfo> const> db,
                                  Reference<IClusterConnectionRecord> connRecord) {
	state TenantBalancer self(tbi, db, connRecord);
	try {
		PersistInterfaceRequest _ = waitNext(tbi.persistInterface.getFuture());
		TraceEvent("TenantBalancerPersistInterfReq").log();
		wait(writeTenantBalancerInterfaceToDb(self.managementDb, tbi));

		wait(tenantBalancerCore(&self));
	} catch (Error& e) {
		TraceEvent("TenantBalancerTerminated", tbi.id()).error(e);
		throw e;
	}
	return Void();
}
