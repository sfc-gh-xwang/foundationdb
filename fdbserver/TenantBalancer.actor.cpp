/*
 * TenantBalancer.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct TenantBalancer {
	explicit TenantBalancer(TenantBalancerInterface tbi,
	                        Reference<AsyncVar<ServerDBInfo> const> dbInfo,
	                        Reference<IClusterConnectionRecord> connRecord)
	  : tbi(tbi), dbInfo(dbInfo), connRecord(connRecord), tenantBalancerMetrics("TenantBalancer", tbi.id().toString()),
	    getMovementStatusRequests("GetMovementStatusRequests", tenantBalancerMetrics),
	    getActiveMovementsRequests("GetActiveMovementsRequests", tenantBalancerMetrics),
	    moveTenantsToClusterRequests("MoveTenantsToClusterRequests", tenantBalancerMetrics),
	    abortMovementRequests("AbortMovementRequests", tenantBalancerMetrics) {}

	TenantBalancerInterface tbi;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Reference<IClusterConnectionRecord> connRecord;

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
	TraceEvent(SevDebug, "TenantBalancerMoveTenantsToCluster", self->tbi.id()).log();

	try {
		MoveTenantsToClusterReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerMoveTenantsToClusterError", self->tbi.id()).error(e);
		req.reply.sendError(e);
	}
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
		wait(tenantBalancerCore(&self));
	} catch (Error& e) {
		TraceEvent("TenantBalancerTerminated", tbi.id()).error(e);
		throw e;
	}
	return Void();
}