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
#include "fdbclient/TenantBalancer.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "flow/ApiVersion.h"
#include "flow/TaskPriority.h"
#include "metacluster/Metacluster.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct TenantBalancer {
	explicit TenantBalancer(TenantBalancerInterface tbi,
	                        Reference<AsyncVar<ServerDBInfo> const> dbInfo,
	                        Reference<IClusterConnectionRecord> connRecord)
	  : tbi(tbi),
	    dbInfo(dbInfo),
	    connRecord(connRecord),
	    tenantBalancerMetrics("TenantBalancer", tbi.id().toString()),
	    getMovementStatusRequests("GetMovementStatusRequests", tenantBalancerMetrics),
	    listMovementsRequests("ListMovementsRequests", tenantBalancerMetrics),
	    startMoveTenantGroupRequests("StartMoveTenantGroupRequests", tenantBalancerMetrics),
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
	Counter listMovementsRequests;
	Counter startMoveTenantGroupRequests;
	Counter abortMovementRequests;
};

ACTOR
Future<Void> writeTenantBalancerInterfaceToDb(TenantBalancer* self) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->managementDb);
	loop {
		state Future<Void> dbInfoChanged = self->dbInfo->onChange();
		choose {
			when(PersistInterfaceRequest req = waitNext(self->tbi.persistInterface.getFuture())) {
				try {
					tr->reset();
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					TenantBalancerMetadata::getTenantBalancerInterface().set(tr, self->tbi);
					choose {
						when(wait(tr->commit())) {
							break;
						}
						when(wait(dbInfoChanged)) {}
					}
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}
			when(wait(dbInfoChanged)) {}
		}
	}
	return Void();
}

ACTOR Future<Void> getMovementStatus(TenantBalancer* self, GetMovementStatusRequest req) {
	++self->getMovementStatusRequests;
	TraceEvent(SevDebug, "TenantBalancerGetMovementStatus", self->tbi.id()).detail("TenantGroup", req.tenantGroupName);

	try {
		GetMovementStatusReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent(SevDebug, "TenantBalancerGetMovementStatusError", self->tbi.id())
		    .error(e)
		    .detail("TenantGroup", req.tenantGroupName);
		req.reply.sendError(e);
	}
	return Void();
}

ACTOR Future<Void> listMovements(TenantBalancer* self, ListMovementsRequest req) {
	++self->listMovementsRequests;
	TraceEvent(SevDebug, "TenantBalancerListMovements", self->tbi.id()).log();

	try {
		ListMovementsReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent("TenantBalancerListMovementsError", self->tbi.id()).error(e);
		req.reply.sendError(e);
	}
	return Void();
}

ACTOR Future<Void> startMoveTenantGroup(TenantBalancer* self, StartMoveTenantGroupRequest req) {
	++self->startMoveTenantGroupRequests;
	TraceEvent("TenantBalancerStartMoveTenantGroup", self->tbi.id()).log();
	wait(delay(0.1));

	try {
		StartMoveTenantGroupReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent("TenantBalancerStartMoveTenantGroupError", self->tbi.id())
		    .error(e)
		    .detail("TenantGroup", req.tenantGroupName);
		req.reply.sendError(e);
	}
	return Void();
}

ACTOR Future<Void> abortMovement(TenantBalancer* self, AbortMovementRequest req) {
	++self->abortMovementRequests;
	TraceEvent(SevDebug, "TenantBalancerAbortMovement", self->tbi.id()).detail("Tenant", req.tenantGroupName);

	try {
		AbortMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent("TenantBalancerAbortMovementError", self->tbi.id())
		    .error(e)
		    .detail("TenantGroup", req.tenantGroupName);
		req.reply.sendError(e);
	}
	return Void();
}

ACTOR Future<Void> tenantBalancerCore(TenantBalancer* self) {
	TraceEvent("TenantBalancerStarting", self->tbi.id()).log();
	self->actors.add(waitFailureServer(self->tbi.waitFailure.getFuture()));
	loop choose {
		when(HaltTenantBalancerRequest req = waitNext(self->tbi.haltTenantBalancer.getFuture())) {
			req.reply.send(Void());
			TraceEvent("TenantBalancerHalted", self->tbi.id()).detail("ReqID", req.requesterID);
			break;
		}
		when(wait(writeTenantBalancerInterfaceToDb(self))) {}
		when(GetMovementStatusRequest req = waitNext(self->tbi.getMovementStatus.getFuture())) {
			self->actors.add(getMovementStatus(self, req));
		}
		when(ListMovementsRequest req = waitNext(self->tbi.listMovements.getFuture())) {
			self->actors.add(listMovements(self, req));
		}
		when(StartMoveTenantGroupRequest req = waitNext(self->tbi.startMoveTenantGroup.getFuture())) {
			self->actors.add(startMoveTenantGroup(self, req));
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
