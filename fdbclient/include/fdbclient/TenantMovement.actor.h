#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_TENANT_MOVEMENT_ACTOR_G_H)
#define FDBCLIENT_TENANT_MOVEMENT_ACTOR_G_H
#include "fdbclient/TenantMovement.actor.g.h"
#elif !defined(FDBCLIENT_TENANT_MOVEMENT_ACTOR_H)
#define FDBCLIENT_TENANT_MOVEMENT_ACTOR_H

#include "fdbclient/IClientApi.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "fdbclient/TenantMovement.h"
#include "flow/actorcompiler.h" // has to be last include

ACTOR
template <typename IDatabase>
static Future<Future<Void>> getTbiChangedFuture(Reference<IDatabase> db) {
	state Reference<ITransaction> tr = db->createTransaction();
	state Future<Void> future = Never();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state ThreadFuture<Void> threadFuture = tr->watch("\xff/TenantBalancerInterf"_sr);
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
	state Optional<TenantBalancerInterface> interf;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			ThreadFuture<Optional<Value>> valueF = tr->get("\xff/TenantBalancerInterf"_sr);
			Optional<Value> value = wait(safeThreadFutureToFuture(valueF));
			if (value.present()) {
				interf = BinaryReader::fromStringRef<TenantBalancerInterface>(value.get(), IncludeVersion());
			}
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
	state Future<Void> onTbiChanged = wait(getTbiChangedFuture(managementDb));
	try {
		state MoveTenantsToClusterRequest req(tenant, dstClusterName);
		state Future<ErrorOr<MoveTenantsToClusterReply>> replyFuture = Never();
		state Future<Void> init = Void();
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

} // namespace MetaclusterAPI

#include "flow/unactorcompiler.h"
#endif