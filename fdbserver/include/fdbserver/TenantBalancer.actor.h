/*
 * TenantBalancer.actor.h
 * Copyright (c) 2023 Snowflake Computing
 */
#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_TENANT_BALANCER_ACTOR_G_H)
#define FDBSERVER_TENANT_BALANCER_ACTOR_G_H
#include "fdbserver/TenantBalancer.actor.g.h"
#elif !defined(FDBSERVER_TENANT_BALANCER_ACTOR_H)
#define FDBSERVER_TENANT_BALANCER_ACTOR_H

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/TenantBalancer.h"
#include "fdbclient/TenantBalancerInterface.h"

// This must be the last include
#include "flow/actorcompiler.h"

ACTOR
Future<Void> writeTenantBalancerInterfaceToDb(Database db, TenantBalancerInterface tbi) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			TenantBalancerMetadata::getTenantBalancerInterface().set(tr, tbi);
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
	return Void();
}

#include "flow/unactorcompiler.h"
#endif