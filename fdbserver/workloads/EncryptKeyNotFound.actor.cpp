/*
 * EncryptKeyTenantNotFound.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/RESTSimKmsVault.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"

#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/flow.h"

// Workload generator to exercise 'encrypt_key_not_found' scenario; it does the following:
// 1. Create N tenants.
// 2. Construct a list of tenants among the created tenants for which fault will be injected. The list is persisted in
// SERVER_KNOBS->encrypt_simulate_key_not_found_ids_test
// 3. Issue M transactions against created tenants (random selection)
// 4. Txns issued for tenants selected for fault injection would fail.

struct EncryptKeyNotFoundWorkload : TestWorkload {
	static constexpr auto NAME = "EncryptKeyNotFound";

	const Key keyName = "key"_sr;
	const TenantName tenantNamePrefix = "encrypt_key_not_found_"_sr;

	int nTenants;
	int nIterations;
	int nKeysNotFoundTenants;
	Database dataDb;
	TenantName localTenantNamePrefix;
	std::vector<Reference<Tenant>> tenants;
	std::unordered_set<EncryptCipherDomainId> tenantsWithKeyMissingFaultInjections;

	EncryptKeyNotFoundWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		nIterations = getOption(options, "numIterations"_sr, 10);
		nTenants = getOption(options, "numTenants"_sr, deterministicRandom()->randomInt(10, 20));
		nKeysNotFoundTenants = getOption(options, "numKeysNotFoundTenants"_sr, deterministicRandom()->randomInt(1, 5));
		localTenantNamePrefix = format("%stenant_%d_", tenantNamePrefix.toString().c_str(), clientId);

		TraceEvent("EncryptKeyNotFoundWorkload")
		    .detail("NumIterations", nIterations)
		    .detail("NumTenants", nTenants)
		    .detail("NumKeysNotFoundTenants", nKeysNotFoundTenants)
		    .detail("LocalTenantPrefix", localTenantNamePrefix);
	}

	~EncryptKeyNotFoundWorkload() { TraceEvent("EncryptKeyNotFoundWorkloadDone"); }

	TenantName chooseTenantName(int count) {
		TenantName tenant(format("%s%08d", localTenantNamePrefix.toString().c_str(), count));
		return tenant;
	}

	ACTOR static Future<Void> _setup(Database cx, EncryptKeyNotFoundWorkload* self) {
		try {
			state int count = 0;
			state int i;
			state std::map<TenantName, TenantMapEntry> createdTenants;

			self->dataDb = cx;
			g_simulator->simEncryptKeyNotFoundIds.clear();

			for (i = 0; i < self->nTenants; ++i) {
				TenantName tenantName = self->chooseTenantName(i);
				ASSERT_EQ(createdTenants.count(tenantName), 0);

				Optional<TenantMapEntry> entry = wait(TenantAPI::createTenant(self->dataDb.getReference(), tenantName));
				ASSERT(entry.present());
				if (count < self->nKeysNotFoundTenants && deterministicRandom()->coinflip()) {
					g_simulator->simEncryptKeyNotFoundIds.insert(entry.get().id);
					self->tenantsWithKeyMissingFaultInjections.insert(entry.get().id);
				}
				Reference<Tenant> t = makeReference<Tenant>(self->dataDb, entry.get().id);
				t->name = entry->tenantName;
				self->tenants.push_back(t);
			}

			TraceEvent("SetupDone")
			    .detail("IdsCount", g_simulator->simEncryptKeyNotFoundIds.size())
			    .detail("CreatedTenants", self->tenants.size());

			return Void();
		} catch (Error& e) {
			TraceEvent("SetupFailed").error(e);
			throw;
		}
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(cx, this);
		} else {
			return Void();
		}
	}

	ACTOR Future<Void> _start(Database cx, EncryptKeyNotFoundWorkload* self) {
		state int count = 0;
		while (count++ < self->nIterations) {
			state int idx = deterministicRandom()->randomInt(0, self->tenants.size());
			state Transaction insertTr(makeReference<Tenant>(self->dataDb, self->tenants[idx]->id()));
			loop {
				try {
					// The value stored in the key will be the name of the tenant
					insertTr.set(self->keyName, self->tenants[idx]->name.get());
					wait(insertTr.commit());
					if (self->tenantsWithKeyMissingFaultInjections.count(self->tenants[idx]->id())) {
						// Transaction should be failed with 'encrypt_key_not_found' error
						ASSERT(false);
					}
					break;
				} catch (Error& e) {
					if (e.code() != error_code_encrypt_key_not_found) {
						wait(insertTr.onError(e));
					} else {
						ASSERT(self->tenantsWithKeyMissingFaultInjections.count(self->tenants[idx]->id()));
						ASSERT_GT(RestSimKms::getEncryptKeyNotFoundErrorCount(), 0);

						// encrypt_key_not_found is an expected error, continue
						break;
					}
				}
			}
			wait(yield());
		}

		ASSERT_GT(RestSimKms::getEncryptKeyNotFoundErrorCount(), 0);

		// reset fault injection, if not done, the test would fail resetting database state
		g_simulator->simEncryptKeyNotFoundIds.clear();

		TraceEvent("EncryptKeyNotFoundWorkloadDone");

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(cx, this);
		} else {
			return Void();
		}
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<EncryptKeyNotFoundWorkload> EncryptKeyNotFoundWorkloadFactory;