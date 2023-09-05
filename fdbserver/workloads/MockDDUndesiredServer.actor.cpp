/*
 * MockDDUndesiredServer.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */
#include "fdbclient/FDBTypes.h"
#include "fdbserver/MockGlobalState.h"
#include "fdbserver/workloads/MockDDTest.h"
#include "fdbserver/MockDataDistributor.h"
#include "fdbserver/DDTxnProcessor.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class MockDDUndesiredServertWorkload : public MockDDTestWorkload {
public:
	static constexpr auto NAME = "MockDDUndesiredServer";
	Reference<DDSharedContext> ddcx;
	Reference<DDMockTxnProcessor> mock;
	MockDataDistributor dataDistributor;
	ActorCollection actors;
	int initialServerCnt;
	Reference<MockStorageServer> undesiredServer;
	// --- test configs ---

	explicit MockDDUndesiredServertWorkload(WorkloadContext const& wcx)
	  : MockDDTestWorkload(wcx),
	    ddcx(makeReference<DDSharedContext>(
	        DataDistributorInterface(LocalityData(), deterministicRandom()->randomUniqueID()))) {}

	Future<Void> setup(Database const& cx) override {
		if (!enabled)
			return Void();
		MockDDTestWorkload::setup(cx);
		// populate sharedMgs before run DD
		populateMgs();
		sharedMgs->addStoragePerProcess();
		mock = makeReference<DDMockTxnProcessor>(sharedMgs);
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (!enabled)
			return Void();

		// start mock servers
		actors.add(waitForAll(sharedMgs->runAllMockServers()));
		// start data distributor
		actors.add(dataDistributor.run(ddcx, mock));

		actors.add(markAsUndesiredServer(this));

		return timeout(reportErrors(actors.getResult(), "MockDDUndesiredServertWorkload"), testDuration, Void());
	}

	// Choose a MockStorageServer as undesired
	ACTOR static Future<Void> markAsUndesiredServer(MockDDUndesiredServertWorkload* self) {
		wait(delayJittered(self->testDuration / 10));
		std::map<UID, Reference<MockStorageServer>>& allServers = self->sharedMgs->allServers;
		self->initialServerCnt = allServers.size();
		// Randomly set one MockStorageServer from initTeam as undesired
		self->undesiredServer = allServers[deterministicRandom()->randomChoice(self->sharedMgs->initTeam)];
		self->undesiredServer->isUndesired = true;
		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (!enabled)
			return true;
		// After all shards were removed from undesiredServer, it should be successfully deleted from mgs
		ASSERT(initialServerCnt == sharedMgs->allServers.size() + 1);
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<MockDDUndesiredServertWorkload> MockDDUnhealthyRequestWorkload;