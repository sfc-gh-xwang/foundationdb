/*
 * RecruiterUnitTests.actor.cpp
 */

#include "fdbserver/ClusterController.actor.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/Recruiter.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

static int workers = 0;

// Adds a worker with the specified process class to the given
// ClusterControllerData's list of workers. This function simulates the
// behavior of `registerWorker`, and is used only for testing purposes.
void addWorker(ClusterControllerData& data, ProcessClass::ClassType classType, LocalityData const& locality) {
	Optional<Standalone<StringRef>> id = locality.processId();

	data.id_worker[id] = WorkerInfo();
	data.id_worker[id].details = WorkerDetails();
	data.id_worker[id].details.interf.locality = locality;
	data.id_worker[id].details.processClass = ProcessClass(classType, ProcessClass::ClassSource::CommandLineSource);
	data.id_worker[id].details.recoveredDiskFiles = true;
}

void addWorkers(ClusterControllerData& data,
                ProcessClass::ClassType classType,
                Optional<Standalone<StringRef>> zoneId,
                Optional<Standalone<StringRef>> machineId,
                Optional<Standalone<StringRef>> dcId,
                int count) {
	for (int i = 0; i < count; ++i) {
		Standalone<StringRef> id = StringRef(std::to_string(workers++));
		addWorker(data, classType, LocalityData(id, zoneId, machineId, dcId));
	}
}

// Returns the number of unique processes the workers in `workers` are running
// on.
int uniqueProcesses(WorkerRecruitment const& workers) {
	std::unordered_set<UID> ids;
	if (workers.sequencer.present()) {
		ids.insert(workers.sequencer.get().interf.id());
	}
	std::transform(workers.grvProxies.begin(),
	               workers.grvProxies.end(),
	               std::inserter(ids, ids.end()),
	               [](WorkerDetails const& w) { return w.interf.id(); });
	std::transform(workers.commitProxies.begin(),
	               workers.commitProxies.end(),
	               std::inserter(ids, ids.end()),
	               [](WorkerDetails const& w) { return w.interf.id(); });
	std::transform(workers.resolvers.begin(),
	               workers.resolvers.end(),
	               std::inserter(ids, ids.end()),
	               [](WorkerDetails const& w) { return w.interf.id(); });
	std::transform(workers.tLogs.begin(),
	               workers.tLogs.end(),
	               std::inserter(ids, ids.end()),
	               [](WorkerDetails const& w) { return w.interf.id(); });
	std::transform(workers.satelliteTLogs.begin(),
	               workers.satelliteTLogs.end(),
	               std::inserter(ids, ids.end()),
	               [](WorkerDetails const& w) { return w.interf.id(); });
	std::transform(workers.storageServers.begin(),
	               workers.storageServers.end(),
	               std::inserter(ids, ids.end()),
	               [](WorkerDetails const& w) { return w.interf.id(); });
	return ids.size();
}

} // namespace

TEST_CASE("/fdbserver/Recruiter/Stateless/GrvProxies") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "2"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 5);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto workers = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(workers.grvProxies.size(), 2);
	ASSERT_EQ(workers.commitProxies.size(), 1);
	ASSERT_EQ(workers.resolvers.size(), 1);

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/Stateless/CommitProxies") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "2"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 5);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto workers = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(workers.grvProxies.size(), 1);
	ASSERT_EQ(workers.commitProxies.size(), 2);
	ASSERT_EQ(workers.resolvers.size(), 1);

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/Stateless/Resolvers") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "2"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 5);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto workers = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(workers.grvProxies.size(), 1);
	ASSERT_EQ(workers.commitProxies.size(), 1);
	ASSERT_EQ(workers.resolvers.size(), 2);

	return Void();
}

// Run the cluster controller without explicitly setting its datacenter ID.
// This should result in a `no_more_servers` error when the recruited stateless
// processes (which do have a datacenter ID set) do not match the DC ID of the
// cluster controller.
TEST_CASE("/fdbserver/Recruiter/Stateless/CCNoDc") {
	DatabaseConfiguration conf;
	// conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	// conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "1"_sr));
	// conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData(), // don't set a dcID for the CC
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 5);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	try {
		auto workers = recruiter.findWorkers(&data, info, false);
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_no_more_servers);
	}

	return Void();
}

// Ensure we can recruit multiple unique stateless roles on a single process.
// Also checks that only one role of each type can be recruited on a process.
TEST_CASE("/fdbserver/Recruiter/Stateless/SingleProcessMultipleRoles") {
	// Configure the database to prefer two of each stateless role.
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "2"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "2"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "2"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 1);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto workers = recruiter.findWorkers(&data, info, false);
	// Check that only one of each role was actually recruited on the single
	// available process.
	ASSERT_EQ(workers.grvProxies.size(), 1);
	ASSERT_EQ(workers.commitProxies.size(), 1);
	ASSERT_EQ(workers.resolvers.size(), 1);

	// Check that all roles are running on the same process.
	ASSERT_EQ(workers.grvProxies.front().interf.id(), workers.commitProxies.front().interf.id());
	ASSERT_EQ(workers.grvProxies.front().interf.id(), workers.resolvers.front().interf.id());

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/Stateless/All") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "3"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "2"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 7);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto workers = recruiter.findWorkers(&data, info, false);
	ASSERT(workers.sequencer.present());
	ASSERT_EQ(workers.grvProxies.size(), 1);
	ASSERT_EQ(workers.grvProxies.size(), 1);
	ASSERT_EQ(workers.commitProxies.size(), 3);
	ASSERT_EQ(workers.resolvers.size(), 2);

	// Make sure all roles were recruited on separate processes.
	ASSERT_EQ(uniqueProcesses(workers), 7);

	return Void();
}

// Even when asking for zero stateless processes, we should always get at least
// one.
TEST_CASE("/fdbserver/Recruiter/Stateless/Zero") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "0"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "0"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "0"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 5);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto workers = recruiter.findWorkers(&data, info, false);
	ASSERT(workers.sequencer.present());
	ASSERT_EQ(workers.grvProxies.size(), 1);
	ASSERT_EQ(workers.commitProxies.size(), 1);
	ASSERT_EQ(workers.resolvers.size(), 1);

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/TLogs") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/logs"_sr, "5"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/log_replicas"_sr, "1"_sr));
	// The default tlog replication policy replicates across the "zoneid"
	// attribute. So make sure zoneid is set on the locality data for each
	// worker.
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::TransactionClass, "1"_sr, {}, "1"_sr, 5);
	// Add an explicit sequencer process to avoid a `no_more_servers` error.
	addWorkers(data, ProcessClass::ClassType::MasterClass, "1"_sr, {}, "1"_sr, 1);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	ASSERT_EQ(recruiter.findWorkers(&data, info, false).tLogs.size(), 5);

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/TLogs/NotEnoughDesired") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/logs"_sr, "5"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/log_replicas"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::TransactionClass, "1"_sr, {}, "1"_sr, 4);
	// Add an explicit sequencer process to avoid a `no_more_servers` error.
	addWorkers(data, ProcessClass::ClassType::MasterClass, "1"_sr, {}, "1"_sr, 1);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	ASSERT_EQ(recruiter.findWorkers(&data, info, false).tLogs.size(), 4);

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/TLogs/NotEnoughRequired") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/logs"_sr, "5"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/log_replicas"_sr, "3"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::TransactionClass, "1"_sr, {}, "1"_sr, 2);
	// Add an explicit sequencer process to ensure the `no_more_servers` error
	// is not thrown from a failure to find a sequencer.
	addWorkers(data, ProcessClass::ClassType::MasterClass, "1"_sr, {}, "1"_sr, 1);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	try {
		auto workers = recruiter.findWorkers(&data, info, false);
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_no_more_servers);
	}

	return Void();
}

// When the desired number of log replicas is not explicitly set, only one tlog
// gets recruited per zone (zones are used to define data replication
// boundaries). This occurs because the tlog recruitment algorithm attempts to
// make assignments based first on zones, and then only allows multiple
// processes in the same zone to be recruited at worse fitness levels if enough
// tlogs in different zones were recruited. The result is somewhat funny
// behavior where not being able to satisy the replication policy means the
// desired number of tlogs will not be recruited.
TEST_CASE("/fdbserver/Recruiter/TLogs/NoReplicationPolicy") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/logs"_sr, "5"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::TransactionClass, "1"_sr, {}, "1"_sr, 5);
	// Add an explicit sequencer process to avoid a `no_more_servers` error.
	addWorkers(data, ProcessClass::ClassType::MasterClass, "1"_sr, {}, "1"_sr, 1);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	ASSERT_EQ(recruiter.findWorkers(&data, info, false).tLogs.size(), 1);

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/BetterMasterExists/Noop") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 4);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto recruitment1 = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(uniqueProcesses(recruitment1), 4);

	// Adding another stateless process shouldn't trigger a recovery.
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 1);
	auto recruitment2 = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(uniqueProcesses(recruitment2), 4);

	ASSERT(!(recruitment1 < recruitment2));
	ASSERT(!(recruitment2 < recruitment1));

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/BetterMasterExists/Unset") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/logs"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/log_replicas"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::UnsetClass, "1"_sr, {}, "1"_sr, 1);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto recruitment1 = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(uniqueProcesses(recruitment1), 1);

	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 1);
	auto recruitment2 = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(uniqueProcesses(recruitment2), 2);

	ASSERT(recruitment1 < recruitment2);

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/BetterMasterExists/AddGrvProxy") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 4);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto recruitment1 = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(uniqueProcesses(recruitment1), 4);

	addWorkers(data, ProcessClass::ClassType::GrvProxyClass, "1"_sr, {}, "1"_sr, 1);
	auto recruitment2 = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(uniqueProcesses(recruitment2), 4);

	ASSERT(recruitment1 < recruitment2);

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/BetterMasterExists/AddCommitProxy") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 4);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto recruitment1 = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(uniqueProcesses(recruitment1), 4);

	addWorkers(data, ProcessClass::ClassType::CommitProxyClass, "1"_sr, {}, "1"_sr, 1);
	auto recruitment2 = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(uniqueProcesses(recruitment2), 4);

	ASSERT(recruitment1 < recruitment2);

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/BetterMasterExists/AddResolver") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 4);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto recruitment1 = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(uniqueProcesses(recruitment1), 4);

	addWorkers(data, ProcessClass::ClassType::ResolutionClass, "1"_sr, {}, "1"_sr, 1);
	auto recruitment2 = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(uniqueProcesses(recruitment2), 4);

	ASSERT(recruitment1 < recruitment2);

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/BetterMasterExists/AddTLog") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/logs"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/log_replicas"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, "1"_sr, {}, "1"_sr, 4);
	addWorkers(data, ProcessClass::ClassType::StorageClass, "1"_sr, {}, "1"_sr, 1);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto recruitment1 = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(uniqueProcesses(recruitment1), 5);

	addWorkers(data, ProcessClass::ClassType::TransactionClass, "1"_sr, {}, "1"_sr, 1);
	auto recruitment2 = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(uniqueProcesses(recruitment2), 5);

	ASSERT(recruitment1 < recruitment2);

	return Void();
}
