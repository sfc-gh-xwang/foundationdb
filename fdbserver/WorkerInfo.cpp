/**
 * WorkerInfo.cpp
 */

#include "fdbserver/RoleFitness.h"
#include "fdbserver/WorkerInfo.h"

namespace {

// TODO: Remove copy of this function in ClusterController.actor.h when
// removing `betterMasterExists`.
void updateIdUsed(const std::vector<WorkerDetails>& workers, std::map<Optional<Standalone<StringRef>>, int>& id_used) {
	for (auto& it : workers) {
		id_used[it.interf.locality.processId()]++;
	}
}

} // namespace

bool WorkerRecruitment::operator<(WorkerRecruitment const& rhs) const {
	std::map<Optional<Standalone<StringRef>>, int> old_id_used;
	updateIdUsed(this->tLogs, old_id_used);
	updateIdUsed(this->satelliteTLogs, old_id_used);
	updateIdUsed(this->grvProxies, old_id_used);
	updateIdUsed(this->commitProxies, old_id_used);
	updateIdUsed(this->resolvers, old_id_used);
	updateIdUsed(this->backupWorkers, old_id_used);
	// TODO: Remote TLogs?
	updateIdUsed(this->oldLogRouters, old_id_used);
	RoleFitness oldTLogFit(this->tLogs, ProcessClass::TLog, old_id_used);
	RoleFitness oldSatelliteTLogFit(this->satelliteTLogs, ProcessClass::TLog, old_id_used);
	RoleFitness oldGrvProxyFit(this->grvProxies, ProcessClass::GrvProxy, old_id_used);
	RoleFitness oldCommitProxyFit(this->commitProxies, ProcessClass::CommitProxy, old_id_used);
	RoleFitness oldResolverFit(this->resolvers, ProcessClass::Resolver, old_id_used);
	RoleFitness oldBackupWorkerFit(this->backupWorkers, ProcessClass::Backup, old_id_used);
	RoleFitness oldLogRouterFit(this->oldLogRouters, ProcessClass::LogRouter, old_id_used);

	std::map<Optional<Standalone<StringRef>>, int> id_used;
	updateIdUsed(rhs.tLogs, id_used);
	updateIdUsed(rhs.satelliteTLogs, id_used);
	updateIdUsed(rhs.grvProxies, id_used);
	updateIdUsed(rhs.commitProxies, id_used);
	updateIdUsed(rhs.resolvers, id_used);
	updateIdUsed(rhs.backupWorkers, id_used);
	updateIdUsed(rhs.oldLogRouters, id_used);
	RoleFitness newTLogFit(rhs.tLogs, ProcessClass::TLog, id_used);
	// TODO: I'm unsure if this is equivalent to the existing
	// betterMasterExists logic, which checks the database configuration and
	// the satellite tlog replication factor
	RoleFitness newSatelliteTLogFit(this->satelliteTLogs, ProcessClass::TLog, id_used);
	RoleFitness newGrvProxyFit(rhs.grvProxies, ProcessClass::GrvProxy, id_used);
	RoleFitness newCommitProxyFit(rhs.commitProxies, ProcessClass::CommitProxy, id_used);
	RoleFitness newResolverFit(rhs.resolvers, ProcessClass::Resolver, id_used);
	RoleFitness newBackupWorkerFit(rhs.backupWorkers, ProcessClass::Backup, id_used);
	RoleFitness newLogRouterFit(rhs.oldLogRouters, ProcessClass::LogRouter, id_used);

	auto oldFit = std::make_tuple(oldTLogFit,
	                              oldSatelliteTLogFit,
	                              oldCommitProxyFit,
	                              oldGrvProxyFit,
	                              oldResolverFit,
	                              oldBackupWorkerFit,
	                              oldLogRouterFit);

	auto newFit = std::make_tuple(newTLogFit,
	                              newSatelliteTLogFit,
	                              newCommitProxyFit,
	                              newGrvProxyFit,
	                              newResolverFit,
	                              newBackupWorkerFit,
	                              newLogRouterFit);

	// Return true when the old fit is worse than the new fit. RoleFitness
	// comparison is opposite because fitness levels closer to 0 represent
	// better fitness than levels further away from 0. So if the old fit is
	// greater than the new fit, the new fit is better.
	return oldFit > newFit;
}
