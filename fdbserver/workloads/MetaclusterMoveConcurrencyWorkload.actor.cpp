/*
 * MetaclusterMoveConcurrencyWorkload.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include <cstdint>
#include <limits>
#include <utility>
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbclient/ThrottleApi.h"
#include "fdbrpc/TenantName.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/CodeProbe.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/flow.h"

#include "metacluster/Metacluster.h"
#include "metacluster/MetaclusterConsistency.actor.h"
#include "metacluster/MetaclusterData.actor.h"
#include "metacluster/MetaclusterMetadata.h"
#include "metacluster/MetaclusterMove.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct MetaclusterMoveConcurrencyWorkload : TestWorkload {
	static constexpr auto NAME = "MetaclusterMoveConcurrency";

	struct DataClusterData {
		Database db;
		std::set<int64_t> tenants;
		std::set<TenantGroupName> tenantGroups;

		DataClusterData() {}
		DataClusterData(Database db) : db(db) {}
	};

	struct TestTenantData {

		TenantName name;
		ClusterName cluster;
		Optional<TenantGroupName> tenantGroup;

		TestTenantData() {}
		TestTenantData(TenantName name, ClusterName cluster, Optional<TenantGroupName> tenantGroup)
		  : name(name),
		    cluster(cluster),
		    tenantGroup(tenantGroup) {}
	};

	struct TenantGroupData {
		ClusterName cluster;
		std::set<int64_t> tenants;
	};

	int nodeCount;
	Key keyPrefix;

	Reference<IDatabase> managementDb;
	std::map<ClusterName, DataClusterData> dataDbs;
	std::vector<ClusterName> dataDbIndex;

	std::map<int64_t, TestTenantData> createdTenants;
	std::map<TenantName, int64_t> tenantNameIndex;
	std::map<TenantGroupName, TenantGroupData> tenantGroups;

	int initialTenants;
	int maxTenantGroups;
	int tenantGroupCapacity;
	// metacluster::metadata::management::MovementRecord moveRecord;

	int64_t reservedQuota;
	int64_t totalQuota;
	int64_t storageQuota;

	MetaclusterMoveConcurrencyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		nodeCount = getOption(options, "nodeCount"_sr, 5000.0);
		keyPrefix = unprintable(getOption(options, "keyPrefix"_sr, ""_sr).toString());
		initialTenants = getOption(options, "initialTenants"_sr, 20);
		maxTenantGroups = deterministicRandom()->randomInt(
		    1, std::min<int>(2 * initialTenants, getOption(options, "maxTenantGroups"_sr, 20)) + 1);
		tenantGroupCapacity =
		    std::max<int>(1, (initialTenants / 2 + maxTenantGroups - 1) / g_simulator->extraDatabases.size());
		reservedQuota = getOption(options, "reservedQuota"_sr, 0);
		totalQuota = getOption(options, "totalQuota"_sr, 1e8);
		storageQuota = getOption(options, "storageQuota"_sr, 1e8);
	}

	ClusterName chooseClusterName() { return dataDbIndex[deterministicRandom()->randomInt(0, dataDbIndex.size())]; }

	TenantName chooseTenantName() {
		TenantName tenant(format("tenant%08d", deterministicRandom()->randomInt(0, initialTenants)));
		return tenant;
	}

	TenantGroupName chooseTenantGroup(Optional<ClusterName> cluster = Optional<ClusterName>()) {
		TenantGroupName tenantGroup =
		    TenantGroupNameRef(format("tenantgroup%08d", deterministicRandom()->randomInt(0, maxTenantGroups)));
		if (cluster.present()) {
			auto const& existingGroups = dataDbs[cluster.get()].tenantGroups;
			if (!existingGroups.empty()) {
				tenantGroup = deterministicRandom()->randomChoice(
				    std::vector<TenantGroupName>(existingGroups.begin(), existingGroups.end()));
			}
		}

		return tenantGroup;
	}

	// Used to gradually increase capacity so that the tenants are somewhat evenly distributed across the clusters
	ACTOR static Future<Void> increaseMetaclusterCapacity(MetaclusterMoveConcurrencyWorkload* self) {
		self->tenantGroupCapacity = ceil(self->tenantGroupCapacity * 1.2);
		state Reference<ITransaction> tr = self->managementDb->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state int dbIndex;
				for (dbIndex = 0; dbIndex < self->dataDbIndex.size(); ++dbIndex) {
					metacluster::DataClusterMetadata clusterMetadata =
					    wait(metacluster::getClusterTransaction(tr, self->dataDbIndex[dbIndex]));
					metacluster::DataClusterEntry updatedEntry = clusterMetadata.entry;
					updatedEntry.capacity.numTenantGroups = self->tenantGroupCapacity;
					metacluster::updateClusterMetadata(
					    tr, self->dataDbIndex[dbIndex], clusterMetadata, {}, updatedEntry);
				}
				wait(safeThreadFutureToFuture(tr->commit()));
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}

		return Void();
	}

	ACTOR static Future<Void> createTenant(MetaclusterMoveConcurrencyWorkload* self) {
		state TenantName tenantName;
		for (int i = 0; i < 10; ++i) {
			tenantName = self->chooseTenantName();
			if (self->tenantNameIndex.count(tenantName) == 0) {
				break;
			}
		}

		if (self->tenantNameIndex.count(tenantName)) {
			return Void();
		}

		loop {
			try {
				metacluster::MetaclusterTenantMapEntry tenantEntry;
				tenantEntry.tenantName = tenantName;
				tenantEntry.tenantGroup = self->chooseTenantGroup();
				wait(metacluster::createTenant(self->managementDb,
				                               tenantEntry,
				                               metacluster::AssignClusterAutomatically::True,
				                               metacluster::IgnoreCapacityLimit::False));
				metacluster::MetaclusterTenantMapEntry createdEntry =
				    wait(metacluster::getTenant(self->managementDb, tenantName));
				TraceEvent(SevDebug, "MetaclusterMoveConcurrencyWorkloadCreatedTenant")
				    .detail("Tenant", tenantName)
				    .detail("TenantId", createdEntry.id);
				self->createdTenants[createdEntry.id] =
				    TestTenantData(tenantName, createdEntry.assignedCluster, createdEntry.tenantGroup);
				self->tenantNameIndex[tenantName] = createdEntry.id;
				auto& dataDb = self->dataDbs[createdEntry.assignedCluster];
				dataDb.tenants.insert(createdEntry.id);
				if (createdEntry.tenantGroup.present()) {
					auto& tenantGroupData = self->tenantGroups[createdEntry.tenantGroup.get()];
					tenantGroupData.cluster = createdEntry.assignedCluster;
					tenantGroupData.tenants.insert(createdEntry.id);
					dataDb.tenantGroups.insert(createdEntry.tenantGroup.get());
				}
				return Void();
			} catch (Error& e) {
				if (e.code() != error_code_metacluster_no_capacity) {
					throw;
				}

				wait(increaseMetaclusterCapacity(self));
			}
		}
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return _setup(cx, this);
		} else {
			return Void();
		}
	}

	ACTOR static Future<Void> _setup(Database cx, MetaclusterMoveConcurrencyWorkload* self) {
		metacluster::DataClusterEntry clusterEntry;
		clusterEntry.capacity.numTenantGroups = self->tenantGroupCapacity;

		metacluster::util::SimulatedMetacluster simMetacluster = wait(metacluster::util::createSimulatedMetacluster(
		    cx,
		    deterministicRandom()->randomInt(TenantAPI::TENANT_ID_PREFIX_MIN_VALUE,
		                                     TenantAPI::TENANT_ID_PREFIX_MAX_VALUE + 1),
		    clusterEntry));

		self->managementDb = simMetacluster.managementDb;
		ASSERT(!simMetacluster.dataDbs.empty());
		for (auto const& [name, db] : simMetacluster.dataDbs) {
			self->dataDbs[name] = DataClusterData(db);
			self->dataDbIndex.push_back(name);
		}

		TraceEvent(SevDebug, "MetaclusterMoveConcurrencyWorkloadCreateTenants")
		    .detail("NumTenants", self->initialTenants);

		while (self->createdTenants.size() < self->initialTenants) {
			wait(createTenant(self));
		}

		TraceEvent(SevDebug, "MetaclusterMoveConcurrencyWorkloadCreateTenantsComplete");

		// bulkSetup usually expects all clients
		// but this is being circumvented by the clientId==0 check
		self->clientCount = 1;
		// container of range-based for with continuation must be a state variable
		state std::map<ClusterName, DataClusterData> dataDbs = self->dataDbs;
		for (auto const& [clusterName, dataDb] : dataDbs) {
			state std::vector<Reference<Tenant>> dataTenants;
			state Database dbObj = dataDb.db;
			state std::set<TenantGroupName> dbTenantGroups = dataDb.tenantGroups;
			// Iterate over each data cluster and attempt to fill some of the tenants with data
			for (auto const& tId : dataDb.tenants) {
				TestTenantData testData = self->createdTenants[tId];
				TenantName tName = testData.name;
				TenantLookupInfo const tenantLookupInfo(tId, testData.tenantGroup);
				dataTenants.push_back(makeReference<Tenant>(dataDb.db, tenantLookupInfo, tName));
			}
			if (dataTenants.size()) {
				wait(bulkSetup(dbObj,
				               self,
				               self->nodeCount,
				               Promise<double>(),
				               false,
				               0.0,
				               1e12,
				               std::vector<uint64_t>(),
				               Promise<std::vector<std::pair<uint64_t, double>>>(),
				               0,
				               0.1,
				               0,
				               0,
				               dataTenants));
			}
			// Blobbify all tenants
			std::vector<Future<bool>> blobFutures;
			for (Reference<Tenant> tenantRef : dataTenants) {
				blobFutures.push_back(dbObj->blobbifyRangeBlocking(normalKeys, tenantRef));
			}
			wait(waitForAll(blobFutures));

			// Set storage and tag quotas
			state Reference<ReadYourWritesTransaction> tr = dbObj->createTransaction();
			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					for (auto const& tGroupName : dbTenantGroups) {
						ThrottleApi::setTagQuota(tr, tGroupName, self->reservedQuota, self->totalQuota);
						TenantMetadata::storageQuota().set(tr, tGroupName, self->storageQuota);
					}
					wait(tr->commit());
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}
		}

		TraceEvent(SevDebug, "MetaclusterMoveConcurrencyWorkloadPopulateTenantDataComplete");

		return Void();
	}

	Future<Optional<metacluster::metadata::management::MovementRecord>> tryGetMoveRecord(TenantGroupName tenantGroup) {
		return runTransaction(managementDb, [tenantGroup](Reference<ITransaction> tr) {
			tr->setOption(FDBTransactionOptions::RAW_ACCESS);
			return map(metacluster::metadata::management::emergency_movement::emergencyMovements().get(tr, tenantGroup),
			           [](Optional<metacluster::metadata::management::MovementRecord> record) { return record; });
		});
	}

	struct MoveBlock {
		TenantName tenant;
		Key begin;
		Key end;

		MoveBlock(TenantName tenant, Key begin, Key end) : tenant(tenant), begin(begin), end(end) {}
	};

	ACTOR static Future<Void> copyTenantData(MetaclusterMoveConcurrencyWorkload* self,
	                                         TenantGroupName tenantGroup,
	                                         MoveBlock moveBlock,
	                                         ClusterName srcDb,
	                                         ClusterName dstDb) {
		state KeyRangeRef keyRange(moveBlock.begin, moveBlock.end);

		state Database srcDbObj = self->dataDbs[srcDb].db;
		state Reference<Tenant> srcTenant = makeReference<Tenant>(srcDbObj, moveBlock.tenant);
		state Reference<ReadYourWritesTransaction> srcTr = makeReference<ReadYourWritesTransaction>(srcTenant);

		state RangeReadResult srcRange;
		loop {
			try {
				srcTr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(store(srcRange, srcTr->getRange(keyRange, 100000)));
				ASSERT(!srcRange.more);
				break;
			} catch (Error& e) {
				wait(srcTr->onError(e));
			}
		}

		state Database dstDbObj = self->dataDbs[dstDb].db;
		state Reference<Tenant> dstTenant = makeReference<Tenant>(dstDbObj, moveBlock.tenant);
		state Reference<ReadYourWritesTransaction> dstTr = makeReference<ReadYourWritesTransaction>(dstTenant);
		loop {
			try {
				dstTr->setOption(FDBTransactionOptions::LOCK_AWARE);
				for (const auto& [k, v] : srcRange) {
					dstTr->set(k, v);
				}
				wait(dstTr->commit());
				break;
			} catch (Error& e) {
				wait(dstTr->onError(e));
			}
		}

		return Void();
	}

	ACTOR static Future<Optional<MoveBlock>> getNextMoveBlock(MetaclusterMoveConcurrencyWorkload* self,
	                                                          Reference<ITransaction> tr,
	                                                          TenantGroupName tenantGroup,
	                                                          UID runId) {
		tr->setOption(FDBTransactionOptions::RAW_ACCESS);

		state Optional<std::pair<TenantName, Key>> optionalQueueHead =
		    wait(metacluster::metadata::management::emergency_movement::movementQueue().get(
		        tr, std::make_pair(tenantGroup, runId.toString())));

		state Tuple endTuple = Tuple::makeTuple(tenantGroup, runId.toString(), TenantName("\xff"_sr));

		if (!optionalQueueHead.present()) {
			KeyBackedRangeResult<std::pair<Tuple, Key>> firstSplitPoint =
			    wait(metacluster::metadata::management::emergency_movement::splitPointsMap().getRange(
			        tr, Tuple::makeTuple(tenantGroup, runId.toString()), endTuple, 1));

			if (firstSplitPoint.results.empty()) {
				return {};
			}

			auto const& headTuple = firstSplitPoint.results[0].first;
			optionalQueueHead = std::make_pair(headTuple.getString(2), headTuple.getString(3));
		}

		state std::pair<TenantName, Key> queueHead = optionalQueueHead.get();

		state Tuple beginTuple = Tuple::makeTuple(tenantGroup, runId.toString(), queueHead.first, queueHead.second);

		state KeyBackedRangeResult<std::pair<Tuple, Key>> splitPoints =
		    wait(metacluster::metadata::management::emergency_movement::splitPointsMap().getRange(
		        tr, beginTuple, endTuple, 2));

		if (splitPoints.results.empty()) {
			throw operation_failed();
		}
		state KeyRef headEnd = splitPoints.results[0].second;

		if (splitPoints.results.size() == 2) {
			Tuple nextTuple = splitPoints.results[1].first;
			TenantName nextTenantName = nextTuple.getString(2);
			Key nextKey = nextTuple.getString(3);
			metacluster::metadata::management::emergency_movement::movementQueue().set(
			    tr, std::make_pair(tenantGroup, runId.toString()), std::make_pair(nextTenantName, nextKey));
		} else {
			metacluster::metadata::management::emergency_movement::movementQueue().erase(
			    tr, std::make_pair(tenantGroup, runId.toString()));
		}

		return MoveBlock(queueHead.first, queueHead.second, headEnd);
	}

	ACTOR static Future<Void> processQueue(MetaclusterMoveConcurrencyWorkload* self,
	                                       TenantGroupName tenantGroup,
	                                       ClusterName srcDb,
	                                       ClusterName dstDb) {
		Optional<metacluster::metadata::management::MovementRecord> moveRecord =
		    wait(self->tryGetMoveRecord(tenantGroup));
		if (!moveRecord.present()) {
			return Void();
		}
		state UID runId = moveRecord.get().runId;
		loop {
			state Optional<MoveBlock> block = wait(
			    runTransaction(self->managementDb,
			                   [self = self, tenantGroup = tenantGroup, runId = runId](Reference<ITransaction> tr) {
				                   return getNextMoveBlock(self, tr, tenantGroup, runId);
			                   }));

			if (!block.present()) {
				return Void();
			}

			wait(copyTenantData(self, tenantGroup, block.get(), srcDb, dstDb));

			wait(runTransactionVoid(
			    self->managementDb,
			    [runId = runId, tenantGroup = tenantGroup, block = block](Reference<ITransaction> tr) {
				    tr->setOption(FDBTransactionOptions::RAW_ACCESS);
				    metacluster::metadata::management::emergency_movement::splitPointsMap().erase(
				        tr, Tuple::makeTuple(tenantGroup, runId.toString(), block.get().tenant, block.get().begin));
				    return Future<Void>(Void());
			    }));
		}
	}

	ACTOR static Future<Void> startMove(MetaclusterMoveConcurrencyWorkload* self,
	                                    TenantGroupName tenantGroup,
	                                    ClusterName srcCluster,
	                                    ClusterName dstCluster) {
		state std::vector<std::string> messages;
		loop {
			try {
				TraceEvent("MetaclusterMoveConcurrencyStartBegin")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				wait(metacluster::startTenantMovement(
				    self->managementDb, tenantGroup, srcCluster, dstCluster, &messages));
				TraceEvent(SevDebug, "MetaclusterMoveConcurrencyStartComplete")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				break;
			} catch (Error& e) {
				state Error err(e);
				TraceEvent("MetaclusterMoveConcurrencyWorkloadStartFailed")
				    .error(err)
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				if (err.code() == error_code_cluster_no_capacity) {
					CODE_PROBE(true, "Tenant move prevented by lack of cluster capacity");
					wait(increaseMetaclusterCapacity(self));
					continue;
				}
				throw err;
			}
		}
		return Void();
	}

	ACTOR static Future<Void> switchMove(MetaclusterMoveConcurrencyWorkload* self,
	                                     TenantGroupName tenantGroup,
	                                     ClusterName srcCluster,
	                                     ClusterName dstCluster) {
		state std::vector<std::string> messages;
		loop {
			try {
				TraceEvent("MetaclusterMoveConcurrencySwitchBegin")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				wait(metacluster::switchTenantMovement(
				    self->managementDb, tenantGroup, srcCluster, dstCluster, &messages));
				TraceEvent(SevDebug, "MetaclusterMoveConcurrencySwitchComplete")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				break;
			} catch (Error& e) {
				TraceEvent("MetaclusterMoveConcurrencyWorkloadSwitchFailed")
				    .error(e)
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				throw;
			}
		}
		return Void();
	}

	ACTOR static Future<Void> finishMove(MetaclusterMoveConcurrencyWorkload* self,
	                                     TenantGroupName tenantGroup,
	                                     ClusterName srcCluster,
	                                     ClusterName dstCluster) {
		state std::vector<std::string> messages;
		loop {
			state Optional<metacluster::metadata::management::MovementRecord> moveRecord =
			    wait(self->tryGetMoveRecord(tenantGroup));
			try {
				TraceEvent("MetaclusterMoveConcurrencyFinishBegin")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				wait(metacluster::finishTenantMovement(
				    self->managementDb, tenantGroup, srcCluster, dstCluster, &messages));
				TraceEvent(SevDebug, "MetaclusterMoveConcurrencyFinishComplete")
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				break;
			} catch (Error& e) {
				state Error err(e);
				TraceEvent("MetaclusterMoveConcurrencyWorkloadFinishFailed")
				    .error(err)
				    .detail("TenantGroup", tenantGroup)
				    .detail("SourceCluster", srcCluster)
				    .detail("DestinationCluster", dstCluster);
				if (err.code() == error_code_tenant_move_failed) {
					if (!moveRecord.present()) {
						throw err;
					}
					// Retryable error
					TraceEvent("MetaclusterMoveConcurrencyAdvanceVersion")
					    .detail("ClusterName", dstCluster)
					    .detail("TargetVersion", moveRecord.get().version);
					wait(advanceVersion(self->dataDbs[dstCluster].db, moveRecord.get().version));
					continue;
				}
				throw err;
			}
		}
		return Void();
	}

	ACTOR static Future<Void> moveTenantGroup(MetaclusterMoveConcurrencyWorkload* self,
	                                          TenantGroupName tenantGroup,
	                                          ClusterName srcCluster,
	                                          ClusterName dstCluster) {
		try {
			wait(startMove(self, tenantGroup, srcCluster, dstCluster));
		} catch (Error& e) {
			TraceEvent("MetaclusterMoveConcurrencyStartError").error(e);
		}
		try {
			wait(processQueue(self, tenantGroup, srcCluster, dstCluster));
		} catch (Error& e) {
			TraceEvent("MetaclusterMoveConcurrencyProcessQueueError").error(e);
		}
		try {
			wait(switchMove(self, tenantGroup, srcCluster, dstCluster));
		} catch (Error& e) {
			TraceEvent("MetaclusterMoveConcurrencySwitchError").error(e);
		}
		try {
			wait(finishMove(self, tenantGroup, srcCluster, dstCluster));
		} catch (Error& e) {
			TraceEvent("MetaclusterMoveConcurrencyFinishError").error(e);
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(cx, this);
		} else {
			return Void();
		}
	}

	ACTOR static Future<Void> _start(Database cx, MetaclusterMoveConcurrencyWorkload* self) {
		state ClusterName srcCluster = self->chooseClusterName();
		auto existingGroups = self->dataDbs[srcCluster].tenantGroups;

		state int tries = 0;
		state int tryLimit = 10;
		// Pick a cluster that has tenant groups
		while (existingGroups.empty()) {
			if (++tries >= tryLimit) {
				return Void();
			}
			srcCluster = self->chooseClusterName();
			existingGroups = self->dataDbs[srcCluster].tenantGroups;
		}
		state TenantGroupName tenantGroup = self->chooseTenantGroup(srcCluster);
		state ClusterName dstCluster = self->chooseClusterName();

		tries = 0;
		while (srcCluster == dstCluster) {
			if (++tries >= tryLimit) {
				return Void();
			}
			dstCluster = self->chooseClusterName();
		}

		state int numMoves = 5;
		state std::vector<Future<Void>> moveFutures;
		for (; numMoves > 0; numMoves--) {
			moveFutures.push_back(moveTenantGroup(self, tenantGroup, srcCluster, dstCluster));
			wait(delay(deterministicRandom()->random01() * 30));
		}
		wait(waitForAll(moveFutures));

		// At least one complete move should succeed
		// This will be indicated by the move metadata being erased
		state Optional<metacluster::metadata::management::MovementRecord> moveRecord =
		    wait(self->tryGetMoveRecord(tenantGroup));
		ASSERT(!moveRecord.present());
		// Check that the tenant group ends up where it's supposed to be
		state Optional<metacluster::MetaclusterTenantGroupEntry> tenantGroupEntry =
		    wait(metacluster::tryGetTenantGroup(self->managementDb, tenantGroup));
		ASSERT(tenantGroupEntry.present());
		ASSERT_EQ(tenantGroupEntry.get().assignedCluster, dstCluster);
		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId == 0) {
			return _check(cx, this);
		} else {
			return true;
		}
	}
	ACTOR static Future<bool> _check(Database cx, MetaclusterMoveConcurrencyWorkload* self) {
		// The metacluster consistency check runs the tenant consistency check for each cluster
		state metacluster::util::MetaclusterConsistencyCheck<IDatabase> metaclusterConsistencyCheck(
		    self->managementDb, metacluster::util::AllowPartialMetaclusterOperations::False);

		wait(metaclusterConsistencyCheck.run());

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
	Key keyForIndex(int n) { return key(n); }
	Key key(int n) { return doubleToTestKey((double)n / nodeCount, keyPrefix); }
	Value value(int n) { return doubleToTestKey(n, keyPrefix); }

	Standalone<KeyValueRef> operator()(int n) { return KeyValueRef(key(n), value((n + 1) % nodeCount)); }
};

WorkloadFactory<MetaclusterMoveConcurrencyWorkload> MetaclusterMoveConcurrencyWorkloadFactory;
