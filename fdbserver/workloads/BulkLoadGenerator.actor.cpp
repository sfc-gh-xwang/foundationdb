#include <limits>
#include <string>
#include "fdbclient/BlobMetadataUtils.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/BulkLoadStager.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Optional.h"
#include "flow/Platform.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include "fmt/chrono.h"

namespace {
constexpr int32_t defaultBulkLoadSizeKiB = 1024; // 1 MiB
constexpr int32_t defaultTargetValueSizeBytes = 1024; // 1 KiB
} // namespace

// A standalone workload that generates and streams random data as a bulk load.
// This is intended for use for performance tests against real clusters.
// The amount of data loaded is specified by the "bulkLoadSizeKiB" parameter.
// The size of each value generated is specified by the "targetValueSizeBytes" parameter.
// Actual values are between 50-150% of that target.
struct BulkLoadGenerator : TestWorkload {
	static constexpr auto NAME = "BulkLoadGenerator";
	BulkLoadGenerator(WorkloadContext const& wcx)
	  : TestWorkload(wcx),
	    thisClientId(clientId),
	    keyPrefix("bulkload_" + std::to_string(thisClientId) + "_"),
	    bulkLoadedBytes("bulk_loaded_bytes"),
	    bulkLoadedRows("bulk_loaded_rows") {
		fakeSnowtramTableId = deterministicRandom()->randomInt(0, std::numeric_limits<int>::max());
		targetBulkLoadSizeBytes = 1024 * getOption(options, "bulkLoadSizeKiB"_sr, defaultBulkLoadSizeKiB);
		targetValueSizeBytes = getOption(options, "targetValueSizeBytes"_sr, defaultTargetValueSizeBytes);
		bstore = BlobConnectionProvider::newBlobConnectionProvider(SERVER_KNOBS->BG_URL);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return waitForBulkLoad(this, cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {
		if (endTime > 0) {
			m.push_back(PerfMetric("bulk_load_duration_seconds", endTime - startTime, Averaged::False));
			m.push_back(bulkLoadedBytes.getMetric());
			m.push_back(bulkLoadedRows.getMetric());
		}
	}

private:
	ACTOR Future<Void> waitForBulkLoad(BulkLoadGenerator* self, Database cx) {
		try {
			if (!SERVER_KNOBS->BULK_LOAD_STAGE_GRANULES || !CLIENT_KNOBS->ALLOW_BULK_LOADS) {
				// Skip this test if bulk loading is not enabled.
				fmt::print("Skipping bulk load when disabled. BULK_LOAD_STAGE_GRANULES: {0} , ALLOW_BULK_LOADS: {1}\n",
				           SERVER_KNOBS->BULK_LOAD_STAGE_GRANULES,
				           CLIENT_KNOBS->ALLOW_BULK_LOADS);
				return Void();
			}
			state BulkLoadID id = wait(cx->startBulkLoad(self->getBulkLoadKeyRange(), {}));
			state Reference<BulkLoadHelpers::BulkLoadStager> bulkLoader =
			    makeReference<BulkLoadHelpers::BulkLoadStager>(
			        cx, self->bstore, id, normalKeys, Optional<Reference<Tenant>>());
			self->startTime = timer_monotonic();
			while (self->bulkLoadedBytes.getValue() < self->targetBulkLoadSizeBytes) {
				state Optional<KeyValue> keyVal = self->getRandomKeyValue();
				if (!keyVal.has_value()) {
					// We hit limits on key value generation. In this rare case, just stop early
					// with whatever we were able to bulk load.
					break;
				}
				wait(bulkLoader->addRow(keyVal.get()));
				self->bulkLoadedBytes += keyVal.expectedSize();
				self->bulkLoadedRows += 1;
			}
			wait(bulkLoader->finishBulkLoad());
			self->endTime = timer_monotonic();
			const double duration = self->endTime - self->startTime;
			fmt::print("Client {0} finished bulk load. Stats:\n", self->thisClientId);
			fmt::print("  Bytes loaded: {0}\n", self->bulkLoadedBytes.getValue());
			fmt::print("  Rows loaded: {0}\n", self->bulkLoadedRows.getValue());
			fmt::print("  Start time: {:%m/%d/%y %H:%M:%S}\n", fmt::localtime(self->startTime));
			fmt::print("  End time:   {:%m/%d/%y %H:%M:%S}\n", fmt::localtime(self->endTime));
			fmt::print("  Duration: {0}s\n", duration);
			fmt::print("  Bytes/s: {0}\n", self->bulkLoadedBytes.getValue() / duration);
			fmt::print("  Rows/s: {0}\n", self->bulkLoadedRows.getValue() / duration);
		} catch (Error& e) {
			fmt::print("Client {0} experienced a Bulk Load error: {1}", self->thisClientId, e.what());
			throw e;
		}
		return Void();
	}

	KeyRange getBulkLoadKeyRange() {
		Key start = getKey(0);
		Key end = keyAfter(getKey(std::numeric_limits<uint32_t>::max()));
		return KeyRange(KeyRangeRef(start, end));
	}

	// Generates a key in fake Snowtram format. This consists of the following tuples:
	// {fakeTableId, fakeTableIncarnation, DATA space, keyPrefix, uint32 value, User-defined stamp}
	Key getKey(uint32_t intKey) {
		std::string stringKey = keyPrefix + std::to_string(intKey);
		Tuple::UserTypeStr udt(0x41, "RandomUDT"_sr);

		return Tuple::makeTuple(fakeSnowtramTableId,
		                        thisClientId,
		                        /*DATA space*/ 2,
		                        keyPrefix,
		                        (int64_t)intKey,
		                        udt)
		    .pack();
	}

	// Returns a random value of length between 50% and 150% of targetValueSizeBytes.
	Value getRandomValue() {
		uint32_t value = deterministicRandom()->randomUInt32();

		// Just for sanity, expect that the target val size is uint32_t aligned.
		ASSERT(targetValueSizeBytes % sizeof(uint32_t) == 0);
		int16_t targetValLength = targetValueSizeBytes / sizeof(uint32_t);
		int16_t valLen = (0.5 + deterministicRandom()->random01()) * targetValLength;
		valLen = std::max<int16_t>(valLen, 10);
		return genVal(value, valLen);
	}

	Optional<KeyValue> getRandomKeyValue() {
		// Either pick a random starting key in uint32 space, or increment the previous key.
		if (!nextIntKey.has_value()) {
			// Pick a random start key in the first 1/4th of the range to give us space to grow.
			nextIntKey = deterministicRandom()->randomUInt32() / 4;
		} else if (nextIntKey.get() == std::numeric_limits<uint32_t>::max()) {
			// If we hit the limit, stop generating rows.
			return {};
		} else {
			nextIntKey = nextIntKey.get() + 1;
		}

		Key k = getKey(nextIntKey.get());
		Value v = getRandomValue();
		return KeyValue(KeyValueRef(k, v));
	}

	const int thisClientId;
	const std::string keyPrefix;
	int fakeSnowtramTableId;
	PerfIntCounter bulkLoadedBytes;
	PerfIntCounter bulkLoadedRows;
	int64_t targetBulkLoadSizeBytes;
	int64_t targetValueSizeBytes;
	Reference<BlobConnectionProvider> bstore;

	double startTime = -1;
	double endTime = -1;
	Optional<uint32_t> nextIntKey;
};

WorkloadFactory<BulkLoadGenerator> BulkLoadGeneratorWorkloadFactory;
