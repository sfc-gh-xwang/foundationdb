/**
 * UpdateStream.actor.cpp
 */

#include "fdbclient/UpdateStream.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // must be last include

class UpdateStreamWorkload : public TestWorkload {
	Key metadataPrefix;
	Key dataPrefix;
	int numKeys;
	int numProducers;
	int numConsumers;
	double producerDuration;
	double consumerStartDelay;
	int batchSize;
	int updatesPerTransaction;
	double delayBetweenTransactions;
	double clearProbability;

	class Consumer {
		std::unordered_map<Key, Value> kvState;
		Key metadataPrefix, dataPrefix;
		int id;

		ACTOR static Future<std::unordered_map<Key, Value>> readSnapshot(Database db, Key dataPrefix) {
			state Transaction tr(db);
			loop {
				try {
					RangeReadResult rangeResult = wait(tr.getRange(prefixRange(dataPrefix), CLIENT_KNOBS->TOO_MANY));
					std::unordered_map<Key, Value> result;
					for (const auto kv : rangeResult.contents()) {
						result[kv.key] = kv.value;
					}
					return result;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

		void applyUpdate(UpdateStream::Update const& update) {
			TraceEvent(SevDebug, "UpdateStreamWorkloadApplyingUpdate")
			    .detail("Key", update.key)
			    .detail("Value", update.value)
			    .detail("Index", update.index)
			    .detail("ConsumerId", id);
			if (update.value.present()) {
				kvState[update.key] = update.value.get();
			} else {
				kvState.erase(update.key);
			}
		}

		static Future<Void> randomDelay(double maxDuration) {
			if (deterministicRandom()->coinflip()) {
				return delay(deterministicRandom()->random01() * maxDuration);
			} else {
				return Void();
			}
		}

		ACTOR static Future<Void> run(Consumer* self, Database db, int batchSize) {
			state Reference<UpdateStream> updateStream;
			loop {
				updateStream = makeReference<UpdateStream>(
				    self->metadataPrefix, batchSize, /*accessSystemKeys=*/false, /*immediatePriority=*/false);
				Optional<uint64_t> startIndex = wait(updateStream->start(db));
				TraceEvent(SevDebug, "UpdateStreamWorkloadReadingSnapshot")
				    .detail("StartIndex", startIndex)
				    .detail("ConsumerId", self->id)
				    .detail("LeaseId", updateStream->getLeaseId());
				wait(randomDelay(5.0));
				wait(store(self->kvState, readSnapshot(db, self->dataPrefix)));
				wait(randomDelay(5.0));
				state FutureStream<UpdateStream::Update> updates = updateStream->getUpdates(db);
				loop {
					try {
						UpdateStream::Update update = waitNext(updates);
						self->applyUpdate(update);
						wait(randomDelay(0.1));
					} catch (Error& e) {
						TraceEvent(SevDebug, "UpdateStreamWorkloadConsumerError")
						    .error(e)
						    .detail("ConsumerId", self->id);
						if (e.code() == error_code_restart_update_stream) {
							wait(randomDelay(5.0));
							break;
						} else {
							throw e;
						}
					}
				}
			}
		}

		ACTOR static Future<Void> waitForConvergence(Consumer const* self, Database db) {
			loop {
				state std::unordered_map<Key, Value> snapshot = wait(readSnapshot(db, self->dataPrefix));
				if (self->kvState == snapshot) {
					TraceEvent(SevDebug, "UpdateStreamWorkloadConsumerReachedConvergence")
					    .detail("ConsumerId", self->id);
					return Void();
				} else {
					wait(delay(1.0));
				}
			}
		}

	public:
		Consumer(Key const& metadataPrefix, Key const& dataPrefix, int id)
		  : metadataPrefix(metadataPrefix),
		    dataPrefix(dataPrefix),
		    id(id) {}

		Future<Void> run(Database db, int batchSize) { return run(this, db, batchSize); }

		Future<Void> waitForConvergence(Database db) const { return waitForConvergence(this, db); }
	};

	Key getKey(uint64_t index) { return BinaryWriter::toValue(index, Unversioned()).withPrefix(dataPrefix); }

	ACTOR static Future<Void> runTransaction(UpdateStreamWorkload* self, Database db) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
		loop {
			try {
				state int i = 0;
				while (i < self->updatesPerTransaction) {
					Key key = self->getKey(deterministicRandom()->randomInt64(0, self->numKeys));
					Optional<Value> value;
					if (deterministicRandom()->random01() > self->clearProbability) {
						value = BinaryWriter::toValue(deterministicRandom()->randomUniqueID(), Unversioned());
					}
					wait(UpdateStream::update(tr, key, value, self->metadataPrefix));
					++i;
				}
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> producer(UpdateStreamWorkload* self, Database db) {
		loop {
			wait(delayJittered(self->delayBetweenTransactions));
			wait(runTransaction(self, db));
		}
	}

	Future<Void> runProducers(Database const& db) {
		std::vector<Future<Void>> producers;
		producers.reserve(numProducers);
		for (int i = 0; i < numProducers; ++i) {
			producers.push_back(producer(this, db));
		}
		return success(timeout(waitForAll(producers), producerDuration));
	}

	ACTOR static Future<Void> runConsumers(UpdateStreamWorkload* self, Database db, Future<Void> producersFinished) {
		state std::vector<Consumer> consumers;
		state std::vector<Future<Void>> runFutures;
		state std::vector<Future<Void>> convergenceFutures;
		wait(delay(self->consumerStartDelay));
		consumers.reserve(self->numConsumers);
		runFutures.reserve(self->numConsumers);
		for (int i = 0; i < self->numConsumers; ++i) {
			auto& consumer = consumers.emplace_back(self->metadataPrefix, self->dataPrefix, i);
			runFutures.push_back(consumer.run(db, self->batchSize));
		}
		wait(producersFinished);
		convergenceFutures.reserve(self->numConsumers);
		for (auto const& consumer : consumers) {
			convergenceFutures.push_back(consumer.waitForConvergence(db));
		}
		wait(waitForAll(convergenceFutures));
		return Void();
	}

public:
	static constexpr auto NAME = "UpdateStream";

	explicit UpdateStreamWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		metadataPrefix = getOption(options, "metadataPrefix"_sr, "updateStreamTestMetadata/"_sr);
		dataPrefix = getOption(options, "dataPrefix"_sr, "updateStreamTestData/"_sr);
		numKeys = getOption(options, "numKeys"_sr, 10);
		numProducers = getOption(options, "numProducers"_sr, deterministicRandom()->randomInt(1, 4));
		numConsumers = getOption(options, "numConsumers"_sr, deterministicRandom()->randomInt(1, 4));
		producerDuration = getOption(options, "producerDuration"_sr, 60.0);
		consumerStartDelay = getOption(options, "consumerStartDelay"_sr, deterministicRandom()->random01() * 20.0);
		batchSize = getOption(options, "batchSize"_sr, deterministicRandom()->randomInt(1, 11));
		updatesPerTransaction = getOption(options, "updatesPerTransaction"_sr, deterministicRandom()->randomInt(1, 6));
		delayBetweenTransactions =
		    getOption(options, "delayBetweenTransactions"_sr, 0.5 + deterministicRandom()->random01());
		clearProbability = getOption(options, "clearProbability"_sr, 0.2);
	}

	Future<Void> setup(Database const& db) override { return Void(); }

	Future<Void> start(Database const& db) override {
		if (clientId != 0) {
			return Void();
		} else {
			TraceEvent("UpdateStreamWorkloadStart")
			    .detail("MetadataPrefix", metadataPrefix)
			    .detail("DataPrefix", dataPrefix)
			    .detail("NumKeys", numKeys)
			    .detail("NumProducers", numProducers)
			    .detail("NumConsumers", numConsumers)
			    .detail("ProducerDuration", producerDuration)
			    .detail("ConsumerStartDelay", consumerStartDelay)
			    .detail("BatchSize", batchSize)
			    .detail("UpdatesPerTransaction", updatesPerTransaction)
			    .detail("DelayBetweenTransactions", delayBetweenTransactions)
			    .detail("ClearProbability", clearProbability);
			return runConsumers(this, db, runProducers(db));
		}
	}

	Future<bool> check(Database const& db) override { return true; }

	void getMetrics(std::vector<PerfMetric>&) override {}
};

WorkloadFactory<UpdateStreamWorkload> UpdateStreamWorkloadFactory;
