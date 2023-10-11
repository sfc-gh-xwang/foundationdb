/*
 * LoadableCache.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_LOADABLE_CACHE_ACTOR_G_H)
#define FDBCLIENT_LOADABLE_CACHE_ACTOR_G_H
#include "fdbclient/LoadableCache.actor.g.h"
#elif !defined(FDBCLIENT_LOADABLE_CACHE_ACTOR_H)
#define FDBCLIENT_LOADABLE_CACHE_ACTOR_H

#define DEBUG_LOADABLE_CACHE 0

#include "flow/FastRef.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/IRandom.h"

#include <any>
#include <functional>
#include <limits>
#include <unordered_map>

#include "flow/actorcompiler.h" // has to be last include

// LoadableCacheOpStats interface
//
// Metrics tracking cache operations per request, it accounts for:
// 1. Cache hits
// 2. Cache misses, populated by already in-progress key-loads (some other request(s))
// 3. Cache misses, key-loads done as part of current request processing

class LoadableCacheOpStats : public ReferenceCounted<LoadableCacheOpStats>, NonCopyable {
public:
	int nHits;
	int nDedups;
	int nFetched;
	int nExpiredEvicted;

	LoadableCacheOpStats() : nHits(0), nDedups(0), nFetched(0), nExpiredEvicted(0) {}

	static void reset(Reference<LoadableCacheOpStats> self) {
		if (self.isValid()) {
			self->nHits = self->nDedups = self->nFetched = 0;
		}
	}

	static void updateHits(Reference<LoadableCacheOpStats> self, const int count) {
		if (self.isValid()) {
			self->nHits += count;
		}
	}

	static void updateDedups(Reference<LoadableCacheOpStats> self, const int count) {
		if (self.isValid()) {
			self->nDedups += count;
		}
	}

	static void updateFetched(Reference<LoadableCacheOpStats> self, const int count) {
		if (self.isValid()) {
			self->nFetched += count;
		}
	}

	static void updateExpiredEvicted(Reference<LoadableCacheOpStats> self, const int count) {
		if (self.isValid()) {
			self->nExpiredEvicted += count;
		}
	}
};

// LoadableCache interface
//
// In-memory caching interface, salient points:
// 1. Read-through caching semantics
// 2. Client supplied value-loader interface
// 3. The cache implements load-key 'atmost once' semantics where, if a key is getting loaded (in-progress), any
// subsequent lookups would piggyback on key-loading operation.
//
// Note: Caller should implement 'toString' for KeyType is running with DEBUG_LOADABLE_CACHE enabled.

template <typename KeyType,
          typename ValueType,
          typename Hash = std::hash<KeyType>,
          typename KeyEqual = std::equal_to<KeyType>>
class LoadableCache : public ReferenceCounted<LoadableCache<KeyType, ValueType, Hash, KeyEqual>> {
	using KVMap = std::unordered_map<KeyType, ValueType, Hash, KeyEqual>;
	using KeyVec = std::vector<KeyType>;
	using KeySet = std::unordered_set<KeyType>;
	using LoadValueFunc = std::function<Future<Void>(std::any data, const KeyVec&, KVMap*)>;
	using CheckValueValidFunc = std::function<bool(const ValueType& v)>;

public:
	LoadableCache(UID id, LoadValueFunc loaderFunc)
	  : myId(id),
	    loadValueFunc(loaderFunc),
	    checkValueValidFunc(nullptr) {}
	LoadableCache(UID id, LoadValueFunc loaderFunc, CheckValueValidFunc valueValidFunc)
	  : myId(id),
	    loadValueFunc(loaderFunc),
	    checkValueValidFunc(valueValidFunc) {}
	LoadableCache(LoadValueFunc loaderFunc)
	  : myId(deterministicRandom()->randomUniqueID()),
	    loadValueFunc(loaderFunc),
	    checkValueValidFunc(nullptr) {}
	LoadableCache(LoadValueFunc loaderFunc, CheckValueValidFunc valueValidFunc)
	  : myId(deterministicRandom()->randomUniqueID()),
	    loadValueFunc(loaderFunc),
	    checkValueValidFunc(valueValidFunc) {}

	~LoadableCache() { disableKeyRefreshes(); }

	// Single key cache lookup API
	// API returns corresponding ValueType future if key is present or can be loaded into the cache
	//
	// Input:
	// key - key to lookup
	// data - opaque pointer passed by the caller, assist in passing caller context if key fetch needs to be done
	// expDurationSec - Optional expiration for cached cell, if not present, then expiry = double::max()
	// stats - instance of LoadableCacheOpStats
	//
	// Return:
	// Corresponding Value if cached and/or fetched, Optional<>() if failed to fetch the value.

	Future<Optional<ValueType>> get(const KeyType& key,
	                                std::any data,
	                                Optional<double> expDurationSec,
	                                Reference<LoadableCacheOpStats> stats = Reference<LoadableCacheOpStats>()) {
		ASSERT(!expDurationSec.present() || expDurationSec.get() > 0);

		LoadableCacheOpStats::reset(stats);
		return getImpl(Reference<LoadableCache<KeyType, ValueType, Hash, KeyEqual>>::addRef(this),
		               key,
		               data,
		               expDurationSec.present() ? expDurationSec.get() : std::numeric_limits<double>::max(),
		               stats);
	}

	// Single key cache lookup API
	// API returns corresponding ValueType future if key is present or can be loaded into the cache. Caller is expected
	// to supply CheckValueValidFunc during cache initialization.
	//
	// Input:
	// key - key to lookup
	// data - opaque pointer passed by the caller, assist in passing caller context if key fetch needs to be done
	// stats - instance of LoadableCacheOpStats
	//
	// Return:
	// Corresponding Value if cached and/or fetched, Optional<>() if failed to fetch the value.

	Future<Optional<ValueType>> get(const KeyType& key,
	                                std::any data,
	                                Reference<LoadableCacheOpStats> stats = Reference<LoadableCacheOpStats>()) {

		ASSERT(checkValueValidFunc != nullptr);

		LoadableCacheOpStats::reset(stats);
		return getImpl(Reference<LoadableCache<KeyType, ValueType, Hash, KeyEqual>>::addRef(this),
		               key,
		               data,
		               Optional<double>(),
		               stats);
	}

	// Vector Cache lookup API
	//
	// Input:
	// {keys} - list of keys to lookup
	// data - opaque pointer passed by the caller, assist in passing caller context if key fetch needs to be done
	// expDurationSec - Optional expiration for cached cell, if not present, then expiry = double::max()
	// stats - instance of LoadableCacheOpStats
	//
	// Return:
	// std::unordered_map of {KeyType, ValueType} for all successful key lookups. Caller is responsible to handle
	// missing-key scenario. Caller is responsible to handle missing - key scenario.Further, cache supports partial
	// key-lookup semantics i.e. no error is thrown if subset of keys can not be resolved.

	Future<KVMap> get(const KeyVec& keys,
	                  std::any data,
	                  Optional<double> expDurationSec,
	                  Reference<LoadableCacheOpStats> stats = Reference<LoadableCacheOpStats>()) {

		ASSERT(!expDurationSec.present() || expDurationSec.get() > 0);

		LoadableCacheOpStats::reset(stats);
		if (keys.empty()) {
			return {};
		}

		return resolveUncachedKeys(Reference<LoadableCache<KeyType, ValueType, Hash, KeyEqual>>::addRef(this),
		                           keys,
		                           data,
		                           expDurationSec.present() ? expDurationSec.get() : std::numeric_limits<double>::max(),
		                           stats);
	}

	// Vector Cache lookup API. Caller is expected to provide valid CheckValueValidFunc during cache initialization
	//
	// Input:
	// {keys} - list of keys to lookup
	// data - opaque pointer passed by the caller, assist in passing caller context if key fetch needs to be done
	// stats - instance of LoadableCacheOpStats
	//
	// Return:
	// std::unordered_map of {KeyType, ValueType} for all successful key lookups. Caller is responsible to handle
	// missing-key scenario. Caller is responsible to handle missing - key scenario.Further, cache supports partial
	// key-lookup semantics i.e. no error is thrown if subset of keys can not be resolved.

	Future<KVMap> get(const KeyVec& keys,
	                  std::any data,
	                  Reference<LoadableCacheOpStats> stats = Reference<LoadableCacheOpStats>()) {

		ASSERT(checkValueValidFunc != nullptr);

		LoadableCacheOpStats::reset(stats);

		if (keys.empty()) {
			return {};
		}

		return resolveUncachedKeys(Reference<LoadableCache<KeyType, ValueType, Hash, KeyEqual>>::addRef(this),
		                           keys,
		                           data,
		                           Optional<double>(),
		                           stats);
	}

	// Vectorized cache insert API.
	// Enables the caller to insert {key, value} directly into the cache.
	//
	// Input:
	// records - vector{Key, Value}
	// ttl - Optional time-to-live value, values gets applied to all record entries supplied by the caller.
	//
	// Return:
	// vector<KeyType> - list of keys inserted into the cache
	//
	// Note: API will insert key into cache IFF either is true:
	//       1. Key is not already cached.
	//       2. Cached key is expired.

	std::vector<KeyType> put(const std::vector<std::pair<KeyType, ValueType>>& records, Optional<double> ttl) {
		std::vector<KeyType> inserted;
		for (const auto& record : records) {
			if (put(record.first, record.second, ttl)) {
				inserted.push_back(record.first);
			}
		}

		if (DEBUG_LOADABLE_CACHE) {
			TraceEvent("LoadableCachePutKeys", id())
			    .detail("ToInsert", records.size())
			    .detail("Inserted", inserted.size());
		}

		return inserted;
	}

	// Single cell cache insert API.
	// Enables the caller to insert {key, value} directly into the cache.
	//
	// Input:
	// key
	// value
	// ttl - Optional time-to-live value
	//
	// Return:
	// true, if value is inserted, false, otherwise.
	//
	// Note: API will insert key into cache IFF either is true:
	//       1. Key is not already cached.
	//       2. Cached key is expired.

	bool put(const KeyType& key, const ValueType& value, Optional<double> ttl) {
		Optional<CacheValue> c = lookup(key, Reference<LoadableCacheOpStats>());
		if (!c.present()) {
			CacheEntry newEntry =
			    checkValueValidFunc == nullptr
			        ? CacheEntry(key, value, ttl.present() ? ttl.get() : std::numeric_limits<double>::max())
			        : CacheEntry(key, value);
			cache[key] = Promise<CacheEntry>();
			cache[key].send(newEntry);

			if (DEBUG_LOADABLE_CACHE) {
				TraceEvent("LoadableCachePutKey", id())
				    .detail("Key", newEntry.key)
				    .detail("Value", newEntry.value)
				    .detail("Ttl", newEntry.ttl)
				    .detail("IsValid", newEntry.isValid);
			}

			return true;
		}
		return false;
	}

	// Routine allows caller to auto-refresh 'expired' keys.
	//
	// Input:
	// data - caller supplied data pointer, need to load keys to cache
	// refreshInterval - frequency of background refreshes
	// initialDelay - delay after which the recurring refresh-key task gets launched
	// ttl - time-to-live for newly fetched keys, needed if cached is configured with no CheckValueValidFunc callback
	//
	// Note: Useful feature when cache provides critical data used in latency-sensitive path. Approach can avoid inline
	// fetch of data due to 'expired' cached value, further, any subsequent lookup for cached key will need to wait for
	// refresh task to fetch the key (de-duped effort)

	void enableKeyRefreshes(std::any data, double refreshInterval, double initialDelay, Optional<double> ttl) {
		if (!refreshKeysCtx.isValid()) {
			refreshKeysCtx = makeReference<RefreshKeysCtx>();
			refreshKeysCtx->cbData = data;
			if (checkValueValidFunc == nullptr) {
				refreshKeysCtx->ttl = ttl.present() ? ttl.get() : std::numeric_limits<double>::max();
			}

			refreshKeysCtx->task = recurringAsync([&]() { return refreshKeys(); }, refreshInterval, true, initialDelay);
			TraceEvent(SevDebug, "LoadableCacheEnableRefreshKeys", myId);
		} else {
			TraceEvent(SevDebug, "LoadableCacheEnableRefreshKeysAlreadyEnabled", myId);
		}
	}

	// Routine disables background auto-refresh of cached, but, 'expired' keys
	void disableKeyRefreshes() {
		if (refreshKeysCtx.isValid()) {
			refreshKeysCtx.clear();
			ASSERT(!refreshKeysCtx.isValid());
			TraceEvent("LoadableCacheRefreshDisableDone", id());
		} else {
			TraceEvent("LoadableCacheRefreshAlreadyDisabled");
		}
	}

	size_t size() const { return cache.size(); }

	// API trims invalid and/or expired cached cell
	void trim() {
		auto it = cache.begin();
		while (it != cache.end()) {
			CacheEntry entry = it->second.getFuture().get();
			if (expired(entry) || !entry.isValid) {
				it = cache.erase(it);
			} else {
				it++;
			}
		}
	}

	bool isRefreshKeyEnabled() const { return refreshKeysCtx.isValid(); }
	UID id() const { return myId; }

	int64_t numKeysRefreshed() const {
		if (refreshKeysCtx.isValid()) {
			return refreshKeysCtx->numKeysRefreshed;
		}
		return 0;
	}

	int64_t numRefreshErrors() const {
		if (refreshKeysCtx.isValid()) {
			return refreshKeysCtx->numKeysRefreshErrors;
		}
		return 0;
	}

private:
	struct CacheEntry {
		// Optimization: embedded key in cached 'value'; approach avoids cache lookup when key-fetch is deduped across
		// different actors; memory versus cpu-time tradeoff

		KeyType key;
		ValueType value;
		Optional<double> ttl;
		bool isValid;

		CacheEntry() : ttl(std::numeric_limits<double>::max()), isValid(false) {}
		CacheEntry(const KeyType& k, const ValueType& v) : key(k), value(v), isValid(true) {}
		CacheEntry(const KeyType& k, const ValueType& v, const double expDurationSec)
		  : key(k),
		    value(v),
		    isValid(true) {
			if (expDurationSec != std::numeric_limits<double>::max()) {
				ttl = now() + expDurationSec;
			} else {
				ttl = std::numeric_limits<double>::max();
			}
		}
	};

	using CacheValue = Promise<CacheEntry>;
	using KVMapImpl = std::unordered_map<KeyType, CacheValue, Hash, KeyEqual>;
	using KVUnresolved = std::pair<KeyType, Optional<CacheValue>>;
	using KVUnresolvedVec = std::vector<KVUnresolved>;

	UID myId;
	KVMapImpl cache;
	// Client supplied callback assisted load of keys if missing from cache
	LoadValueFunc loadValueFunc;

	// Client supplied callback to validate if a given "cached value" is valid, approach assists usecases where 'value'
	// data model controls the validation of the "cached value"
	CheckValueValidFunc checkValueValidFunc;

	struct RefreshKeysCtx : ReferenceCounted<RefreshKeysCtx>, NonCopyable {
		std::any cbData;
		Optional<double> ttl;
		Future<Void> task;
		int64_t numKeysRefreshed;
		int64_t numKeysRefreshErrors;

		RefreshKeysCtx() : cbData(std::any()), numKeysRefreshed(0), numKeysRefreshErrors(0) {}
		~RefreshKeysCtx() { deinit(); }

		void deinit() {
			task.cancel();
			cbData.reset();
			ttl.reset();
			numKeysRefreshed = numKeysRefreshErrors = 0;
		}
	};
	Reference<RefreshKeysCtx> refreshKeysCtx;

	bool expired(const CacheEntry& entry) const {
		if (checkValueValidFunc != nullptr) {
			return checkValueValidFunc(entry.value);
		}
		ASSERT(entry.ttl.present());
		return now() >= entry.ttl.get();
	}

	Optional<CacheValue> lookup(const KeyType& key,
	                            Reference<LoadableCacheOpStats> stats = Reference<LoadableCacheOpStats>()) {
		auto it = cache.find(key);
		if (it == cache.end()) {
			return Optional<CacheValue>();
		}

		CacheValue entryPromise = it->second;
		if (entryPromise.isSet()) {
			if (expired(entryPromise.getFuture().get())) {
				// cached key is 'expired', remove entry and fetch a new one
				cache.erase(it);
				LoadableCacheOpStats::updateExpiredEvicted(stats, 1);

				if (DEBUG_LOADABLE_CACHE) {
					TraceEvent("LoadableCacheErase").detail("Key", key).detail("Size", cache.size());
				}
				CODE_PROBE(true, "LoadableCache remove expired keys");
				return Optional<CacheValue>();
			}
		}
		return entryPromise;
	}

	ACTOR static Future<Void> refreshKeysImpl(Reference<LoadableCache<KeyType, ValueType, Hash, KeyEqual>> self,
	                                          KeyVec toRefresh,
	                                          Reference<RefreshKeysCtx> refreshKeysCtx) {
		state Reference<LoadableCacheOpStats> stats = makeReference<LoadableCacheOpStats>();

		try {
			KVMap ret = wait(resolveUncachedKeys(self, toRefresh, refreshKeysCtx->cbData, refreshKeysCtx->ttl, stats));
			refreshKeysCtx->numKeysRefreshed += ret.size();

			TraceEvent("LoadableCacheRefreshKeys", self->id())
			    .detail("NumKeys", ret.size())
			    .detail("Total", refreshKeysCtx->numKeysRefreshed);
			if (DEBUG_LOADABLE_CACHE) {
				for (const auto& item : ret) {
					TraceEvent("LoaadableCacheKeyRefreshed", self->id())
					    .detail("Key", item.first)
					    .detail("Value", item.second);
				}
			}
			CODE_PROBE(true, "LoadableCache auto refresh cache");
		} catch (Error& e) {
			TraceEvent("LoadableCacheRefreshKeysError", self->id()).error(e);
			refreshKeysCtx->numKeysRefreshErrors++;
			CODE_PROBE(true, "LoadableCache auto refresh cache error");
		}

		return Void();
	}

	Future<Void> refreshKeys() {
		ASSERT(refreshKeysCtx.isValid());

		KeyVec toRefresh;
		for (auto& it : cache) {
			CacheValue entryPromise = it.second;
			if (entryPromise.isSet() && expired(entryPromise.getFuture().get())) {
				toRefresh.push_back(it.first);
			}
		}

		return refreshKeysImpl(
		    Reference<LoadableCache<KeyType, ValueType, Hash, KeyEqual>>::addRef(this), toRefresh, refreshKeysCtx);
	}

	ACTOR static Future<Optional<ValueType>> getImpl(Reference<LoadableCache<KeyType, ValueType, Hash, KeyEqual>> self,
	                                                 KeyType key,
	                                                 std::any data,
	                                                 Optional<double> expDurationSec,
	                                                 Reference<LoadableCacheOpStats> stats) {
		KVMap rMap = wait(resolveUncachedKeys(self, { key }, data, expDurationSec, stats));
		if (rMap.empty()) {
			return Optional<ValueType>();
		}
		auto it = rMap.find(key);
		ASSERT(it != rMap.end());
		return it->second;
	}

	ACTOR static Future<KVMap> resolveUncachedKeys(Reference<LoadableCache<KeyType, ValueType, Hash, KeyEqual>> self,
	                                               KeyVec keys,
	                                               std::any data,
	                                               Optional<double> expDurationSec,
	                                               Reference<LoadableCacheOpStats> stats) {
		state KVMap rMap;
		state std::vector<Future<CacheEntry>> pendingFetches;
		state KeyVec toFetch;
		state Future<Void> fetchFuture;
		state KVMap fetchedKVMap;

		// Steps involved:
		// 1. Lookup already cached elements
		// 2. Otherwise two possibilities:
		//  2.a If element present in cache, but, not 'ready'; record corresponding 'Future' to wait upon
		//  2.b If element not present, spin up an actor to fetch and cache the elements

		try {
			for (const auto& k : keys) {
				Optional<CacheValue> c = self->lookup(k, stats);
				if (c.present()) {
					if (c.get().isSet()) {
						CacheEntry entry = c.get().getFuture().get();
						if (entry.isValid) {
							rMap[k] = c.get().getFuture().get().value;
						} else {
							// ignore the entries corresponding to invalid-key
						}
					} else {
						if (DEBUG_LOADABLE_CACHE) {
							TraceEvent("LoadableCachePendingFetch").detail("Key", k);
						}
						pendingFetches.push_back(c.get().getFuture());
					}
				} else {
					auto result = self->cache.emplace(k, Promise<CacheEntry>());
					ASSERT(result.second);
					toFetch.push_back(k);

					if (DEBUG_LOADABLE_CACHE) {
						TraceEvent("LoadableCacheKeyNotFound").detail("Key", k);
					}
				}
			}

			ASSERT_LE(rMap.size(), keys.size());

			LoadableCacheOpStats::updateHits(stats, rMap.size());

			if (rMap.size() == keys.size()) {
				ASSERT(pendingFetches.empty());
				return rMap;
			}

			// Approach parallelizes: a) wait for in-progress key fetches b) fetch uncached elements into the
			// cache

			if (!toFetch.empty()) {
				fetchFuture = self->loadValueFunc(data, toFetch, &fetchedKVMap);
			}

			// wait for pending key fetches
			wait(waitForAll(pendingFetches));

			for (auto p : pendingFetches) {
				if (p.get().isValid) {
					rMap[p.get().key] = p.get().value;
				} else {
					// ignore failures due to invalid-key accesses; caller is responsible to handle misses keys scenario
				}
			}

			if (fetchFuture.isValid()) {
				wait(fetchFuture);

				ASSERT_LE(fetchedKVMap.size(), toFetch.size());

				// Partial key fetch: invalid key could fail the lookup, cleanup the cache with corresponging CacheValue
				// promise as it can NEVER to fulfiled. Approach constructs 'unordered_set' to filter failed lookups,
				// given the uncommon path, approach avoids penalty of doing necessary book-keeping in the more common
				// fast path (successful lookup for all keys)

				if (fetchedKVMap.size() != toFetch.size()) {
					KeySet s(toFetch.begin(), toFetch.end());
					for (const auto e : fetchedKVMap) {
						s.erase(e.first);
					}

					ASSERT_GT(s.size(), 0);

					for (const auto& k : s) {
						// notify actors waiting for fetch completion of 'invalid key'; populate cache with an
						// 'invalid' and an 'expired' key.

						auto it = self->cache.find(k);
						ASSERT(it != self->cache.end());
						ASSERT(it->second.canBeSet());
						it->second.send(CacheEntry());
						self->cache.erase(it);
						if (DEBUG_LOADABLE_CACHE) {
							TraceEvent("LoadableCacheInvalidKey").detail("Key", k);
						}
					}
				}

				for (const auto e : fetchedKVMap) {
					if (DEBUG_LOADABLE_CACHE) {
						TraceEvent("LoadableCacheInsertKey")
						    .detail("Key", e.first)
						    .detail("Value", e.second)
						    .detail("CanSet", self->cache[e.first].canBeSet())
						    .detail("ExpAt", expDurationSec);
					}

					if (self->checkValueValidFunc != nullptr) {
						ASSERT(!expDurationSec.present());
						self->cache[e.first].send(CacheEntry(e.first, e.second));
					} else {
						ASSERT(expDurationSec.present() && expDurationSec.get() > 0);
						self->cache[e.first].send(CacheEntry(e.first, e.second, expDurationSec.get()));
					}

					rMap[e.first] = e.second;
				}
			}

			LoadableCacheOpStats::updateFetched(stats, fetchedKVMap.size());
			LoadableCacheOpStats::updateDedups(stats, pendingFetches.size());

			CODE_PROBE(!pendingFetches.empty(), "LoadableCache pending load dedups");
			CODE_PROBE(!fetchedKVMap.empty(), "LoadableCache fetch unresolved keys");
			CODE_PROBE(rMap.size() != keys.size(), "LoadableCache partial key lookup");

			return rMap;
		} catch (Error& e) {
			// Propagate key-fetch error to actors waiting for fetch completion, in other words, all the requests
			// would obtain the same error.

			for (const auto key : toFetch) {
				auto it = self->cache.find(key);
				if (it != self->cache.end()) {
					if (DEBUG_LOADABLE_CACHE) {
						TraceEvent("LoadableCacheError")
						    .error(e)
						    .detail("Key", key)
						    .detail("IsSet", self->cache[key].isSet());
					}
					it->second.sendError(e);
					self->cache.erase(it);
				}
			}

			CODE_PROBE(true, "LoadableCache error loading key(s)");
			throw;
		}
	}
};

#endif
