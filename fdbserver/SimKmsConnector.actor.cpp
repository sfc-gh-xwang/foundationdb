/*
 * SimEncryptKmsProxy.actor.cpp
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

#include "fdbclient/BlobCipher.h"
#include "fdbclient/BlobMetadataUtils.h"
#include "fdbclient/SimKmsVault.h"

#include "fdbrpc/sim_validation.h"
#include "fdbrpc/simulator.h"

#include "fdbserver/KmsConnector.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/SimKmsConnector.h"

#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/CodeProbe.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/ITrace.h"
#include "flow/Knobs.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/UnitTest.h"

#include "fmt/format.h"

#include <cstring>
#include <limits>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "flow/actorcompiler.h" // This must be the last #include.

#define DEBUG_SIM_KEY_CIPHER DEBUG_ENCRYPT_KEY_CIPHER

namespace {
Optional<int64_t> getRefreshInterval(const int64_t now, const int64_t defaultTtl) {
	if (BUGGIFY) {
		return Optional<int64_t>(now);
	}
	return Optional<int64_t>(now + defaultTtl);
}

Optional<int64_t> getExpireInterval(Optional<int64_t> refTS, const int64_t defaultTtl) {
	ASSERT(refTS.present());

	if (BUGGIFY) {
		return Optional<int64_t>(-1);
	}
	return (refTS.get() + defaultTtl);
}
} // namespace

ACTOR Future<Standalone<VectorRef<KmsCipherKeyDetailsRef>>> ekLookupByIds(
    std::vector<KmsConnLookupKeyIdsInfo> encryptKeyInfos,
    Optional<UID> debugId) {
	state Standalone<VectorRef<KmsCipherKeyDetailsRef>> cipherKeyDetails;
	state Optional<TraceEvent> dbgKIdTrace =
	    debugId.present() ? TraceEvent("SimKmsGetByKeyIds", debugId.get()) : Optional<TraceEvent>();

	if (dbgKIdTrace.present()) {
		dbgKIdTrace.get().setMaxEventLength(100000);
		dbgKIdTrace.get().detail("DbgId", debugId.get());
	}

	// Lookup corresponding EncryptKeyCtx for input keyId
	const int64_t currTS = (int64_t)now();
	// Fetch default TTL to avoid BUGGIFY giving different value per invocation causing refTS > expTS
	const int64_t defaultTtl = FLOW_KNOBS->ENCRYPT_CIPHER_KEY_CACHE_TTL;
	Optional<int64_t> refAtTS = getRefreshInterval(currTS, defaultTtl);
	Optional<int64_t> expAtTS = getExpireInterval(refAtTS, defaultTtl);
	TraceEvent("SimKmsEKLookupById").detail("RefreshAt", refAtTS).detail("ExpireAt", expAtTS);
	for (const auto& item : encryptKeyInfos) {
		Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByBaseCipherId(item.baseCipherId);
		if (keyCtx.isValid()) {
			// TODO: Relax assert if EKP APIs are updated to make 'domain_id' optional for encryption keys point lookups
			ASSERT(item.domainId.present());
			cipherKeyDetails.emplace_back_deep(
			    cipherKeyDetails.arena(), item.domainId.get(), keyCtx->id, keyCtx->key, keyCtx->kcv, refAtTS, expAtTS);

			if (dbgKIdTrace.present()) {
				// {encryptDomainId, baseCipherId} forms a unique tuple across encryption domains
				dbgKIdTrace.get().detail(
				    getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_RESULT_PREFIX, item.domainId.get(), keyCtx->id), "");
			}

			if (DEBUG_SIM_KEY_CIPHER) {
				TraceEvent("SimKmsEKLookupByKeyId")
				    .detail("DomId", item.domainId.get())
				    .detail("BaseCipherId", item.baseCipherId)
				    .detail("BaseCipherKeyLen", keyCtx->keyLen)
				    .detail("BaseCipherKCV", keyCtx->kcv);
			}
		} else {
			TraceEvent("SimKmsEKLookupByIdsKeyNotFound")
			    .detail("DomId", item.domainId)
			    .detail("BaseCipherId", item.baseCipherId);
			throw encrypt_key_not_found();
		}
	}

	wait(delayJittered(1.0)); // simulate network delay
	return cipherKeyDetails;
}

Future<Standalone<VectorRef<KmsCipherKeyDetailsRef>>> SimKmsConnector::fetchEncryptionKeysByKeyIds(
    std::any data,
    const std::vector<KmsConnLookupKeyIdsInfo>& encryptKeyInfos,
    Optional<UID> debugId) {
	ASSERT(!data.has_value());
	return ekLookupByIds(encryptKeyInfos, debugId);
}

ACTOR Future<Standalone<VectorRef<KmsCipherKeyDetailsRef>>> ekLookupByDomainIds(
    std::vector<EncryptCipherDomainId> domainIds,
    Optional<UID> debugId) {
	state Standalone<VectorRef<KmsCipherKeyDetailsRef>> cipherKeyDetails;
	state Optional<TraceEvent> dbgDIdTrace =
	    debugId.present() ? TraceEvent("SimKmsGetsByDomIds", debugId.get()) : Optional<TraceEvent>();

	if (dbgDIdTrace.present()) {
		dbgDIdTrace.get().setMaxEventLength(16384).detail("DbgId", debugId.get());
	}

	// Map encryptionDomainId to corresponding EncryptKeyCtx element using a modulo operation. This
	// would mean multiple domains gets mapped to the same encryption key which is fine, the
	// EncryptKeyStore guarantees that keyId -> plaintext encryptKey mapping is idempotent.
	const int64_t currTS = (int64_t)now();
	// Fetch default TTL to avoid BUGGIFY giving different value per invocation causing refTS > expTS
	const int64_t defaultTtl = FLOW_KNOBS->ENCRYPT_CIPHER_KEY_CACHE_TTL;
	Optional<int64_t> refAtTS = getRefreshInterval(currTS, defaultTtl);
	Optional<int64_t> expAtTS = getExpireInterval(refAtTS, defaultTtl);
	TraceEvent("SimKmsEKLookupByDomainId").detail("RefreshAt", refAtTS).detail("ExpireAt", expAtTS);
	for (const auto domainId : domainIds) {
		// Ensure domainIds are acceptable
		if (domainId < FDB_DEFAULT_ENCRYPT_DOMAIN_ID) {
			break;
		}

		Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByDomainId(domainId);
		if (keyCtx.isValid()) {
			cipherKeyDetails.emplace_back_deep(
			    cipherKeyDetails.arena(), domainId, keyCtx->id, keyCtx->key, keyCtx->kcv, refAtTS, expAtTS);
			if (dbgDIdTrace.present()) {
				// {encryptId, baseCipherId} forms a unique tuple across encryption domains
				dbgDIdTrace.get().detail(getEncryptDbgTraceKey(ENCRYPT_DBG_TRACE_RESULT_PREFIX, domainId, keyCtx->id),
				                         "");
			}
			if (DEBUG_SIM_KEY_CIPHER) {
				TraceEvent("SimKmsEKLookupByDomainId")
				    .detail("DomId", domainId)
				    .detail("BaseCipherId", keyCtx->id)
				    .detail("BaseCipherKeyLen", keyCtx->keyLen)
				    .detail("BaseCipherKCV", keyCtx->kcv);
			}
		} else {
			TraceEvent("SimKmsEKLookupByDomainIdKeyNotFound").detail("DomId", domainId);
			throw encrypt_key_not_found();
		}
	}

	wait(delayJittered(1.0)); // simulate network delay
	return cipherKeyDetails;
}

Future<Standalone<VectorRef<KmsCipherKeyDetailsRef>>> SimKmsConnector::fetchEncryptionKeysByDomainIds(
    std::any data,
    const std::vector<EncryptCipherDomainId>& domainIds,
    Optional<UID> debugId) {
	ASSERT(!data.has_value());
	return ekLookupByDomainIds(domainIds, debugId);
}

ACTOR Future<Standalone<VectorRef<BlobMetadataDetailsRef>>> blobMetadataLookup(
    std::vector<EncryptCipherDomainId> domainIds,
    Optional<UID> debugId) {
	state Standalone<VectorRef<BlobMetadataDetailsRef>> metadataDetails;
	state Optional<TraceEvent> dbgDIdTrace =
	    debugId.present() ? TraceEvent("SimKmsBlobMetadataLookup", debugId.get()) : Optional<TraceEvent>();
	if (dbgDIdTrace.present()) {
		dbgDIdTrace.get().detail("DbgId", debugId.get());
	}

	for (auto const domainId : domainIds) {
		Standalone<BlobMetadataDetailsRef> metadataRef = SimKmsVault::getBlobMetadata(domainId, SERVER_KNOBS->BG_URL);
		metadataDetails.arena().dependsOn(metadataRef.arena());
		metadataDetails.push_back(metadataDetails.arena(), metadataRef);
	}

	wait(delay(deterministicRandom()->random01())); // simulate network delay

	// buggify omitted tenants, errors, or unexpectedly ordered locations in response
	if (g_network->isSimulated() && !g_simulator->speedUpSimulation && BUGGIFY_WITH_PROB(0.01)) {
		int bug = deterministicRandom()->randomInt(0, 3);
		if (bug == 0) {
			// remove some number of tenants from the response
			int targetSize = deterministicRandom()->randomInt(0, metadataDetails.size());
			while (metadataDetails.size() > targetSize) {
				swapAndPop(&metadataDetails, deterministicRandom()->randomInt(0, metadataDetails.size()));
			}
		} else if (bug == 1) {
			// send error
			throw connection_failed();
		} else if (bug == 2) {
			int targetDetail = deterministicRandom()->randomInt(0, metadataDetails.size());
			auto& locs = metadataDetails[targetDetail].locations;
			if (locs.size() > 1) {
				int targetIdx1 = deterministicRandom()->randomInt(0, locs.size());
				int targetIdx2 = targetIdx1;
				while (targetIdx2 == targetIdx1) {
					targetIdx2 = deterministicRandom()->randomInt(0, locs.size());
				}
				std::swap(locs[targetIdx1], locs[targetIdx2]);
			}
		} else {
			// developer forgot to update cases
			ASSERT(false);
		}
	}

	return metadataDetails;
}

Future<Standalone<VectorRef<BlobMetadataDetailsRef>>> SimKmsConnector::fetchBlobMetadata(
    std::any data,
    const std::vector<EncryptCipherDomainId>& domainIds,
    Optional<UID> debugId) {
	ASSERT(!data.has_value());
	return blobMetadataLookup(domainIds, debugId);
}

void forceLinkSimKmsConnectorTests() {}

namespace {

ACTOR Future<Void> testRunWorkload(KmsConnector* connInf) {
	state uint32_t maxEncryptionKeys = SimKmsVault::maxSimKeys();
	state int maxDomainIds = deterministicRandom()->randomInt(121, 295);
	state int maxIterations = deterministicRandom()->randomInt(5, 15);
	state Optional<UID> debugId =
	    deterministicRandom()->coinflip() ? deterministicRandom()->randomUniqueID() : Optional<UID>();
	state int i;
	state std::vector<KmsConnLookupKeyIdsInfo> encryptKeyInfos;

	TraceEvent("RunWorkloadStart").detail("MaxDomainIds", maxDomainIds).detail("MaxIterations", maxIterations);

	// construct {encryptId -> baseCipherId} mapping
	state std::unordered_map<EncryptCipherDomainId, EncryptCipherBaseKeyId> domainIdMap;
	state std::vector<EncryptCipherDomainId> domainIds;
	state std::vector<EncryptCipherDomainId> domainIdsReq;
	for (i = 0; i < maxDomainIds; i++) {
		domainIds.emplace_back(i);
	}
	state Standalone<VectorRef<KmsCipherKeyDetailsRef>> cipherKeyDetails =
	    wait(connInf->fetchEncryptionKeysByDomainIds(nullptr, domainIdsReq));
	for (auto& element : cipherKeyDetails) {
		Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByDomainId(element.encryptDomainId);
		ASSERT(keyCtx.isValid());
		ASSERT_EQ(element.encryptKeyId, keyCtx->id);
		ASSERT_EQ(element.encryptKey.compare(keyCtx->key), 0);
		ASSERT_EQ(element.encryptKCV, keyCtx->kcv);
		domainIdMap[element.encryptDomainId] = keyCtx->id;
		domainIds.push_back(element.encryptDomainId);
	}

	for (i = 0; i < maxIterations; i++) {
		const int batchSize = deterministicRandom()->randomInt(1, maxEncryptionKeys);
		int domIdx = deterministicRandom()->randomInt(0, domainIds.size() - 1);
		if (deterministicRandom()->coinflip()) {
			state std::vector<EncryptCipherDomainId> domIds;
			for (int j = 0; j < batchSize && domIdx < domainIds.size(); j++, domIdx++) {
				domIds.emplace_back(domIdx);
			}
			Standalone<VectorRef<KmsCipherKeyDetailsRef>> cipherKeyDetails =
			    wait(connInf->fetchEncryptionKeysByDomainIds(nullptr, domIds, debugId));
			for (auto& element : cipherKeyDetails) {
				Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByDomainId(element.encryptDomainId);
				ASSERT(keyCtx.isValid());
				ASSERT_EQ(element.encryptKeyId, keyCtx->id);
				ASSERT_EQ(element.encryptKey.compare(keyCtx->key), 0);
				ASSERT_EQ(element.encryptKCV, keyCtx->kcv);
			}
		} else {
			encryptKeyInfos.clear();
			for (int j = 0; j < batchSize && domIdx < domainIds.size(); j++, domIdx++) {
				auto itr = domainIdMap.find(domIdx);
				ASSERT(itr != domainIdMap.end());
				encryptKeyInfos.emplace_back(domIdx, itr->second);
			}
			Standalone<VectorRef<KmsCipherKeyDetailsRef>> cipherKeyDetails =
			    wait(connInf->fetchEncryptionKeysByKeyIds(nullptr, encryptKeyInfos, debugId));
			for (auto& element : cipherKeyDetails) {
				Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByBaseCipherId(element.encryptKeyId);
				ASSERT(keyCtx.isValid());
				ASSERT_EQ(element.encryptKeyId, keyCtx->id);
				ASSERT_EQ(element.encryptKey.compare(keyCtx->key), 0);
				ASSERT_EQ(element.encryptKCV, keyCtx->kcv);
			}
		}
	}

	{
		// Verify unknown key access returns the error
		encryptKeyInfos.clear();
		encryptKeyInfos.emplace_back(1, maxEncryptionKeys + 1);
		try {
			Standalone<VectorRef<KmsCipherKeyDetailsRef>> cipherKeyDetails =
			    wait(ekLookupByIds(encryptKeyInfos, Optional<UID>()));
		} catch (Error& e) {
			ASSERT(e.code() == error_code_encrypt_key_not_found);
		}
	}

	TraceEvent("RunWorkloadDone").log();
	return Void();
}

} // namespace

TEST_CASE("fdbserver/SimKmsConnector") {
	std::unique_ptr<KmsConnector> connInf = std::make_unique<SimKmsConnector>("SimKmsConnector");
	wait(testRunWorkload(connInf.get()));
	return Void();
}
