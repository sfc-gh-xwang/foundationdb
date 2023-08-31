/*
 * BlobCipher.cpp
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

#include "fdbclient/BlobCipher.h"
#include "fdbclient/EncryptBlobCipher.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"

#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#include "flow/FileIdentifier.h"
#include "flow/FastRef.h"
#include "flow/IndexedSet.h"
#include "flow/flow.h"
#include "flow/Error.h"
#include "flow/Knobs.h"
#include "flow/IRandom.h"
#include "flow/ITrace.h"
#include "flow/ObjectSerializer.h"
#include "flow/Platform.h"
#include "flow/ProtocolVersion.h"
#include "flow/network.h"
#include "flow/serialize.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>

#include <unistd.h>

#define BLOB_CIPHER_DEBUG DEBUG_ENCRYPT_KEY_CIPHER

namespace {
void validateEncryptHeaderFlagVersion(const int flagsVersion) {
	if (flagsVersion > CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION) {
		TraceEvent("EncryptHeaderUnsupportedFlagVersion")
		    .detail("MaxSupportedVersion", CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION)
		    .detail("Version", flagsVersion);
		throw not_implemented();
	}
}

void validateEncryptHeaderAlgoHeaderVersion(const EncryptCipherMode cipherMode,
                                            const EncryptAuthTokenMode authMode,
                                            const EncryptAuthTokenAlgo authAlgo,
                                            const int version) {
	if (!isCipherModeSupported(cipherMode)) {
		TraceEvent("EncryptHeaderUnsupportedEncryptCipherMode")
		    .detail("MaxSupportedVersion", CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION)
		    .detail("CipherMode", cipherMode);
		throw not_implemented();
	}

	int maxSupportedVersion = -1;
	if (authMode == ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		maxSupportedVersion = CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_NO_AUTH_VERSION;
	} else {
		ASSERT_EQ(authMode, ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);

		if (authAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA) {
			maxSupportedVersion = CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_HMAC_SHA_AUTH_VERSION;
		} else if (authAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC) {
			maxSupportedVersion = CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_AES_CMAC_AUTH_VERSION;
		} else if (authAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM) {
			maxSupportedVersion = CLIENT_KNOBS->ENCRYPT_HEADER_AES_GCM_AUTH_VERSION;
		} else {
			// Unknown encryption authentication algo
		}
	}

	if (version > maxSupportedVersion || maxSupportedVersion == -1) {
		TraceEvent("EncryptHeaderUnsupportedEncryptAuthToken")
		    .detail("CipherMode", cipherMode)
		    .detail("AuthMode", authMode)
		    .detail("AuthAlgo", authAlgo)
		    .detail("AlgoHeaderVersion", version)
		    .detail("MaxSsupportedVersion", maxSupportedVersion);
		throw not_implemented();
	}
}
} // namespace

// BlobCipherEncryptHeaderRef

uint32_t BlobCipherEncryptHeaderRef::getHeaderSize(const int flagVersion,
                                                   const int algoVersion,
                                                   const EncryptCipherMode cipherMode,
                                                   const EncryptAuthTokenMode authMode,
                                                   const EncryptAuthTokenAlgo authAlgo) {
	if (flagVersion != 1) {
		TraceEvent("BlobCipherGetHeaderSizeInvalidFlagVersion").detail("FlagVersion", flagVersion);
		throw not_implemented();
	}
	if (algoVersion != 1) {
		TraceEvent("BlobCipherGetHeaderSizeInvalidAlgoVersion").detail("AlgoVersion", algoVersion);
		throw not_implemented();
	}

	uint32_t total = sizeof(BlobCipherEncryptHeaderFlagsV1) + 2; // 2 bytes of std::variant index

	if (!isCipherModeSupported(cipherMode)) {
		throw not_implemented();
	}

	if (authMode == ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		total += AesCtrNoAuth::getSize();
	} else {
		if (authAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA) {
			total += AesCtrWithHmac::getSize();
		} else if (authAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM) {
			total += AesGcm::getSize();
		} else {
			ASSERT_EQ(authAlgo, ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC);
			total += AesCtrWithCmac::getSize();
		}
	}
	return total;
}

const uint8_t* BlobCipherEncryptHeaderRef::getIV() const {
	validateEncryptHeaderFlagVersion(flagsVersion());
	ASSERT_EQ(flagsVersion(), 1);

	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(this->flags);

	validateEncryptHeaderAlgoHeaderVersion((EncryptCipherMode)flags.encryptMode,
	                                       (EncryptAuthTokenMode)flags.authTokenMode,
	                                       (EncryptAuthTokenAlgo)flags.authTokenAlgo,
	                                       algoHeaderVersion());
	ASSERT_EQ(algoHeaderVersion(), 1);

	return std::visit(
	    [](auto& h) {
		    using T = std::decay_t<decltype(h)>;
		    if constexpr (std::is_same_v<T, AesGcm>) {
			    return get<AesGcmV1>(h.headerVersions).iv;
		    } else {
			    return h.v1.iv;
		    }
	    },
	    algoHeader);
}

int BlobCipherEncryptHeaderRef::getIVSize() const {
	EncryptCipherMode encryptMode = getEncryptMode();
	if (encryptMode == EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR) {
		return AES_256_CTR_IV_LENGTH;
	} else if (encryptMode == EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM) {
		return AES_256_GCM_IV_LENGTH;
	}
	throw not_implemented();
}

template <class>
inline constexpr bool always_false_v = false;

const EncryptHeaderCipherDetails BlobCipherEncryptHeaderRef::getCipherDetails() const {
	validateEncryptHeaderFlagVersion(flagsVersion());
	ASSERT_EQ(flagsVersion(), 1);

	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(this->flags);

	validateEncryptHeaderAlgoHeaderVersion((EncryptCipherMode)flags.encryptMode,
	                                       (EncryptAuthTokenMode)flags.authTokenMode,
	                                       (EncryptAuthTokenAlgo)flags.authTokenAlgo,
	                                       algoHeaderVersion());
	ASSERT_EQ(algoHeaderVersion(), 1);

	// TODO: Replace with "Overload visitor pattern" someday.
	return std::visit(
	    [](auto&& h) {
		    using T = std::decay_t<decltype(h)>;
		    if constexpr (std::is_same_v<T, AesCtrNoAuth>) {
			    return EncryptHeaderCipherDetails(h.v1.cipherTextDetails);
		    } else if constexpr (std::is_same_v<T, AesGcm>) {
			    return EncryptHeaderCipherDetails(get<AesGcmV1>(h.headerVersions).cipherTextDetails);
		    } else if constexpr (std::is_same_v<T, AesCtrWithHmac> || std::is_same_v<T, AesCtrWithCmac>) {
			    return EncryptHeaderCipherDetails(h.v1.cipherTextDetails, h.v1.cipherHeaderDetails);
		    } else {
			    static_assert(always_false_v<T>, "Unknown encryption authentication");
		    }
	    },
	    algoHeader);
}

EncryptAuthTokenMode BlobCipherEncryptHeaderRef::getAuthTokenMode() const {
	// TODO: Replace with "Overload visitor pattern" someday.
	return std::visit(
	    [](auto&& f) {
		    using T = std::decay_t<decltype(f)>;
		    if constexpr (std::is_same_v<T, BlobCipherEncryptHeaderFlagsV1>) {
			    return (EncryptAuthTokenMode)f.authTokenMode;
		    } else {
			    static_assert(always_false_v<T>, "Unknown encryption flag header");
		    }
	    },
	    flags);
}

EncryptCipherMode BlobCipherEncryptHeaderRef::getEncryptMode() const {
	// TODO: Replace with "Overload visitor pattern" someday.
	return std::visit(
	    [](auto&& f) {
		    using T = std::decay_t<decltype(f)>;
		    if constexpr (std::is_same_v<T, BlobCipherEncryptHeaderFlagsV1>) {
			    return (EncryptCipherMode)f.encryptMode;
		    } else {
			    static_assert(always_false_v<T>, "Unknown encryption flag header");
		    }
	    },
	    flags);
}

const uint8_t* BlobCipherEncryptHeaderRef::getAuthToken() const {
	validateEncryptHeaderFlagVersion(flagsVersion());
	ASSERT_EQ(flagsVersion(), 1);

	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(this->flags);

	validateEncryptHeaderAlgoHeaderVersion((EncryptCipherMode)flags.encryptMode,
	                                       (EncryptAuthTokenMode)flags.authTokenMode,
	                                       (EncryptAuthTokenAlgo)flags.authTokenAlgo,
	                                       algoHeaderVersion());
	ASSERT_EQ(algoHeaderVersion(), 1);
	if (std::holds_alternative<AesCtrNoAuth>(algoHeader)) {
		return nullptr;
	} else if (std::holds_alternative<AesCtrWithHmac>(algoHeader)) {
		return get<AesCtrWithHmac>(algoHeader).v1.authToken;
	} else if (std::holds_alternative<AesCtrWithCmac>(algoHeader)) {
		return get<AesCtrWithCmac>(algoHeader).v1.authToken;
	} else {
		ASSERT(std::holds_alternative<AesGcm>(algoHeader));
		return get<AesGcmV1>(get<AesGcm>(algoHeader).headerVersions).authToken;
	}
}

EncryptCipherDomainId BlobCipherEncryptHeaderRef::getDomainId() const {
	return std::visit(
	    [](auto& h) {
		    using T = std::decay_t<decltype(h)>;
		    if constexpr (std::is_same_v<T, AesGcm>) {
			    return get<AesGcmV1>(h.headerVersions).cipherTextDetails.encryptDomainId;
		    } else {
			    return h.v1.cipherTextDetails.encryptDomainId;
		    }
	    },
	    algoHeader);
}

EncryptHeaderCipherKCVs BlobCipherEncryptHeaderRef::getKCVs() const {
	validateEncryptHeaderFlagVersion(flagsVersion());
	ASSERT_EQ(flagsVersion(), 1);

	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(this->flags);

	validateEncryptHeaderAlgoHeaderVersion((EncryptCipherMode)flags.encryptMode,
	                                       (EncryptAuthTokenMode)flags.authTokenMode,
	                                       (EncryptAuthTokenAlgo)flags.authTokenAlgo,
	                                       algoHeaderVersion());
	ASSERT_EQ(algoHeaderVersion(), 1);

	// TODO: Replace with "Overload visitor pattern" someday.
	return std::visit(
	    [](auto&& h) {
		    using T = std::decay_t<decltype(h)>;
		    if constexpr (std::is_same_v<T, AesCtrNoAuth>) {
			    return EncryptHeaderCipherKCVs(h.v1.textKCV);
		    } else if constexpr (std::is_same_v<T, AesGcm>) {
			    return EncryptHeaderCipherKCVs(get<AesGcmV1>(h.headerVersions).textKCV);
		    } else if constexpr (std::is_same_v<T, AesCtrWithHmac> || std::is_same_v<T, AesCtrWithCmac>) {
			    return EncryptHeaderCipherKCVs(h.v1.textKCV, h.v1.headerKCV);
		    } else {
			    static_assert(always_false_v<T>, "Unknown encryption authentication");
		    }
	    },
	    algoHeader);
}

void BlobCipherEncryptHeaderRef::validateEncryptionHeaderDetails(const BlobCipherDetails& textCipherDetails,
                                                                 const BlobCipherDetails& headerCipherDetails,
                                                                 const EncryptHeaderCipherKCVs& kcvs,
                                                                 const StringRef& ivRef) const {
	validateEncryptHeaderFlagVersion(flagsVersion());
	ASSERT_EQ(flagsVersion(), 1);

	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(this->flags);

	validateEncryptHeaderAlgoHeaderVersion((EncryptCipherMode)flags.encryptMode,
	                                       (EncryptAuthTokenMode)flags.authTokenMode,
	                                       (EncryptAuthTokenAlgo)flags.authTokenAlgo,
	                                       algoHeaderVersion());
	ASSERT_EQ(algoHeaderVersion(), 1);

	BlobCipherDetails persistedTextCipherDetails;
	BlobCipherDetails persistedHeaderCipherDetails;
	uint8_t* persistedIV = nullptr;
	EncryptCipherKeyCheckValue persistedTextKCV;
	Optional<EncryptCipherKeyCheckValue> persistedHeaderKCV;

	// TODO: Replace with "Overload visitor pattern" someday.
	return std::visit(
	    [&persistedTextCipherDetails,
	     &persistedHeaderCipherDetails,
	     &persistedIV,
	     &persistedTextKCV,
	     &persistedHeaderKCV](auto&& h) {
		    using T = std::decay_t<decltype(h)>;
		    if constexpr (std::is_same_v<T, AesCtrNoAuth>) {
			    persistedTextCipherDetails = h.v1.cipherTextDetails;
			    persistedIV = (uint8_t*)&h.v1.iv[0];
			    persistedTextKCV = h.v1.textKCV;
		    } else if constexpr (std::is_same_v<T, AesGcm>) {
			    persistedTextCipherDetails = get<AesGcmV1>(h.headerVersions).cipherTextDetails;
			    persistedIV = (uint8_t*)&get<AesGcmV1>(h.headerVersions).iv[0];
			    persistedTextKCV = get<AesGcmV1>(h.headerVersions).textKCV;
		    } else if constexpr (std::is_same_v<T, AesCtrWithHmac> || std::is_same_v<T, AesCtrWithCmac>) {
			    persistedTextCipherDetails = h.v1.cipherTextDetails;
			    persistedHeaderCipherDetails = h.v1.cipherHeaderDetails;
			    persistedIV = (uint8_t*)&h.v1.iv[0];
			    persistedTextKCV = h.v1.textKCV;
			    persistedHeaderKCV = h.v1.headerKCV;
		    } else {
			    static_assert(always_false_v<T>, "Unknown encryption authentication");
		    }
	    },
	    algoHeader);

	// Validate encryption header 'cipherHeader' details sanity
	if (flags.authTokenMode != ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE &&
	    headerCipherDetails != persistedHeaderCipherDetails) {
		TraceEvent(SevError, "ValidateEncryptHeaderMismatch")
		    .detail("HeaderDomainId", headerCipherDetails.encryptDomainId)
		    .detail("PersistedHeaderDomainId", persistedHeaderCipherDetails.encryptDomainId)
		    .detail("HeaderBaseCipherId", headerCipherDetails.baseCipherId)
		    .detail("ExpectedHeaderBaseCipherId", persistedHeaderCipherDetails.baseCipherId)
		    .detail("HeaderSalt", headerCipherDetails.salt)
		    .detail("ExpectedHeaderSalt", persistedHeaderCipherDetails.salt);
		throw encrypt_header_metadata_mismatch();
	}
	// Validate encryption header 'cipherText' details sanity
	if (textCipherDetails != persistedTextCipherDetails) {
		TraceEvent(SevError, "ValidateEncryptHeaderMismatch")
		    .detail("TextDomainId", textCipherDetails.encryptDomainId)
		    .detail("PersistedTextDomainId", persistedTextCipherDetails.encryptDomainId)
		    .detail("TextBaseCipherId", textCipherDetails.baseCipherId)
		    .detail("PersistedTextBaseCipherId", persistedTextCipherDetails.encryptDomainId)
		    .detail("TextSalt", textCipherDetails.salt)
		    .detail("PersistedTextSalt", persistedTextCipherDetails.salt);
		throw encrypt_header_metadata_mismatch();
	}
	// Validate 'Initialization Vector' sanity
	int ivSize = getIVSize();
	EncryptCipherMode encryptMode = getEncryptMode();
	if (encryptMode == EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR && ivSize != AES_256_CTR_IV_LENGTH) {
		TraceEvent(SevError, "EncryptionHeaderIVSizeMismatch")
		    .detail("IVSize", ivSize)
		    .detail("ExpectedIVSize", AES_256_CTR_IV_LENGTH);
		throw encrypt_header_metadata_mismatch();
	} else if (encryptMode == EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM && ivSize != AES_256_GCM_IV_LENGTH) {
		TraceEvent(SevError, "EncryptionHeaderIVSizeMismatch")
		    .detail("IVSize", ivSize)
		    .detail("ExpectedIVSize", AES_256_GCM_IV_LENGTH);
		throw encrypt_header_metadata_mismatch();
	}

	if (memcmp(ivRef.begin(), persistedIV, ivSize) != 0) {
		TraceEvent(SevError, "EncryptionHeaderIVMismatch")
		    .detail("IVChecksum", XXH3_64bits(ivRef.begin(), ivRef.size()))
		    .detail("ExpectedIVChecksum", XXH3_64bits(persistedIV, ivSize));
		throw encrypt_header_metadata_mismatch();
	}

	// Validate baseCipher KCVs
	if (persistedTextKCV != kcvs.textKCV) {
		TraceEvent(SevError, "EncryptionHeadeTextKCVMismatch")
		    .detail("Persisted", persistedTextKCV)
		    .detail("Expected", kcvs.textKCV);
		throw encrypt_key_check_value_mismatch();
	}
	if (persistedHeaderKCV.present()) {
		if (!kcvs.headerKCV.present()) {
			TraceEvent(SevError, "EncryptionHeadeMissingHeaderKCV");
			throw encrypt_key_check_value_mismatch();
		}
		if (persistedHeaderKCV.get() != kcvs.headerKCV.get()) {
			TraceEvent(SevError, "EncryptionHeadeTextKCVMismatch")
			    .detail("Persisted", persistedTextKCV)
			    .detail("Expected", kcvs.textKCV);
			throw encrypt_key_check_value_mismatch();
		}
	}
}

// BlobCipherMetrics methods

const std::unordered_map<int, std::string> BlobCipherMetrics::usageTypeNames = {
	{ BlobCipherMetrics::UsageType::ALL, "" },
	{ BlobCipherMetrics::UsageType::TLOG, "TLog" },
	{ BlobCipherMetrics::UsageType::TLOG_POST_RESOLUTION, "TLogPostResolution" },
	{ BlobCipherMetrics::UsageType::KV_MEMORY, "KVMemory" },
	{ BlobCipherMetrics::UsageType::KV_REDWOOD, "KVRedwood" },
	{ BlobCipherMetrics::UsageType::BLOB_GRANULE, "BlobGranule" },
	{ BlobCipherMetrics::UsageType::BACKUP, "Backup" },
	{ BlobCipherMetrics::UsageType::RESTORE, "Restore" },
	{ BlobCipherMetrics::UsageType::TEST, "Test" },
};

BlobCipherMetrics::BlobCipherMetrics()
  : cc("BlobCipher"),
    cipherKeyCacheHit("CipherKeyCacheHit", cc),
    cipherKeyCacheMiss("CipherKeyCacheMiss", cc),
    cipherKeyCacheExpired("CipherKeyCacheExpired", cc),
    latestCipherKeyCacheHit("LatestCipherKeyCacheHit", cc),
    latestCipherKeyCacheMiss("LatestCipherKeyCacheMiss", cc),
    latestCipherKeyCacheNeedsRefresh("LatestCipherKeyCacheNeedsRefresh", cc),
    blobMetadataCacheHit("BlobMetadataCacheHit", cc),
    blobMetadataCacheMiss("BlobMetadataCacheMiss", cc),
    blobMetadataNeedsRefresh("BlobMetadataNeedsRefresh", cc),
    getBlobMetadataLatency("GetBlobMetadataLatency",
                           UID(),
                           FLOW_KNOBS->ENCRYPT_KEY_CACHE_LATENCY_LOGGING_INTERVAL,
                           FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_SKETCH_ACCURACY),
    getCipherKeysLatency("GetCipherKeysLatency",
                         UID(),
                         FLOW_KNOBS->ENCRYPT_KEY_CACHE_LATENCY_LOGGING_INTERVAL,
                         FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_SKETCH_ACCURACY,
                         true),
    getLatestCipherKeysLatency("GetLatestCipherKeysLatency",
                               UID(),
                               FLOW_KNOBS->ENCRYPT_KEY_CACHE_LATENCY_LOGGING_INTERVAL,
                               FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_SKETCH_ACCURACY,
                               true),
    outstandingCipherKeyRequests(0),
    outstandingLatestCipherKeysRequests(0),
    outstandingBlobMetadataRequests(0) {
	specialCounter(cc, "CacheSize", []() { return BlobCipherKeyCache::getInstance()->getSize(); });

	specialCounter(
	    cc, "OutstandingCipherKeyRequests", [this]() { return this->outstandingCipherKeyRequests.getValue(); });
	specialCounter(cc, "OutstandingLatestCipherKeysRequests", [this]() {
		return this->outstandingLatestCipherKeysRequests.getValue();
	});
	specialCounter(
	    cc, "OutstandingBlobMetadataRequests", [this]() { return this->outstandingBlobMetadataRequests.getValue(); });

	traceFuture = cc.traceCounters("BlobCipherMetrics", UID(), FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_INTERVAL);
}

std::string toString(BlobCipherMetrics::UsageType type) {
	switch (type) {
	case BlobCipherMetrics::UsageType::TLOG:
		return "TLog";
	case BlobCipherMetrics::UsageType::TLOG_POST_RESOLUTION:
		return "TLogPostResolution";
	case BlobCipherMetrics::UsageType::KV_MEMORY:
		return "KVMemory";
	case BlobCipherMetrics::UsageType::KV_REDWOOD:
		return "KVRedwood";
	case BlobCipherMetrics::UsageType::BLOB_GRANULE:
		return "BlobGranule";
	case BlobCipherMetrics::UsageType::BACKUP:
		return "Backup";
	case BlobCipherMetrics::UsageType::RESTORE:
		return "Restore";
	case BlobCipherMetrics::UsageType::TEST:
		return "Test";
	default:
		ASSERT(false);
		return "";
	}
}

// BlobCipherKey class methods

BlobCipherKey::BlobCipherKey(const EncryptCipherDomainId& domainId,
                             const EncryptCipherBaseKeyId& baseCiphId,
                             const uint8_t* baseCiph,
                             const int baseCiphLen,
                             const EncryptCipherKeyCheckValue baseCiphKCV,
                             const int64_t refreshAt,
                             const int64_t expireAt) {
	// Salt generated is used while applying HMAC Key derivation, hence, not using crypto-secure hash algorithm is
	// ok. Further, 'deterministic' salt generation is used to preserve simulation determinism properties.
	EncryptCipherRandomSalt salt;
	if (g_network->isSimulated()) {
		salt = deterministicRandom()->randomUInt64();
	} else {
		salt = nondeterministicRandom()->randomUInt64();
	}

	// Support two type of CipherKeys: 'revocable' & 'non-revocable' ciphers.
	// In all cases, either cipherKey never expires i.e. refreshAt == infinite, or, refreshAt needs <= expireAt
	// timestamp.
	ASSERT(refreshAt == std::numeric_limits<int64_t>::max() || (refreshAt <= expireAt));

	initKey(domainId, baseCiphId, baseCiph, baseCiphLen, baseCiphKCV, salt, refreshAt, expireAt);
}

BlobCipherKey::BlobCipherKey(const EncryptCipherDomainId& domainId,
                             const EncryptCipherBaseKeyId& baseCiphId,
                             const uint8_t* baseCiph,
                             const int baseCiphLen,
                             const EncryptCipherKeyCheckValue baseCiphKCV,
                             const EncryptCipherRandomSalt& salt,
                             const int64_t refreshAt,
                             const int64_t expireAt) {
	initKey(domainId, baseCiphId, baseCiph, baseCiphLen, baseCiphKCV, salt, refreshAt, expireAt);
}

void BlobCipherKey::initKey(const EncryptCipherDomainId& domainId,
                            const EncryptCipherBaseKeyId& baseCiphId,
                            const uint8_t* baseCiph,
                            const int baseCiphLen,
                            const EncryptCipherKeyCheckValue baseCiphKCV,
                            const EncryptCipherRandomSalt& salt,
                            const int64_t refreshAt,
                            const int64_t expireAt) {
	if (baseCiphLen > MAX_BASE_CIPHER_LEN) {
		// HMAC_SHA digest generation accepts upto MAX_BASE_CIPHER_LEN key-buffer, longer keys are truncated and weakens
		// the security guarantees.
		TraceEvent(SevWarnAlways, "MaxBaseCipherKeyLimit")
		    .detail("MaxAllowed", MAX_BASE_CIPHER_LEN)
		    .detail("BaseCipherLen", baseCiphLen);
		CODE_PROBE(true, "Encryption max base cipher len violation");
		throw encrypt_max_base_cipher_len();
	}

	const EncryptCipherKeyCheckValue computedKCV = Sha256KCV().computeKCV(baseCiph, baseCiphLen);
	if (computedKCV != baseCiphKCV) {
		TraceEvent(SevWarnAlways, "BlobCipherKeyInitBaseCipherKCVMismatch")
		    .detail("DomId", domainId)
		    .detail("BaseCipherId", baseCiphId)
		    .detail("Computed", computedKCV)
		    .detail("BaseCipherKCV", baseCipherKCV);
		throw encrypt_ops_error();
	}

	// Set the base encryption key properties
	baseCipher = std::make_unique<uint8_t[]>(baseCiphLen);
	memcpy(baseCipher.get(), baseCiph, baseCiphLen);
	baseCipherLen = baseCiphLen;
	baseCipherKCV = baseCiphKCV;
	baseCipherId = baseCiphId;
	// Set the encryption domain for the base encryption key
	encryptDomainId = domainId;
	randomSalt = salt;
	// derive the encryption key
	cipher = std::make_unique<uint8_t[]>(AES_256_KEY_LENGTH);
	memset(cipher.get(), 0, AES_256_KEY_LENGTH);
	applyHmacSha256Derivation();
	// update cipher 'refresh' and 'expire' TS
	refreshAtTS = refreshAt;
	expireAtTS = expireAt;

#if BLOB_CIPHER_DEBUG
	TraceEvent(SevDebug, "BlobCipherKeyInit")
	    .detail("DomainId", domainId)
	    .detail("BaseCipherId", baseCipherId)
	    .detail("BaseCipherLen", baseCipherLen)
	    .detail("RandomSalt", randomSalt)
	    .detail("RefreshAt", refreshAtTS)
	    .detail("ExpireAtTS", expireAtTS)
	    .detail("BaseCipherKCV", baseCipherKCV);
#endif
}

void BlobCipherKey::applyHmacSha256Derivation() {
	Arena arena;
	uint8_t buf[baseCipherLen + sizeof(EncryptCipherRandomSalt)];
	memcpy(&buf[0], baseCipher.get(), baseCipherLen);
	memcpy(&buf[0] + baseCipherLen, &randomSalt, sizeof(EncryptCipherRandomSalt));
	HmacSha256DigestGen hmacGen(baseCipher.get(), baseCipherLen);
	unsigned int digestLen = hmacGen.digest(
	    { { &buf[0], baseCipherLen + sizeof(EncryptCipherRandomSalt) } }, cipher.get(), AUTH_TOKEN_HMAC_SHA_SIZE);
	if (digestLen < AES_256_KEY_LENGTH) {
		memcpy(cipher.get() + digestLen, buf, AES_256_KEY_LENGTH - digestLen);
	}
}

void BlobCipherKey::reset() {
	memset(baseCipher.get(), 0, baseCipherLen);
	memset(cipher.get(), 0, AES_256_KEY_LENGTH);
}

// BlobKeyIdCache class methods

BlobCipherKeyIdCache::BlobCipherKeyIdCache(EncryptCipherDomainId dId, size_t* sizeStat)
  : domainId(dId),
    latestBaseCipherKeyId(),
    latestRandomSalt(),
    sizeStat(sizeStat) {
	ASSERT(sizeStat != nullptr);
	TraceEvent(SevInfo, "BlobCipherKeyIdCacheInit").detail("DomainId", domainId);
}

BlobCipherKeyIdCacheKey BlobCipherKeyIdCache::getCacheKey(const EncryptCipherBaseKeyId& baseCipherKeyId,
                                                          const EncryptCipherRandomSalt& salt) {
	if (baseCipherKeyId == INVALID_ENCRYPT_CIPHER_KEY_ID || salt == INVALID_ENCRYPT_RANDOM_SALT) {
		throw encrypt_invalid_id();
	}
	return std::make_pair(baseCipherKeyId, salt);
}

Reference<BlobCipherKey> BlobCipherKeyIdCache::getLatestCipherKey() {
	if (!latestBaseCipherKeyId.present()) {
		return Reference<BlobCipherKey>();
	}

	ASSERT_NE(latestBaseCipherKeyId.get(), INVALID_ENCRYPT_CIPHER_KEY_ID);
	ASSERT(latestRandomSalt.present());
	ASSERT_NE(latestRandomSalt.get(), INVALID_ENCRYPT_RANDOM_SALT);

	Reference<BlobCipherKey> latest = getCipherByBaseCipherId(latestBaseCipherKeyId.get(), latestRandomSalt.get());
	if (!latest.isValid()) {
		// Cipher already 'expired'
		return Reference<BlobCipherKey>();
	}

	ASSERT(!latest->isExpired());
	ASSERT_EQ(latest->getBaseCipherId(), latestBaseCipherKeyId.get());
	ASSERT_EQ(latest->getSalt(), latestRandomSalt.get());

	if (latest->needsRefresh()) {
#if BLOB_CIPHER_DEBUG
		TraceEvent(SevDebug, "BlobCipherGetLatestNeedsRefresh")
		    .detail("DomainId", domainId)
		    .detail("Now", now())
		    .detail("RefreshAt", latest->getRefreshAtTS())
		    .detail("ExpireAt", latest->getExpireAtTS());
#endif
		++BlobCipherMetrics::getInstance()->latestCipherKeyCacheNeedsRefresh;
		latestBaseCipherKeyId.reset();
		latestRandomSalt.reset();
		return Reference<BlobCipherKey>();
	}
	return latest;
}

Reference<BlobCipherKey> BlobCipherKeyIdCache::getCipherByBaseCipherId(const EncryptCipherBaseKeyId& baseCipherKeyId,
                                                                       const EncryptCipherRandomSalt& salt) {
	BlobCipherKeyIdCacheMapCItr itr = keyIdCache.find(getCacheKey(baseCipherKeyId, salt));
	if (itr == keyIdCache.end()) {
		return Reference<BlobCipherKey>();
	}

	if (itr->second->isExpired()) {
#if BLOB_CIPHER_DEBUG
		TraceEvent(SevDebug, "BlobCipherGetCipherExpired")
		    .detail("DomainId", domainId)
		    .detail("BaseCipherId", itr->second->getBaseCipherId())
		    .detail("Now", now())
		    .detail("ExpireAt", itr->second->getExpireAtTS());
#endif
		++BlobCipherMetrics::getInstance()->cipherKeyCacheExpired;
		// remove the expired key from the cache
		keyIdCache.erase(itr);
		return Reference<BlobCipherKey>();
	}
	return itr->second;
}

Reference<BlobCipherKey> BlobCipherKeyIdCache::insertBaseCipherKey(const EncryptCipherBaseKeyId& baseCipherId,
                                                                   const uint8_t* baseCipher,
                                                                   const int baseCipherLen,
                                                                   const EncryptCipherKeyCheckValue baseCipherKCV,
                                                                   const int64_t refreshAt,
                                                                   const int64_t expireAt) {
	ASSERT_GT(baseCipherId, INVALID_ENCRYPT_CIPHER_KEY_ID);
	ASSERT_GT(baseCipherLen, 0);

	// BaseCipherKeys are immutable, given the routine invocation updates 'latestCipher',
	// ensure no key-tampering is done
	Reference<BlobCipherKey> latestCipherKey = getLatestCipherKey();
	if (latestCipherKey.isValid() && latestCipherKey->getBaseCipherId() == baseCipherId) {
		if (memcmp(latestCipherKey->rawBaseCipher(), baseCipher, baseCipherLen) == 0) {
#if BLOB_CIPHER_DEBUG
			TraceEvent(SevDebug, "InsertBaseCipherKeyAlreadyPresent")
			    .detail("BaseCipherKeyId", baseCipherId)
			    .detail("DomainId", domainId)
			    .detail("BaseCipherKCV", baseCipherKCV);
#endif

			// Key is already present; nothing more to do.
			return latestCipherKey;
		} else {
			TraceEvent(SevInfo, "BlobCipherUpdatetBaseCipherKey")
			    .detail("BaseCipherKeyId", baseCipherId)
			    .detail("DomainId", domainId);
			throw encrypt_update_cipher();
		}
	}

	// Logging only tracks newly inserted cipher in the cache
	// Approach limits the logging to two instances when new cipher gets added to the cache, two
	// possible scenarios could be:
	// 1. Cold start - cache getting warmed up
	// 2. New cipher - new Tenant and/or KMS driven key-rotation
	// Frequency of the log is governed by KMS driven `refreshAt` interval which is usually a long duration (days if
	// not months)
	TraceEvent(SevInfo, "BlobCipherKeyInsertBaseCipherKeyLatest")
	    .detail("DomainId", domainId)
	    .detail("BaseCipherId", baseCipherId)
	    .detail("BaseCipherLen", baseCipherLen)
	    .detail("BaseCipherKCV", baseCipherKCV)
	    .detail("RefreshAt", refreshAt)
	    .detail("ExpireAt", expireAt);

	Reference<BlobCipherKey> cipherKey = makeReference<BlobCipherKey>(
	    domainId, baseCipherId, baseCipher, baseCipherLen, baseCipherKCV, refreshAt, expireAt);
	BlobCipherKeyIdCacheKey cacheKey = getCacheKey(cipherKey->getBaseCipherId(), cipherKey->getSalt());
	auto result = keyIdCache.emplace(cacheKey, cipherKey);
	ASSERT(result.second);

	// Update the latest BaseCipherKeyId for the given encryption domain
	latestBaseCipherKeyId = baseCipherId;
	latestRandomSalt = cipherKey->getSalt();

	(*sizeStat)++;
	return cipherKey;
}

Reference<BlobCipherKey> BlobCipherKeyIdCache::insertBaseCipherKey(const EncryptCipherBaseKeyId& baseCipherId,
                                                                   const uint8_t* baseCipher,
                                                                   const int baseCipherLen,
                                                                   const EncryptCipherKeyCheckValue baseCipherKCV,
                                                                   const EncryptCipherRandomSalt& salt,
                                                                   const int64_t refreshAt,
                                                                   const int64_t expireAt) {
	ASSERT_NE(baseCipherId, INVALID_ENCRYPT_CIPHER_KEY_ID);
	ASSERT_NE(salt, INVALID_ENCRYPT_RANDOM_SALT);
	ASSERT_GT(baseCipherLen, 0);

	BlobCipherKeyIdCacheKey cacheKey = getCacheKey(baseCipherId, salt);

	// BaseCipherKeys are immutable, ensure that cached value doesn't get updated.
	BlobCipherKeyIdCacheMapCItr itr = keyIdCache.find(cacheKey);
	if (itr != keyIdCache.end()) {
		if (memcmp(itr->second->rawBaseCipher(), baseCipher, baseCipherLen) == 0) {
#if BLOB_CIPHER_DEBUG
			TraceEvent(SevDebug, "InsertBaseCipherKeyAlreadyPresent")
			    .detail("BaseCipherKeyId", baseCipherId)
			    .detail("DomainId", domainId)
			    .detail("BaseCipherKCV", baseCipherKCV);
#endif

			// Key is already present; nothing more to do.
			return itr->second;
		} else {
			TraceEvent(SevInfo, "BlobCipherUpdateBaseCipherKey")
			    .detail("BaseCipherKeyId", baseCipherId)
			    .detail("DomainId", domainId);
			throw encrypt_update_cipher();
		}
	}

	// Logging only tracks newly inserted cipher in the cache
	// possible scenarios could be:
	// 1. Cold start - cache getting warmed up
	// 2. New cipher - new Tenant and/or KMS driven key-rotation
	// Frequency of the log is governed by KMS driven `refreshAt` interval which is usually a long duration (days if
	// not months)
	TraceEvent(SevInfo, "BlobCipherKeyInsertBaseCipherKey")
	    .detail("DomainId", domainId)
	    .detail("BaseCipherId", baseCipherId)
	    .detail("BaseCipherLen", baseCipherLen)
	    .detail("BaseCipherKCV", baseCipherKCV)
	    .detail("Salt", salt)
	    .detail("RefreshAt", refreshAt)
	    .detail("ExpireAt", expireAt);

	Reference<BlobCipherKey> cipherKey = makeReference<BlobCipherKey>(
	    domainId, baseCipherId, baseCipher, baseCipherLen, baseCipherKCV, salt, refreshAt, expireAt);
	auto result = keyIdCache.emplace(cacheKey, cipherKey);
	ASSERT(result.second);

	(*sizeStat)++;
	return cipherKey;
}

void BlobCipherKeyIdCache::cleanup() {
	for (auto& keyItr : keyIdCache) {
		keyItr.second->reset();
	}

	keyIdCache.clear();
}

std::vector<Reference<BlobCipherKey>> BlobCipherKeyIdCache::getAllCipherKeys() {
	std::vector<Reference<BlobCipherKey>> cipherKeys;
	for (auto& keyItr : keyIdCache) {
		cipherKeys.push_back(keyItr.second);
	}
	return cipherKeys;
}

// BlobCipherKeyCache class methods

Reference<BlobCipherKey> BlobCipherKeyCache::insertCipherKey(const EncryptCipherDomainId& domainId,
                                                             const EncryptCipherBaseKeyId& baseCipherId,
                                                             const uint8_t* baseCipher,
                                                             const int baseCipherLen,
                                                             const EncryptCipherKeyCheckValue baseCipherKCV,
                                                             const int64_t refreshAt,
                                                             const int64_t expireAt) {
	if (domainId == INVALID_ENCRYPT_DOMAIN_ID || baseCipherId == INVALID_ENCRYPT_CIPHER_KEY_ID) {
		throw encrypt_invalid_id();
	}

	Reference<BlobCipherKey> cipherKey;

	try {
		auto domainItr = domainCacheMap.find(domainId);
		if (domainItr == domainCacheMap.end()) {
			// Add mapping to track new encryption domain
			Reference<BlobCipherKeyIdCache> keyIdCache = makeReference<BlobCipherKeyIdCache>(domainId, &size);
			cipherKey = keyIdCache->insertBaseCipherKey(
			    baseCipherId, baseCipher, baseCipherLen, baseCipherKCV, refreshAt, expireAt);
			domainCacheMap.emplace(domainId, keyIdCache);
		} else {
			// Track new baseCipher keys
			Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
			cipherKey = keyIdCache->insertBaseCipherKey(
			    baseCipherId, baseCipher, baseCipherLen, baseCipherKCV, refreshAt, expireAt);
		}
	} catch (Error& e) {
		TraceEvent(SevWarn, "BlobCipherInsertCipherKeyFailed")
		    .detail("BaseCipherKeyId", baseCipherId)
		    .detail("DomainId", domainId);
		throw;
	}
	return cipherKey;
}

Reference<BlobCipherKey> BlobCipherKeyCache::insertCipherKey(const EncryptCipherDomainId& domainId,
                                                             const EncryptCipherBaseKeyId& baseCipherId,
                                                             const uint8_t* baseCipher,
                                                             const int baseCipherLen,
                                                             const EncryptCipherKeyCheckValue baseCipherKCV,
                                                             const EncryptCipherRandomSalt& salt,
                                                             const int64_t refreshAt,
                                                             const int64_t expireAt) {
	if (domainId == INVALID_ENCRYPT_DOMAIN_ID || baseCipherId == INVALID_ENCRYPT_CIPHER_KEY_ID ||
	    salt == INVALID_ENCRYPT_RANDOM_SALT) {
		throw encrypt_invalid_id();
	}

	Reference<BlobCipherKey> cipherKey;
	try {
		auto domainItr = domainCacheMap.find(domainId);
		if (domainItr == domainCacheMap.end()) {
			// Add mapping to track new encryption domain
			Reference<BlobCipherKeyIdCache> keyIdCache = makeReference<BlobCipherKeyIdCache>(domainId, &size);
			cipherKey = keyIdCache->insertBaseCipherKey(
			    baseCipherId, baseCipher, baseCipherLen, baseCipherKCV, salt, refreshAt, expireAt);
			domainCacheMap.emplace(domainId, keyIdCache);
		} else {
			// Track new baseCipher keys
			Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
			cipherKey = keyIdCache->insertBaseCipherKey(
			    baseCipherId, baseCipher, baseCipherLen, baseCipherKCV, salt, refreshAt, expireAt);
		}
	} catch (Error& e) {
		TraceEvent(SevWarn, "BlobCipherInsertCipherKeyFailed")
		    .detail("BaseCipherKeyId", baseCipherId)
		    .detail("DomainId", domainId)
		    .detail("Salt", salt);
		throw;
	}
	return cipherKey;
}

Reference<BlobCipherKey> BlobCipherKeyCache::getLatestCipherKey(const EncryptCipherDomainId& domainId) {
	if (domainId == INVALID_ENCRYPT_DOMAIN_ID) {
		TraceEvent(SevWarn, "BlobCipherGetLatestCipherKeyInvalidID").detail("DomainId", domainId);
		throw encrypt_invalid_id();
	}
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		TraceEvent(SevInfo, "BlobCipherGetLatestCipherKeyDomainNotFound").detail("DomainId", domainId);
		return Reference<BlobCipherKey>();
	}

	Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
	Reference<BlobCipherKey> cipherKey = keyIdCache->getLatestCipherKey();

	cipherKey.isValid() ? ++BlobCipherMetrics::getInstance()->latestCipherKeyCacheHit
	                    : ++BlobCipherMetrics::getInstance()->latestCipherKeyCacheMiss;
	return cipherKey;
}

Reference<BlobCipherKey> BlobCipherKeyCache::getCipherKey(const EncryptCipherDomainId& domainId,
                                                          const EncryptCipherBaseKeyId& baseCipherId,
                                                          const EncryptCipherRandomSalt& salt) {
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		return Reference<BlobCipherKey>();
	}

	Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
	Reference<BlobCipherKey> cipherKey = keyIdCache->getCipherByBaseCipherId(baseCipherId, salt);

	cipherKey.isValid() ? ++BlobCipherMetrics::getInstance()->cipherKeyCacheHit
	                    : ++BlobCipherMetrics::getInstance()->cipherKeyCacheMiss;

	return cipherKey;
}

void BlobCipherKeyCache::resetEncryptDomainId(const EncryptCipherDomainId domainId) {
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		return;
	}

	Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
	ASSERT(keyIdCache->getSize() <= size);
	size -= keyIdCache->getSize();
	keyIdCache->cleanup();
	TraceEvent(SevInfo, "BlobCipherResetEncryptDomainId").detail("DomainId", domainId);
}

void BlobCipherKeyCache::cleanup() noexcept {
	Reference<BlobCipherKeyCache> instance = BlobCipherKeyCache::getInstance();

	TraceEvent(SevInfo, "BlobCipherKeyCacheCleanup").log();

	for (auto& domainItr : instance->domainCacheMap) {
		Reference<BlobCipherKeyIdCache> keyIdCache = domainItr.second;
		keyIdCache->cleanup();
		TraceEvent(SevInfo, "BlobCipherKeyCacheCleanup").detail("DomainId", domainItr.first);
	}

	instance->domainCacheMap.clear();
	instance->size = 0;
}

std::vector<Reference<BlobCipherKey>> BlobCipherKeyCache::getAllCiphers(const EncryptCipherDomainId& domainId) {
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		return {};
	}

	Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
	return keyIdCache->getAllCipherKeys();
}

void BlobCipherDetails::validateCipherDetailsWithCipherKey(Reference<BlobCipherKey> cipherKey) {
	if (!(baseCipherId == cipherKey->getBaseCipherId() && encryptDomainId == cipherKey->getDomainId() &&
	      salt == cipherKey->getSalt())) {
		TraceEvent(SevWarn, "EncryptionHeaderCipherMismatch")
		    .detail("TextDomainId", cipherKey->getDomainId())
		    .detail("ExpectedTextDomainId", encryptDomainId)
		    .detail("TextBaseCipherId", cipherKey->getBaseCipherId())
		    .detail("ExpectedTextBaseCipherId", baseCipherId)
		    .detail("TextSalt", cipherKey->getSalt())
		    .detail("ExpectedTextSalt", salt);
		throw encrypt_header_metadata_mismatch();
	}
}

Sha256KCV::Sha256KCV() : ctx(EVP_MD_CTX_new()) {
	if (ctx == nullptr) {
		TraceEvent(SevError, "ComputeSha256AllocFailed");
		throw encrypt_ops_error();
	}
}

Sha256KCV::~Sha256KCV() {
	if (ctx != nullptr) {
		EVP_MD_CTX_free(ctx);
	}
}

EncryptCipherKeyCheckValue Sha256KCV::computeKCV(const uint8_t* cipher, const int len) {
	if (!EVP_DigestInit_ex(ctx, EVP_sha256(), NULL)) {
		TraceEvent(SevWarnAlways, "ComputeSha256DigestInitFailed");
		throw encrypt_ops_error();
	}

	if (!EVP_DigestUpdate(ctx, cipher, len)) {
		TraceEvent(SevWarnAlways, "ComputeSha256DigestUpdateFailed");
		throw encrypt_ops_error();
	}

	unsigned char sha256[EVP_MAX_MD_SIZE];
	unsigned int sha256Len;
	if (!EVP_DigestFinal_ex(ctx, sha256, &sha256Len)) {
		TraceEvent(SevWarnAlways, "ComputeSha256DigestFinalFailed");
		throw encrypt_ops_error();
	}

	// KeyValueCheck token allows FDB code to sanitize 'baseCipher' buffer, an external input to FDB. Given the token is
	// NOT meant to protect against any tampering attack, it is OK to truncate generated SHA256 KCV.

	ASSERT_LE(sizeof(EncryptCipherKeyCheckValue), EVP_MAX_MD_SIZE);
	EncryptCipherKeyCheckValue kcv;
	std::memcpy((uint8_t*)&kcv, sha256, sizeof(EncryptCipherKeyCheckValue));

#if BLOB_CIPHER_DEBUG
	TraceEvent("ComputeSha256KCV").detail("KCV", kcv);
#endif

	CODE_PROBE(true, "Sha256 KCV generation done");
	return kcv;
}

void Sha256KCV::checkEqual(const Reference<BlobCipherKey>& cipher, const EncryptCipherKeyCheckValue persisted) {
	ASSERT(cipher.isValid());

#if BLOB_CIPHER_DEBUG
	TraceEvent(SevDebug, "Sha256KCVCheckEqual")
	    .detail("CipherKCV", cipher->getBaseCipherKCV())
	    .detail("Persisted", persisted);
#endif

	if (cipher->getBaseCipherKCV() != persisted) {
		CODE_PROBE(true, "Sha256 Key Check Value mismatch");
		TraceEvent(SevWarnAlways, "Sha256KCVMismatch")
		    .detail("Computed", cipher->getBaseCipherKCV())
		    .detail("Persited", persisted)
		    .detail("DomainId", cipher->getDomainId())
		    .detail("BaseCipherId", cipher->getBaseCipherId());
		throw encrypt_key_check_value_mismatch();
	}
	CODE_PROBE(true, "Sha256 KCV validation done");
}

// Only used to link unit tests
void forceLinkBlobCipherTests() {}

// Tests cases includes:
// 1. Populate cache by inserting 'baseCipher' details for new encryptionDomainIds
// 2. Random lookup for cipherKeys and content validation
// 3. Inserting of 'identical' cipherKey (already cached) more than once works as desired.
// 4. Inserting of 'non-identical' cipherKey (already cached) more than once works as desired.
// 5. Validation encryption ops (correctness):
//  5.1. Encrypt a buffer followed by decryption of the buffer, validate the contents.
//  5.2. Simulate anomalies such as: EncryptionHeader corruption, authToken mismatch / encryptionMode mismatch etc.
// 6. Cache cleanup
//  6.1  cleanup cipherKeys by given encryptDomainId
//  6.2. Cleanup all cached cipherKeys

namespace {
// Construct a dummy External Key Manager representation and populate with some keys
class BaseCipher : public ReferenceCounted<BaseCipher>, NonCopyable {
public:
	EncryptCipherDomainId domainId;
	int len;
	EncryptCipherBaseKeyId keyId;
	std::unique_ptr<uint8_t[]> key;
	EncryptCipherKeyCheckValue kcv;
	int64_t refreshAt;
	int64_t expireAt;
	EncryptCipherRandomSalt generatedSalt;

	BaseCipher(const EncryptCipherDomainId& dId,
	           const EncryptCipherBaseKeyId& kId,
	           const int64_t rAt,
	           const int64_t eAt)
	  : domainId(dId),
	    len(deterministicRandom()->randomInt(4, MAX_BASE_CIPHER_LEN + 1)),
	    keyId(kId),
	    key(std::make_unique<uint8_t[]>(len)),
	    refreshAt(rAt),
	    expireAt(eAt) {
		deterministicRandom()->randomBytes(key.get(), len);
		kcv = Sha256KCV().computeKCV(key.get(), len);
	}
};

Reference<BlobCipherKey> corruptCipherKey(const Reference<BlobCipherKey>& cipherKey) {
	std::unique_ptr<uint8_t[]> corruptedBaseCipher = std::make_unique<uint8_t[]>(cipherKey->getBaseCipherLen());
	memcpy(corruptedBaseCipher.get(), cipherKey->rawBaseCipher(), cipherKey->getBaseCipherLen());
	const int idx = deterministicRandom()->randomInt(0, cipherKey->getBaseCipherLen());
	corruptedBaseCipher.get()[idx]++;
	const EncryptCipherKeyCheckValue baseCipherKCV =
	    Sha256KCV().computeKCV(corruptedBaseCipher.get(), cipherKey->getBaseCipherLen());
	return makeReference<BlobCipherKey>(cipherKey->getDomainId(),
	                                    cipherKey->getBaseCipherId(),
	                                    corruptedBaseCipher.get(),
	                                    cipherKey->getBaseCipherLen(),
	                                    baseCipherKCV,
	                                    cipherKey->getRefreshAtTS(),
	                                    cipherKey->getExpireAtTS());
}

using BaseKeyMap = std::unordered_map<EncryptCipherBaseKeyId, Reference<BaseCipher>>;
using DomainKeyMap = std::unordered_map<EncryptCipherDomainId, BaseKeyMap>;

template <class Params>
int getAuthHeaderVersion() {
	if (std::is_same_v<Params, AesCtrWithHmacParams>) {
		return CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_HMAC_SHA_AUTH_VERSION;
	} else if (std::is_same_v<Params, AesCtrWithCmacParams>) {
		return CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_AES_CMAC_AUTH_VERSION;
	} else {
		throw unsupported_operation();
	}
}

template <class Params>
EncryptAuthTokenAlgo getAuthTokenAlgo() {
	if (std::is_same_v<Params, AesCtrWithHmacParams>) {
		return EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA;
	} else if (std::is_same_v<Params, AesCtrWithCmacParams>) {
		return EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC;
	} else {
		throw unsupported_operation();
	}
}

template <class Params>
EncryptCipherMode getEncryptMode() {
	if (std::is_same_v<Params, AesCtrWithHmacParams> || std::is_same_v<Params, AesCtrWithCmacParams>) {
		return EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR;
	} else {
		throw unsupported_operation();
	}
}

template <class Params>
std::string getEncryptModeStr() {
	if (std::is_same_v<Params, AesCtrWithHmacParams>) {
		return "HMAC_SHA";
	} else if (std::is_same_v<Params, AesCtrWithCmacParams>) {
		return "CMAC_SHA";
	} else {
		throw unsupported_operation();
	}
}

// The following test functions wrap encryptInplace() and encrypt() and randomly decide which one to use for the test
StringRef encryptTest(Reference<EncryptBlobCipher> encryptor,
                      const uint8_t* plaintext,
                      const int plaintextLen,
                      BlobCipherEncryptHeaderRef* headerRef,
                      Arena& arena) {
	bool useInplace = deterministicRandom()->coinflip();
	StringRef encrypted;
	if (useInplace && encryptor->supportsInplaceEncryption()) {
		encrypted = makeString(plaintextLen, arena);
		uint8_t* data = mutateString(encrypted);
		memcpy(data, plaintext, plaintextLen);
		encryptor->encryptInplace(data, plaintextLen, headerRef);
	} else {
		encrypted = encryptor->encrypt(plaintext, plaintextLen, headerRef, arena);
	}
	return encrypted;
}

// The following test functions wrap decryptInplace() and decrypt() and randomly decide which one to use for the test
StringRef decryptTest(Reference<DecryptBlobCipher> decryptor,
                      const uint8_t* ciphertext,
                      const int ciphertextLen,
                      const BlobCipherEncryptHeaderRef& headerRef,
                      Arena& arena) {
	bool useInplace = deterministicRandom()->coinflip();
	StringRef decrypted;
	if (useInplace && decryptor->supportsInplaceDecryption()) {
		decrypted = makeString(ciphertextLen, arena);
		uint8_t* data = mutateString(decrypted);
		memcpy(data, ciphertext, ciphertextLen);
		decryptor->decryptInplace(data, ciphertextLen, headerRef);
	} else {
		decrypted = decryptor->decrypt(ciphertext, ciphertextLen, headerRef, arena);
	}
	return decrypted;
}

} // namespace

void testMaxBaseCipherLen() {
	TraceEvent("TestMaxBaseCipherLenStart");
	try {
		const int baseCipherLen = deterministicRandom()->randomInt(MAX_BASE_CIPHER_LEN + 1, MAX_BASE_CIPHER_LEN + 10);
		uint8_t baseCipher[baseCipherLen];
		deterministicRandom()->randomBytes(&baseCipher[0], baseCipherLen);
		const EncryptCipherKeyCheckValue baseCipherKCV = Sha256KCV().computeKCV(&baseCipher[0], baseCipherLen);
		Reference<BlobCipherKey> cipher = makeReference<BlobCipherKey>(1,
		                                                               1,
		                                                               &baseCipher[0],
		                                                               baseCipherLen,
		                                                               baseCipherKCV,
		                                                               std::numeric_limits<int64_t>::max(),
		                                                               std::numeric_limits<int64_t>::max());
		ASSERT(false); // error expected
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_max_base_cipher_len);
	}
	TraceEvent("TestMaxBaseCipherLenDone");
}

void testKeyCacheEssentials(DomainKeyMap& domainKeyMap,
                            const int minDomainId,
                            const int maxDomainId,
                            const int minBaseCipherKeyId) {
	TraceEvent("TestCacheEssentialsStart");

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// validate getLatestCipherKey return empty when there's no cipher key
	TraceEvent("BlobCipherTestLatestKeyNotExists").log();
	try {
		cipherKeyCache->getLatestCipherKey(INVALID_ENCRYPT_DOMAIN_ID);
		ASSERT(false); // shouldn't get here
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_invalid_id);
	}

	// insert BlobCipher keys into BlobCipherKeyCache map and validate
	TraceEvent("TestInsertKeys").log();
	for (auto& domainItr : domainKeyMap) {
		for (auto& baseKeyItr : domainItr.second) {
			Reference<BaseCipher> baseCipher = baseKeyItr.second;

			cipherKeyCache->insertCipherKey(baseCipher->domainId,
			                                baseCipher->keyId,
			                                baseCipher->key.get(),
			                                baseCipher->len,
			                                baseCipher->kcv,
			                                baseCipher->refreshAt,
			                                baseCipher->expireAt);
			Reference<BlobCipherKey> fetchedKey = cipherKeyCache->getLatestCipherKey(baseCipher->domainId);
			baseCipher->generatedSalt = fetchedKey->getSalt();
		}
	}
	// insert EncryptHeader BlobCipher key
	Reference<BaseCipher> headerBaseCipher = makeReference<BaseCipher>(
	    ENCRYPT_HEADER_DOMAIN_ID, 1, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max());
	cipherKeyCache->insertCipherKey(headerBaseCipher->domainId,
	                                headerBaseCipher->keyId,
	                                headerBaseCipher->key.get(),
	                                headerBaseCipher->len,
	                                headerBaseCipher->kcv,
	                                headerBaseCipher->refreshAt,
	                                headerBaseCipher->expireAt);

	TraceEvent("TestInsertKeysDone").log();

	// validate the cipherKey lookups work as desired
	for (auto& domainItr : domainKeyMap) {
		for (auto& baseKeyItr : domainItr.second) {
			Reference<BaseCipher> baseCipher = baseKeyItr.second;
			Reference<BlobCipherKey> cipherKey =
			    cipherKeyCache->getCipherKey(baseCipher->domainId, baseCipher->keyId, baseCipher->generatedSalt);
			ASSERT(cipherKey.isValid());
			// validate common cipher properties - domainId, baseCipherId, baseCipherLen, rawBaseCipher
			ASSERT_EQ(cipherKey->getBaseCipherId(), baseCipher->keyId);
			ASSERT_EQ(cipherKey->getDomainId(), baseCipher->domainId);
			ASSERT_EQ(cipherKey->getBaseCipherLen(), baseCipher->len);
			// ensure that baseCipher matches with the cached information
			ASSERT_EQ(std::memcmp(cipherKey->rawBaseCipher(), baseCipher->key.get(), cipherKey->getBaseCipherLen()), 0);
			// validate the encryption derivation
			const int len = std::min(AES_256_KEY_LENGTH, cipherKey->getBaseCipherLen());
			ASSERT_NE(std::memcmp(cipherKey->rawCipher(), baseCipher->key.get(), len), 0);
		}
	}
	TraceEvent("TestLooksupDone").log();

	// Ensure attemtping to insert existing cipherKey (identical) more than once is treated as a NOP
	try {
		Reference<BaseCipher> baseCipher = domainKeyMap[minDomainId][minBaseCipherKeyId];
		cipherKeyCache->insertCipherKey(baseCipher->domainId,
		                                baseCipher->keyId,
		                                baseCipher->key.get(),
		                                baseCipher->len,
		                                baseCipher->kcv,
		                                std::numeric_limits<int64_t>::max(),
		                                std::numeric_limits<int64_t>::max());
	} catch (Error& e) {
		throw;
	}
	TraceEvent("TestReinsertIdempotentKeyDone").log();

	// Ensure attemtping to insert an existing cipherKey (modified) fails with appropriate error
	try {
		Reference<BaseCipher> baseCipher = domainKeyMap[minDomainId][minBaseCipherKeyId];
		uint8_t rawCipher[baseCipher->len];
		memcpy(rawCipher, baseCipher->key.get(), baseCipher->len);
		// modify cipherKey by flipping a bit
		const int idx = deterministicRandom()->randomInt(0, baseCipher->len);
		rawCipher[idx]++;
		cipherKeyCache->insertCipherKey(baseCipher->domainId,
		                                baseCipher->keyId,
		                                &rawCipher[0],
		                                baseCipher->len,
		                                baseCipher->kcv,
		                                std::numeric_limits<int64_t>::max(),
		                                std::numeric_limits<int64_t>::max());
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_update_cipher) {
			throw;
		}
		TraceEvent("TestReinsertNonIdempotentKeyDone");
	}

	TraceEvent("TestCacheEssentialsEnd");
}

void testKeyCacheRefreshExpireCipherKey(DomainKeyMap& domainKeyMap, const int maxDomainId) {
	TraceEvent("BlobCipherCacheRefreshCipherKey");

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
	EncryptCipherDomainId domId = maxDomainId + 1;
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(domId);
	ASSERT(!cipherKey.isValid());

	Standalone<StringRef> baseCipher = makeString(4);
	deterministicRandom()->randomBytes(mutateString(baseCipher), 4);
	EncryptCipherKeyCheckValue baseCipherKCV = Sha256KCV().computeKCV(baseCipher.begin(), baseCipher.size());

	Counter::Value expectedNeedRefreshCount =
	    BlobCipherMetrics::getInstance()->latestCipherKeyCacheNeedsRefresh.getValue();
	Counter::Value expectedLatestHitCount = BlobCipherMetrics::getInstance()->latestCipherKeyCacheHit.getValue();
	Counter::Value expectedLatestMissCount = BlobCipherMetrics::getInstance()->cipherKeyCacheMiss.getValue();
	Counter::Value expectedMissCount = BlobCipherMetrics::getInstance()->cipherKeyCacheMiss.getValue();
	Counter::Value expectedHitCount = BlobCipherMetrics::getInstance()->cipherKeyCacheHit.getValue();
	Counter::Value expectedExpiredKeys = BlobCipherMetrics::getInstance()->cipherKeyCacheExpired.getValue();
	// Insert key that needs refresh
	int64_t refreshAt = now() - 1;
	int64_t expireAt = std::numeric_limits<int64_t>::max();
	Reference<BlobCipherKey> inserted = cipherKeyCache->insertCipherKey(
	    domId, 1, baseCipher.begin(), baseCipher.size(), baseCipherKCV, refreshAt, expireAt);
	EncryptCipherRandomSalt salt = inserted->getSalt();

	Reference<BlobCipherKey> cipher = cipherKeyCache->getLatestCipherKey(domId);
	expectedLatestMissCount++;
	expectedNeedRefreshCount++;
	// Ensure cache return an invalid cipher
	ASSERT(!cipher.isValid());
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheNeedsRefresh.getValue(), expectedNeedRefreshCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheMiss.getValue(), expectedLatestMissCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheHit.getValue(), expectedLatestHitCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheMiss.getValue(), expectedMissCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheHit.getValue(), expectedHitCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheExpired.getValue(), expectedExpiredKeys);

	// Ensure point-lookup still returns valid key
	cipher = cipherKeyCache->getCipherKey(domId, 1, salt);
	expectedHitCount++;
	ASSERT(cipher.isValid());
	ASSERT_EQ(cipher->getDomainId(), domId);
	ASSERT_EQ(cipher->getBaseCipherId(), 1);
	ASSERT_EQ(cipher->getBaseCipherLen(), 4);
	ASSERT_EQ(memcmp(cipher->rawBaseCipher(), baseCipher.begin(), 4), 0);
	ASSERT_EQ(cipher->getRefreshAtTS(), refreshAt);
	ASSERT_EQ(cipher->getExpireAtTS(), expireAt);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheNeedsRefresh.getValue(), expectedNeedRefreshCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheMiss.getValue(), expectedLatestMissCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheHit.getValue(), expectedLatestHitCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheMiss.getValue(), expectedMissCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheHit.getValue(), expectedHitCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheExpired.getValue(), expectedExpiredKeys);

	// Re-insert same key with same 'baseCipherId' and cache should accept it
	refreshAt = now() + 5;
	expireAt = now() + 10; // limit the expiry of the cipher
	Reference<BlobCipherKey> insertAgain = cipherKeyCache->insertCipherKey(
	    domId, 1, baseCipher.begin(), baseCipher.size(), baseCipherKCV, refreshAt, expireAt);
	salt = insertAgain->getSalt();
	cipher = cipherKeyCache->getLatestCipherKey(domId);
	expectedLatestHitCount++;
	ASSERT(cipher.isValid());
	ASSERT_EQ(cipher->getDomainId(), domId);
	ASSERT_EQ(cipher->getBaseCipherId(), 1);
	ASSERT_EQ(cipher->getBaseCipherLen(), 4);
	ASSERT_EQ(memcmp(cipher->rawBaseCipher(), baseCipher.begin(), 4), 0);
	ASSERT_EQ(cipher->getRefreshAtTS(), refreshAt);
	ASSERT_EQ(cipher->getExpireAtTS(), expireAt);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheNeedsRefresh.getValue(), expectedNeedRefreshCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheMiss.getValue(), expectedLatestMissCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheHit.getValue(), expectedLatestHitCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheMiss.getValue(), expectedMissCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheHit.getValue(), expectedHitCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheExpired.getValue(), expectedExpiredKeys);

	// Insert an expired cipherkey
	domId++;
	expireAt = now() - 100;
	refreshAt = expireAt - 10;
	inserted = cipherKeyCache->insertCipherKey(
	    domId, 1, baseCipher.begin(), baseCipher.size(), baseCipherKCV, refreshAt, expireAt);
	salt = inserted->getSalt();

	// Ensure getLatestCipher desired behavior
	cipher = cipherKeyCache->getLatestCipherKey(domId);
	ASSERT(!cipher.isValid());
	// Already expired key, hence, getLookupByBaseCipher would fail, hence, NOT increment 'needsRefresh' counter
	expectedLatestMissCount++;
	expectedExpiredKeys++;
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheNeedsRefresh.getValue(), expectedNeedRefreshCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheMiss.getValue(), expectedLatestMissCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheHit.getValue(), expectedLatestHitCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheMiss.getValue(), expectedMissCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheHit.getValue(), expectedHitCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheExpired.getValue(), expectedExpiredKeys);

	// Ensure getCipher desired behavior
	inserted = cipherKeyCache->insertCipherKey(
	    domId, 1, baseCipher.begin(), baseCipher.size(), baseCipherKCV, refreshAt, expireAt);
	salt = inserted->getSalt();
	cipher = cipherKeyCache->getCipherKey(domId, 1, salt);
	ASSERT(!cipher.isValid());
	expectedMissCount++;
	expectedExpiredKeys++;
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheNeedsRefresh.getValue(), expectedNeedRefreshCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheMiss.getValue(), expectedLatestMissCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->latestCipherKeyCacheHit.getValue(), expectedLatestHitCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheMiss.getValue(), expectedMissCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheHit.getValue(), expectedHitCount);
	ASSERT_EQ(BlobCipherMetrics::getInstance()->cipherKeyCacheExpired.getValue(), expectedExpiredKeys);
}

void testConfigurableEncryptionBlobCipherHeaderFlagsV1Ser() {
	Arena arena;

	// Version-1
	BlobCipherEncryptHeaderFlagsV1 flags(
	    ENCRYPT_CIPHER_MODE_AES_256_CTR, getRandomAuthTokenMode(), getRandomAuthTokenAlgo());
	Standalone<StringRef> ser = BlobCipherEncryptHeaderFlagsV1::toStringRef(flags, arena);
	ASSERT_EQ(ser.size(), sizeof(flags));
}

void testConfigurableEncryptionAesCtrNoAuthV1Ser(const int minDomainId) {
	Arena arena;
	BlobCipherEncryptHeaderRef headerRef;
	uint32_t size = 0;

	BlobCipherEncryptHeaderFlagsV1 flags = BlobCipherEncryptHeaderFlagsV1(
	    ENCRYPT_CIPHER_MODE_AES_256_CTR, ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE, ENCRYPT_HEADER_AUTH_TOKEN_ALGO_NONE);
	size += sizeof(BlobCipherEncryptHeaderFlagsV1) + 2;
	headerRef.flags = flags;

	AesCtrNoAuth noAuth;
	noAuth.v1.cipherTextDetails = BlobCipherDetails(1, 2, 23);
	deterministicRandom()->randomBytes(&noAuth.v1.iv[0], AES_256_CTR_IV_LENGTH);
	Standalone<StringRef> serAlgo = AesCtrNoAuth::toStringRef(noAuth);
	ASSERT_EQ(serAlgo.size(), sizeof(noAuth));

	size += AesCtrNoAuth::getSize();

	headerRef.algoHeader = noAuth;
	Standalone<StringRef> serHeader = BlobCipherEncryptHeaderRef::toStringRef(headerRef);
	ASSERT_EQ(serHeader.size(), size);
	ASSERT_EQ(size,
	          BlobCipherEncryptHeaderRef::getHeaderSize(headerRef.flagsVersion(),
	                                                    headerRef.algoHeaderVersion(),
	                                                    (EncryptCipherMode)flags.encryptMode,
	                                                    (EncryptAuthTokenMode)flags.authTokenMode,
	                                                    (EncryptAuthTokenAlgo)flags.authTokenAlgo));
}

template <class Params>
void testConfigurableEncryptionAesCtrWithAuthSer(const int minDomainId) {
	Arena arena;
	BlobCipherEncryptHeaderRef headerRef;
	uint32_t size = 0;

	BlobCipherEncryptHeaderFlagsV1 flags = BlobCipherEncryptHeaderFlagsV1(
	    getEncryptMode<Params>(), ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE, getAuthTokenAlgo<Params>());
	size += sizeof(BlobCipherEncryptHeaderFlagsV1) + 2;

	headerRef.flags = flags;

	AesWithAuth<Params> withAuth;
	withAuth.v1.cipherTextDetails = BlobCipherDetails(1, 2, 23);
	withAuth.v1.cipherHeaderDetails = BlobCipherDetails(ENCRYPT_HEADER_DOMAIN_ID, 2, 23);
	deterministicRandom()->randomBytes(&withAuth.v1.iv[0], AES_256_CTR_IV_LENGTH);
	deterministicRandom()->randomBytes(&withAuth.v1.authToken[0], Params::authTokenSize);
	Standalone<StringRef> serAlgo = AesWithAuth<Params>::toStringRef(withAuth);
	ASSERT_EQ(serAlgo.size(), AesWithAuth<Params>::getSize());

	size += AesWithAuth<Params>::getSize();

	headerRef.algoHeader = withAuth;
	Standalone<StringRef> serHeader = BlobCipherEncryptHeaderRef::toStringRef(headerRef);
	ASSERT_EQ(serHeader.size(), size);
	ASSERT_EQ(size,
	          BlobCipherEncryptHeaderRef::getHeaderSize(headerRef.flagsVersion(),
	                                                    headerRef.algoHeaderVersion(),
	                                                    (EncryptCipherMode)flags.encryptMode,
	                                                    (EncryptAuthTokenMode)flags.authTokenMode,
	                                                    (EncryptAuthTokenAlgo)flags.authTokenAlgo));
}

void testConfigurableEncryptionAesCtrWithAuthSerGcm(const int minDomainId) {
	Arena arena;
	BlobCipherEncryptHeaderRef headerRef;
	uint32_t size = 0;

	BlobCipherEncryptHeaderFlagsV1 flags =
	    BlobCipherEncryptHeaderFlagsV1(EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM,
	                                   ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE,
	                                   EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM);
	size += sizeof(BlobCipherEncryptHeaderFlagsV1) + 2;

	headerRef.flags = flags;

	AesGcm withAuth;
	get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails = BlobCipherDetails(1, 2, 23);
	deterministicRandom()->randomBytes(&get<AesGcmV1>(withAuth.headerVersions).iv[0], AES_256_GCM_IV_LENGTH);
	deterministicRandom()->randomBytes(&get<AesGcmV1>(withAuth.headerVersions).authToken[0], AES_256_GCM_TAG_LENGTH);
	Standalone<StringRef> serAlgo = AesGcm::toStringRef(withAuth);
	ASSERT_EQ(serAlgo.size(), AesGcm::getSize());

	size += AesGcm::getSize();

	headerRef.algoHeader = withAuth;
	Standalone<StringRef> serHeader = BlobCipherEncryptHeaderRef::toStringRef(headerRef);
	ASSERT_EQ(serHeader.size(), size);
	ASSERT_EQ(size,
	          BlobCipherEncryptHeaderRef::getHeaderSize(headerRef.flagsVersion(),
	                                                    headerRef.algoHeaderVersion(),
	                                                    (EncryptCipherMode)flags.encryptMode,
	                                                    (EncryptAuthTokenMode)flags.authTokenMode,
	                                                    (EncryptAuthTokenAlgo)flags.authTokenAlgo));
}

void testConfigurableEncryptionHeaderNoAuthMode(const int minDomainId) {
	TraceEvent("TestConfigurableEncryptionHeader").detail("Mode", "No-Auth");

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	Arena arena;
	uint8_t iv[AES_256_CTR_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_CTR_IV_LENGTH);

	Reference<EncryptBlobCipher> encryptor =
	    EncryptBlobCipher::getEncryptor(cipherKey,
	                                    headerCipherKey,
	                                    iv,
	                                    AES_256_CTR_IV_LENGTH,
	                                    EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	                                    EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_NONE,
	                                    EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR,
	                                    BlobCipherMetrics::TEST);

	BlobCipherEncryptHeaderRef headerRef;
	encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);

	ASSERT_EQ(headerRef.flagsVersion(), 1);
	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	ASSERT_EQ(flags.authTokenMode, headerRef.getAuthTokenMode());
	AesCtrNoAuth noAuth = std::get<AesCtrNoAuth>(headerRef.algoHeader);

	const uint8_t* headerIV = headerRef.getIV();
	ASSERT_EQ(memcmp(&headerIV[0], &iv[0], AES_256_CTR_IV_LENGTH), 0);

	EncryptHeaderCipherDetails validateDetails = headerRef.getCipherDetails();
	ASSERT(validateDetails.textCipherDetails.isValid() &&
	       validateDetails.textCipherDetails ==
	           BlobCipherDetails(cipherKey->getDomainId(), cipherKey->getBaseCipherId(), cipherKey->getSalt()));
	ASSERT(!validateDetails.headerCipherDetails.present());

	Standalone<StringRef> serHeaderRef = BlobCipherEncryptHeaderRef::toStringRef(headerRef);
	BlobCipherEncryptHeaderRef validateHeader = BlobCipherEncryptHeaderRef::fromStringRef(serHeaderRef);
	BlobCipherEncryptHeaderFlagsV1 validateFlags = std::get<BlobCipherEncryptHeaderFlagsV1>(validateHeader.flags);
	ASSERT(validateFlags == flags);

	AesCtrNoAuth validateAlgo = std::get<AesCtrNoAuth>(validateHeader.algoHeader);
	ASSERT(validateAlgo.v1.cipherTextDetails == noAuth.v1.cipherTextDetails);
	ASSERT_EQ(memcmp(&validateAlgo.v1.iv[0], &noAuth.v1.iv[0], AES_256_CTR_IV_LENGTH), 0);

	TraceEvent("NoAuthHeaderSize")
	    .detail("Flags", sizeof(flags))
	    .detail("AlgoHeader", noAuth.getSize())
	    .detail("TotalHeader", serHeaderRef.size());

	TraceEvent("TestConfigurableEncryptionHeader").detail("Mode", "No-Auth");
}

void testConfigurableEncryptionNoAuthMode(const int minDomainId) {
	TraceEvent("TestConfigurableEncryptionNoAuthModeStart");

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	Arena arena;
	uint8_t iv[AES_256_CTR_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_CTR_IV_LENGTH);

	Reference<EncryptBlobCipher> encryptor =
	    EncryptBlobCipher::getEncryptor(cipherKey,
	                                    headerCipherKey,
	                                    iv,
	                                    AES_256_CTR_IV_LENGTH,
	                                    EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	                                    EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_NONE,
	                                    EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR,
	                                    BlobCipherMetrics::TEST);

	BlobCipherEncryptHeaderRef headerRef;

	StringRef encryptedBuf = encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);

	// validate header version details
	AesCtrNoAuth noAuth = std::get<AesCtrNoAuth>(headerRef.algoHeader);
	Reference<BlobCipherKey> tCipherKey = cipherKeyCache->getCipherKey(noAuth.v1.cipherTextDetails.encryptDomainId,
	                                                                   noAuth.v1.cipherTextDetails.baseCipherId,
	                                                                   noAuth.v1.cipherTextDetails.salt);
	ASSERT(tCipherKey->isEqual(cipherKey));
	Reference<DecryptBlobCipher> decryptor =
	    DecryptBlobCipher::getDecryptor(tCipherKey,
	                                    Reference<BlobCipherKey>(),
	                                    &noAuth.v1.iv[0],
	                                    EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR,
	                                    BlobCipherMetrics::TEST);

	StringRef decryptedBuf = decryptTest(decryptor, encryptedBuf.begin(), encryptedBuf.size(), headerRef, arena);
	ASSERT_EQ(decryptedBuf.size(), bufLen);
	ASSERT_EQ(memcmp(decryptedBuf.begin(), &orgData[0], bufLen), 0);

	TraceEvent("TestConfigurableEncryptionNoAuthDecryptDone")
	    .detail("HeaderFlagsVersion", headerRef.flagsVersion())
	    .detail("AlgoHeaderVersion", headerRef.algoHeaderVersion())
	    .detail("HeaderEncryptMode", ENCRYPT_CIPHER_MODE_AES_256_CTR)
	    .detail("HeaderEncryptAuthTokenMode", ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE)
	    .detail("HeaderEncryptAuthTokenAlgo", ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE)
	    .detail("DomainId", noAuth.v1.cipherTextDetails.encryptDomainId)
	    .detail("BaseCipherId", noAuth.v1.cipherTextDetails.baseCipherId)
	    .detail("Salt", noAuth.v1.cipherTextDetails.salt);

	// induce encryption header corruption - encryptionMode corrupted
	BlobCipherEncryptHeaderRef corruptedHeaderRef = BlobCipherEncryptHeaderRef(headerRef);
	BlobCipherEncryptHeaderFlagsV1 corruptedFlags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	corruptedFlags.encryptMode = EncryptCipherMode::ENCRYPT_CIPHER_MODE_LAST;
	corruptedHeaderRef.flags = corruptedFlags;
	try {
		encryptedBuf = encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);
		Reference<DecryptBlobCipher> decryptor =
		    DecryptBlobCipher::getDecryptor(tCipherKey,
		                                    Reference<BlobCipherKey>(),
		                                    &iv[0],
		                                    EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR,
		                                    BlobCipherMetrics::TEST);
		decryptedBuf = decryptTest(decryptor, encryptedBuf.begin(), bufLen, corruptedHeaderRef, arena);
		ASSERT(false); // error expected
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_metadata_mismatch) {
			throw;
		}
		TraceEvent("TestConfigurableEncryptionNoAuthHeaderCorruptionDone");
	}

	// induce encrypted buffer payload corruption
	try {
		encryptedBuf = encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);
		uint8_t temp[bufLen];
		deterministicRandom()->randomBytes(&temp[0], bufLen);
		memcpy((void*)encryptedBuf.begin(), &temp[0], bufLen);
		int tIdx = deterministicRandom()->randomInt(0, bufLen - 1);
		temp[tIdx] += 1;
		Reference<DecryptBlobCipher> decryptor =
		    DecryptBlobCipher::getDecryptor(tCipherKey,
		                                    Reference<BlobCipherKey>(),
		                                    &iv[0],
		                                    EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR,
		                                    BlobCipherMetrics::TEST);
		decryptedBuf = decryptTest(decryptor, &temp[0], bufLen, headerRef, arena);
		ASSERT_NE(memcmp(decryptedBuf.begin(), &orgData[0], bufLen), 0);
		TraceEvent("TestConfigurableEncryptionNoAuthPayloadCorruptionDone");
	} catch (Error& e) {
		// No authToken, hence, no corruption detection supported
		ASSERT(false);
	}

	// induce baseCipher corruption
	try {
		Reference<BlobCipherKey> corruptedTextCipher = corruptCipherKey(tCipherKey);
		encryptedBuf = encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);
		Reference<DecryptBlobCipher> decryptor =
		    DecryptBlobCipher::getDecryptor(corruptedTextCipher,
		                                    Reference<BlobCipherKey>(),
		                                    &iv[0],
		                                    EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR,
		                                    BlobCipherMetrics::TEST);
		decryptedBuf = decryptTest(decryptor, encryptedBuf.begin(), bufLen, headerRef, arena);
		ASSERT(false); // error expected
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_key_check_value_mismatch);
		TraceEvent("TestConfigurableEncryptionNoAuthBaseCipherCorruptionDone");
	}

	TraceEvent("ConfigurableEncryptionNoAuthDone");
}

// validate basic encrypt followed by decrypt operation for AUTH_TOKEN_MODE_SINGLE
// HMAC_SHA authToken algorithm
template <class Params>
void testConfigurableEncryptionHeaderSingleAuthMode(int minDomainId) {
	TraceEvent("TestEncryptionHeaderStart").detail("Mode", getEncryptModeStr<Params>());

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	Arena arena;
	uint8_t iv[Params::ivSize];
	deterministicRandom()->randomBytes(&iv[0], Params::ivSize);
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	Reference<EncryptBlobCipher> encryptor =
	    EncryptBlobCipher::getEncryptor(cipherKey,
	                                    headerCipherKey,
	                                    iv,
	                                    Params::ivSize,
	                                    EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE,
	                                    getAuthTokenAlgo<Params>(),
	                                    getEncryptMode<Params>(),
	                                    BlobCipherMetrics::TEST);

	BlobCipherEncryptHeaderRef headerRef;
	encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);

	ASSERT_EQ(headerRef.flagsVersion(), 1);
	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	ASSERT_EQ(flags.authTokenMode, headerRef.getAuthTokenMode());
	AesWithAuth<Params> algoHeader = std::get<AesWithAuth<Params>>(headerRef.algoHeader);

	const uint8_t* headerIV = headerRef.getIV();
	ASSERT_EQ(memcmp(&headerIV[0], &iv[0], Params::ivSize), 0);

	EncryptHeaderCipherDetails validateDetails = headerRef.getCipherDetails();
	ASSERT(validateDetails.textCipherDetails.isValid() &&
	       validateDetails.textCipherDetails ==
	           BlobCipherDetails(cipherKey->getDomainId(), cipherKey->getBaseCipherId(), cipherKey->getSalt()));
	ASSERT(validateDetails.headerCipherDetails.present() && validateDetails.headerCipherDetails.get().isValid() &&
	       validateDetails.headerCipherDetails.get() == BlobCipherDetails(headerCipherKey->getDomainId(),
	                                                                      headerCipherKey->getBaseCipherId(),
	                                                                      headerCipherKey->getSalt()));

	Standalone<StringRef> serHeaderRef = BlobCipherEncryptHeaderRef::toStringRef(headerRef);
	BlobCipherEncryptHeaderRef validateHeader = BlobCipherEncryptHeaderRef::fromStringRef(serHeaderRef);
	BlobCipherEncryptHeaderFlagsV1 validateFlags = std::get<BlobCipherEncryptHeaderFlagsV1>(validateHeader.flags);
	ASSERT(validateFlags == flags);

	AesWithAuth<Params> validateAlgo = std::get<AesWithAuth<Params>>(validateHeader.algoHeader);
	ASSERT(validateAlgo.v1.cipherTextDetails == algoHeader.v1.cipherTextDetails);
	ASSERT(validateAlgo.v1.cipherHeaderDetails == algoHeader.v1.cipherHeaderDetails);
	ASSERT_EQ(memcmp(&iv[0], &validateAlgo.v1.iv[0], Params::ivSize), 0);
	ASSERT_EQ(memcmp(&algoHeader.v1.authToken[0], &validateAlgo.v1.authToken[0], Params::authTokenSize), 0);

	TraceEvent("HeaderSize")
	    .detail("Flags", sizeof(flags))
	    .detail("AlgoHeader", algoHeader.getSize())
	    .detail("TotalHeader", serHeaderRef.size());

	TraceEvent("TestEncryptionHeaderEnd").detail("Mode", getEncryptMode<Params>());
}

void testConfigurableEncryptionHeaderSingleAuthModeGcm(int minDomainId) {
	TraceEvent("TestEncryptionHeaderStart").detail("Mode", "AES-GCM");

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	Arena arena;
	uint8_t iv[AES_256_GCM_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_GCM_IV_LENGTH);
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	Reference<EncryptBlobCipher> encryptor =
	    EncryptBlobCipher::getEncryptor(cipherKey,
	                                    Optional<Reference<BlobCipherKey>>(),
	                                    iv,
	                                    AES_256_GCM_IV_LENGTH,
	                                    EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE,
	                                    EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM,
	                                    EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM,
	                                    BlobCipherMetrics::TEST);

	BlobCipherEncryptHeaderRef headerRef;
	encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);

	ASSERT_EQ(headerRef.flagsVersion(), 1);
	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	ASSERT_EQ(flags.authTokenMode, headerRef.getAuthTokenMode());
	AesGcm algoHeader = std::get<AesGcm>(headerRef.algoHeader);

	const uint8_t* headerIV = headerRef.getIV();
	ASSERT_EQ(memcmp(&headerIV[0], &iv[0], AES_256_GCM_IV_LENGTH), 0);

	EncryptHeaderCipherDetails validateDetails = headerRef.getCipherDetails();
	ASSERT(validateDetails.textCipherDetails.isValid() &&
	       validateDetails.textCipherDetails ==
	           BlobCipherDetails(cipherKey->getDomainId(), cipherKey->getBaseCipherId(), cipherKey->getSalt()));
	ASSERT(!validateDetails.headerCipherDetails.present());

	Standalone<StringRef> serHeaderRef = BlobCipherEncryptHeaderRef::toStringRef(headerRef);
	BlobCipherEncryptHeaderRef validateHeader = BlobCipherEncryptHeaderRef::fromStringRef(serHeaderRef);
	BlobCipherEncryptHeaderFlagsV1 validateFlags = std::get<BlobCipherEncryptHeaderFlagsV1>(validateHeader.flags);
	ASSERT(validateFlags == flags);

	AesGcm validateAlgo = std::get<AesGcm>(validateHeader.algoHeader);
	ASSERT(get<AesGcmV1>(validateAlgo.headerVersions).cipherTextDetails ==
	       get<AesGcmV1>(algoHeader.headerVersions).cipherTextDetails);
	ASSERT_EQ(memcmp(&iv[0], &get<AesGcmV1>(validateAlgo.headerVersions).iv[0], AES_256_GCM_IV_LENGTH), 0);
	ASSERT_EQ(memcmp(&get<AesGcmV1>(algoHeader.headerVersions).authToken[0],
	                 &get<AesGcmV1>(validateAlgo.headerVersions).authToken[0],
	                 AES_256_GCM_TAG_LENGTH),
	          0);

	TraceEvent("HeaderSize")
	    .detail("Flags", sizeof(flags))
	    .detail("AlgoHeader", algoHeader.getSize())
	    .detail("TotalHeader", serHeaderRef.size());

	TraceEvent("TestEncryptionHeaderEnd").detail("Mode", "AES-GCM");
}

// validate basic encrypt followed by decrypt operation for AUTH_TOKEN_MODE_SINGLE
template <class Params>
void testConfigurableEncryptionSingleAuthMode(const int minDomainId) {
	const EncryptCipherMode encryptMode = getEncryptMode<Params>();
	const EncryptAuthTokenAlgo authAlgo = getAuthTokenAlgo<Params>();
	const std::string authAlgoStr = getEncryptModeStr<Params>();
	const int algoHeaderVersion = getAuthHeaderVersion<Params>();

	TraceEvent("TestConfigurableEncryptionSingleAuthStart").detail("Mode", authAlgoStr);

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	Arena arena;
	uint8_t iv[Params::ivSize];
	deterministicRandom()->randomBytes(&iv[0], Params::ivSize);
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	Reference<EncryptBlobCipher> encryptor =
	    EncryptBlobCipher::getEncryptor(cipherKey,
	                                    headerCipherKey,
	                                    iv,
	                                    Params::ivSize,
	                                    EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE,
	                                    authAlgo,
	                                    encryptMode,
	                                    BlobCipherMetrics::TEST);
	BlobCipherEncryptHeaderRef headerRef;
	StringRef encryptedBuf = encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);

	ASSERT_EQ(encryptedBuf.size(), bufLen);
	ASSERT_NE(memcmp(&orgData[0], encryptedBuf.begin(), bufLen), 0);
	ASSERT_EQ(headerRef.flagsVersion(), CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION);
	ASSERT_EQ(headerRef.algoHeaderVersion(), algoHeaderVersion);

	// validate flags
	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	ASSERT_EQ(flags.encryptMode, encryptMode);
	ASSERT_EQ(flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	ASSERT_EQ(flags.authTokenAlgo, authAlgo);

	// validate IV
	AesWithAuth<Params> withAuth = std::get<AesWithAuth<Params>>(headerRef.algoHeader);
	ASSERT_EQ(memcmp(&iv[0], &withAuth.v1.iv[0], Params::ivSize), 0);
	ASSERT_NE(memcmp(&orgData[0], encryptedBuf.begin(), bufLen), 0);
	// validate cipherKey details
	ASSERT_EQ(withAuth.v1.cipherTextDetails.encryptDomainId, cipherKey->getDomainId());
	ASSERT_EQ(withAuth.v1.cipherTextDetails.baseCipherId, cipherKey->getBaseCipherId());
	ASSERT_EQ(withAuth.v1.cipherTextDetails.salt, cipherKey->getSalt());
	ASSERT_EQ(withAuth.v1.cipherHeaderDetails.encryptDomainId, headerCipherKey->getDomainId());
	ASSERT_EQ(withAuth.v1.cipherHeaderDetails.baseCipherId, headerCipherKey->getBaseCipherId());
	ASSERT_EQ(withAuth.v1.cipherHeaderDetails.salt, headerCipherKey->getSalt());

	Reference<BlobCipherKey> tCipherKey = cipherKeyCache->getCipherKey(withAuth.v1.cipherTextDetails.encryptDomainId,
	                                                                   withAuth.v1.cipherTextDetails.baseCipherId,
	                                                                   withAuth.v1.cipherTextDetails.salt);
	Reference<BlobCipherKey> hCipherKey = cipherKeyCache->getCipherKey(withAuth.v1.cipherHeaderDetails.encryptDomainId,
	                                                                   withAuth.v1.cipherHeaderDetails.baseCipherId,
	                                                                   withAuth.v1.cipherHeaderDetails.salt);
	ASSERT(tCipherKey->isEqual(cipherKey));
	ASSERT(hCipherKey->isEqual(headerCipherKey));
	Reference<DecryptBlobCipher> decryptor = DecryptBlobCipher::getDecryptor(
	    tCipherKey, hCipherKey, &withAuth.v1.iv[0], encryptMode, BlobCipherMetrics::TEST);
	StringRef decryptedBuf = decryptTest(decryptor, encryptedBuf.begin(), bufLen, headerRef, arena);

	ASSERT_EQ(decryptedBuf.size(), bufLen);
	ASSERT_EQ(memcmp(decryptedBuf.begin(), &orgData[0], bufLen), 0);

	TraceEvent("TestConfigurableEncryptSingleAuthDecryptDone")
	    .detail("HeaderFlagsVersion", headerRef.flagsVersion())
	    .detail("AlgoHeaderVersion", headerRef.algoHeaderVersion())
	    .detail("HeaderEncryptMode", flags.encryptMode)
	    .detail("HeaderEncryptAuthTokenMode", flags.authTokenMode)
	    .detail("HeaderEncryptAuthTokenAlgo", flags.authTokenAlgo)
	    .detail("TextDomainId", withAuth.v1.cipherTextDetails.encryptDomainId)
	    .detail("TextBaseCipherId", withAuth.v1.cipherTextDetails.baseCipherId)
	    .detail("TextSalt", withAuth.v1.cipherTextDetails.salt)
	    .detail("HeaderDomainId", withAuth.v1.cipherHeaderDetails.encryptDomainId)
	    .detail("HeaderBaseCipherId", withAuth.v1.cipherHeaderDetails.baseCipherId)
	    .detail("HeaderSalt", withAuth.v1.cipherHeaderDetails.salt);

	// induce encryption header corruption - encryptionMode corrupted
	BlobCipherEncryptHeaderRef corruptedHeaderRef = BlobCipherEncryptHeaderRef(headerRef);
	BlobCipherEncryptHeaderFlagsV1 corruptedFlags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	corruptedFlags.encryptMode = EncryptCipherMode::ENCRYPT_CIPHER_MODE_LAST;
	corruptedHeaderRef.flags = corruptedFlags;
	try {
		encryptedBuf = encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);
		Reference<DecryptBlobCipher> decryptor =
		    DecryptBlobCipher::getDecryptor(tCipherKey,
		                                    hCipherKey,
		                                    &iv[0],
		                                    EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR,
		                                    BlobCipherMetrics::TEST);
		decryptedBuf = decryptTest(decryptor, encryptedBuf.begin(), bufLen, corruptedHeaderRef, arena);
		ASSERT(false); // error expected
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_metadata_mismatch) {
			throw;
		}
		TraceEvent("TestConfigurableEncryptionCorruptEncryptModeDone").detail("Mode", authAlgoStr);
	}

	// induce encrypted buffer payload corruption
	try {
		encryptedBuf = encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);
		uint8_t temp[bufLen];
		deterministicRandom()->randomBytes(temp, bufLen);
		Reference<DecryptBlobCipher> decryptor =
		    DecryptBlobCipher::getDecryptor(tCipherKey, hCipherKey, &iv[0], encryptMode, BlobCipherMetrics::TEST);
		decryptedBuf = decryptTest(decryptor, temp, bufLen, headerRef, arena);
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_header_authtoken_mismatch);
		TraceEvent("TestConfigurableEncryptionCorruptPayloadDone").detail("Mode", authAlgoStr);
	}

	// induce baseCipher payload corruption
	try {
		const bool corruptTextCipher = deterministicRandom()->coinflip();
		encryptedBuf = encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);
		if (corruptTextCipher) {
			Reference<BlobCipherKey> corruptedCipher = corruptCipherKey(tCipherKey);
			Reference<DecryptBlobCipher> decryptor = DecryptBlobCipher::getDecryptor(
			    corruptedCipher, hCipherKey, &iv[0], encryptMode, BlobCipherMetrics::TEST);
			decryptedBuf = decryptTest(decryptor, encryptedBuf.begin(), bufLen, headerRef, arena);
		} else {
			Reference<BlobCipherKey> corruptedCipher = corruptCipherKey(hCipherKey);
			Reference<DecryptBlobCipher> decryptor = DecryptBlobCipher::getDecryptor(
			    tCipherKey, corruptedCipher, &iv[0], encryptMode, BlobCipherMetrics::TEST);
			decryptedBuf = decryptTest(decryptor, encryptedBuf.begin(), bufLen, headerRef, arena);
		}
		ASSERT(false); // error expected
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_key_check_value_mismatch);
		TraceEvent("TestConfigurableEncryptionBaseCipherCorruptionDone").detail("Mode", authAlgoStr);
	}

	TraceEvent("TestSingleAuthTokenConfigurableEncryptionEnd").detail("Mode", authAlgoStr);
}

void testConfigurableEncryptionSingleAuthModeGcm(const int minDomainId) {
	const EncryptCipherMode encryptMode = EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM;
	const EncryptAuthTokenAlgo authAlgo = EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM;
	const std::string authAlgoStr = "AES-GCM";
	const int algoHeaderVersion = CLIENT_KNOBS->ENCRYPT_HEADER_AES_GCM_AUTH_VERSION;

	TraceEvent("TestConfigurableEncryptionSingleAuthStart").detail("Mode", authAlgoStr);

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	Arena arena;
	uint8_t iv[AES_256_GCM_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_GCM_IV_LENGTH);
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	Reference<EncryptBlobCipher> encryptor =
	    EncryptBlobCipher::getEncryptor(cipherKey,
	                                    Optional<Reference<BlobCipherKey>>(),
	                                    iv,
	                                    AES_256_GCM_IV_LENGTH,
	                                    EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE,
	                                    authAlgo,
	                                    encryptMode,
	                                    BlobCipherMetrics::TEST);
	BlobCipherEncryptHeaderRef headerRef;
	StringRef encryptedBuf = encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);

	ASSERT_EQ(encryptedBuf.size(), bufLen);
	ASSERT_NE(memcmp(&orgData[0], encryptedBuf.begin(), bufLen), 0);
	ASSERT_EQ(headerRef.flagsVersion(), CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION);
	ASSERT_EQ(headerRef.algoHeaderVersion(), algoHeaderVersion);

	// validate flags
	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	ASSERT_EQ(flags.encryptMode, encryptMode);
	ASSERT_EQ(flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	ASSERT_EQ(flags.authTokenAlgo, authAlgo);

	// validate IV
	AesGcm withAuth = std::get<AesGcm>(headerRef.algoHeader);
	ASSERT_EQ(memcmp(&iv[0], &get<AesGcmV1>(withAuth.headerVersions).iv[0], AES_256_GCM_IV_LENGTH), 0);
	ASSERT_NE(memcmp(&orgData[0], encryptedBuf.begin(), bufLen), 0);
	// validate cipherKey details
	ASSERT_EQ(get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.encryptDomainId, cipherKey->getDomainId());
	ASSERT_EQ(get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.baseCipherId, cipherKey->getBaseCipherId());
	ASSERT_EQ(get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.salt, cipherKey->getSalt());

	Reference<BlobCipherKey> tCipherKey =
	    cipherKeyCache->getCipherKey(get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.encryptDomainId,
	                                 get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.baseCipherId,
	                                 get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.salt);
	ASSERT(tCipherKey->isEqual(cipherKey));
	Reference<DecryptBlobCipher> decryptor =
	    DecryptBlobCipher::getDecryptor(tCipherKey,
	                                    Optional<Reference<BlobCipherKey>>(),
	                                    &get<AesGcmV1>(withAuth.headerVersions).iv[0],
	                                    encryptMode,
	                                    BlobCipherMetrics::TEST);
	StringRef decryptedBuf = decryptTest(decryptor, encryptedBuf.begin(), bufLen, headerRef, arena);

	ASSERT_EQ(decryptedBuf.size(), bufLen);
	ASSERT_EQ(memcmp(decryptedBuf.begin(), &orgData[0], bufLen), 0);

	TraceEvent("TestConfigurableEncryptSingleAuthDecryptDone")
	    .detail("HeaderFlagsVersion", headerRef.flagsVersion())
	    .detail("AlgoHeaderVersion", headerRef.algoHeaderVersion())
	    .detail("HeaderEncryptMode", flags.encryptMode)
	    .detail("HeaderEncryptAuthTokenMode", flags.authTokenMode)
	    .detail("HeaderEncryptAuthTokenAlgo", flags.authTokenAlgo)
	    .detail("TextDomainId", get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.encryptDomainId)
	    .detail("TextBaseCipherId", get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.baseCipherId)
	    .detail("TextSalt", get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.salt);

	// induce encryption header corruption - encryptionMode corrupted
	BlobCipherEncryptHeaderRef corruptedHeaderRef = BlobCipherEncryptHeaderRef(headerRef);
	BlobCipherEncryptHeaderFlagsV1 corruptedFlags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	corruptedFlags.encryptMode = EncryptCipherMode::ENCRYPT_CIPHER_MODE_LAST;
	corruptedHeaderRef.flags = corruptedFlags;
	try {
		encryptedBuf = encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);
		Reference<DecryptBlobCipher> decryptor =
		    DecryptBlobCipher::getDecryptor(tCipherKey,
		                                    Optional<Reference<BlobCipherKey>>(),
		                                    &iv[0],
		                                    EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR,
		                                    BlobCipherMetrics::TEST);
		decryptedBuf = decryptTest(decryptor, encryptedBuf.begin(), bufLen, corruptedHeaderRef, arena);
		ASSERT(false); // error expected
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_metadata_mismatch) {
			throw;
		}
		TraceEvent("TestConfigurableEncryptionCorruptEncryptModeDone").detail("Mode", authAlgoStr);
	}

	// induce encrypted buffer payload corruption
	try {
		encryptedBuf = encryptTest(encryptor, &orgData[0], bufLen, &headerRef, arena);
		uint8_t temp[bufLen];
		deterministicRandom()->randomBytes(temp, bufLen);
		Reference<DecryptBlobCipher> decryptor = DecryptBlobCipher::getDecryptor(
		    tCipherKey, Optional<Reference<BlobCipherKey>>(), &iv[0], encryptMode, BlobCipherMetrics::TEST);
		decryptedBuf = decryptTest(decryptor, temp, bufLen, headerRef, arena);
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_ops_error);
		TraceEvent("TestConfigurableEncryptionCorruptPayloadDone").detail("Mode", authAlgoStr);
	}

	// induce baseCipher payload corruption
	try {
		Reference<BlobCipherKey> corruptedCipher = corruptCipherKey(tCipherKey);
		Reference<DecryptBlobCipher> decryptor = DecryptBlobCipher::getDecryptor(
		    corruptedCipher, Optional<Reference<BlobCipherKey>>(), &iv[0], encryptMode, BlobCipherMetrics::TEST);
		decryptedBuf = decryptTest(decryptor, encryptedBuf.begin(), bufLen, headerRef, arena);
		ASSERT(false); // error expected
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_key_check_value_mismatch);
		TraceEvent("TestConfigurableEncryptionBaseCipherCorruptionDone").detail("Mode", authAlgoStr);
	}

	TraceEvent("TestSingleAuthTokenConfigurableEncryptionEnd").detail("Mode", authAlgoStr);
}

void testKeyCacheCleanup(const int minDomainId, const int maxDomainId) {
	TraceEvent("BlobCipherTestKeyCacheCleanupStart");

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
	// Validate dropping encryptDomainId cached keys
	const EncryptCipherDomainId candidate = deterministicRandom()->randomInt(minDomainId, maxDomainId);
	cipherKeyCache->resetEncryptDomainId(candidate);
	std::vector<Reference<BlobCipherKey>> cachedKeys = cipherKeyCache->getAllCiphers(candidate);
	ASSERT(cachedKeys.empty());

	// Validate dropping all cached cipherKeys
	cipherKeyCache->cleanup();
	for (int dId = minDomainId; dId < maxDomainId; dId++) {
		std::vector<Reference<BlobCipherKey>> cachedKeys = cipherKeyCache->getAllCiphers(dId);
		ASSERT(cachedKeys.empty());
	}

	TraceEvent("BlobCipherTestKeyCacheCleanupDone");
}

void testConfigurableEncryptionInvalidEncryptionKeyNoAuth(const int minDomainId) {
	TraceEvent("TestConfigurableEncryptionInvalidEncryptKeyNoAuthStart");

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	Arena arena;
	uint8_t iv[AES_256_CTR_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_CTR_IV_LENGTH);

	Reference<EncryptBlobCipher> encryptor =
	    EncryptBlobCipher::getEncryptor(cipherKey,
	                                    headerCipherKey,
	                                    iv,
	                                    AES_256_CTR_IV_LENGTH,
	                                    EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	                                    EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_NONE,
	                                    EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR,
	                                    BlobCipherMetrics::TEST);

	BlobCipherEncryptHeaderRef headerRef;
	StringRef encryptedBuf = encryptor->encrypt(&orgData[0], bufLen, &headerRef, arena);

	// Test scenario where 'encryption key' with which the data was encrypted is 'different' from the one decryption
	// gets attempted
	AesCtrNoAuth noAuth = std::get<AesCtrNoAuth>(headerRef.algoHeader);
	Reference<BlobCipherKey> tCipherKey = makeReference<BlobCipherKey>(cipherKey->getDomainId(),
	                                                                   cipherKey->getBaseCipherId(),
	                                                                   cipherKey->rawBaseCipher(),
	                                                                   cipherKey->getBaseCipherLen(),
	                                                                   cipherKey->getBaseCipherKCV(),
	                                                                   cipherKey->getRefreshAtTS(),
	                                                                   cipherKey->getExpireAtTS());
	// BlobCipherKey uses unique random salt to ensure generated encryption-keys are different
	ASSERT(!tCipherKey->isEqual(cipherKey));
	Reference<DecryptBlobCipher> decryptor =
	    DecryptBlobCipher::getDecryptor(tCipherKey,
	                                    Reference<BlobCipherKey>(),
	                                    &noAuth.v1.iv[0],
	                                    EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR,
	                                    BlobCipherMetrics::TEST);

	try {
		StringRef decryptedBuf = decryptor->decrypt(encryptedBuf.begin(), encryptedBuf.size(), headerRef, arena);
		ASSERT_EQ(decryptedBuf.size(), bufLen);
		ASSERT_NE(memcmp(decryptedBuf.begin(), &orgData[0], bufLen), 0);
	} catch (Error& e) {
		// underlying layer 'may' throw exception
		TraceEvent("InvalidEncryptKeyError").error(e);
	}

	TraceEvent("TestConfigurableEncryptionInvalidEncryptKeyNoAuthEnd");
}

template <class Params>
void testConfigurableEncryptionInvalidEncryptKeySingleAuthMode(const int minDomainId) {
	const std::string authAlgoStr = getEncryptModeStr<Params>();
	const EncryptAuthTokenAlgo authAlgo = getAuthTokenAlgo<Params>();
	const int algoHeaderVersion = getAuthHeaderVersion<Params>();
	const EncryptCipherMode encryptMode = getEncryptMode<Params>();

	TraceEvent("TestConfigurableEncryptionSingleAuthStart").detail("Mode", authAlgoStr);

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	Arena arena;
	uint8_t iv[Params::ivSize];
	deterministicRandom()->randomBytes(&iv[0], Params::ivSize);
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	Reference<EncryptBlobCipher> encryptor =
	    EncryptBlobCipher::getEncryptor(cipherKey,
	                                    headerCipherKey,
	                                    iv,
	                                    Params::ivSize,
	                                    EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE,
	                                    authAlgo,
	                                    encryptMode,
	                                    BlobCipherMetrics::TEST);
	BlobCipherEncryptHeaderRef headerRef;
	StringRef encryptedBuf = encryptor->encrypt(&orgData[0], bufLen, &headerRef, arena);

	ASSERT_EQ(encryptedBuf.size(), bufLen);
	ASSERT_NE(memcmp(&orgData[0], encryptedBuf.begin(), bufLen), 0);
	ASSERT_EQ(headerRef.flagsVersion(), CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION);
	ASSERT_EQ(headerRef.algoHeaderVersion(), algoHeaderVersion);

	// validate flags
	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	ASSERT_EQ(flags.encryptMode, encryptMode);
	ASSERT_EQ(flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	ASSERT_EQ(flags.authTokenAlgo, authAlgo);

	// validate IV
	AesWithAuth<Params> withAuth = std::get<AesWithAuth<Params>>(headerRef.algoHeader);
	ASSERT_EQ(memcmp(&iv[0], &withAuth.v1.iv[0], Params::ivSize), 0);
	ASSERT_NE(memcmp(&orgData[0], encryptedBuf.begin(), bufLen), 0);
	// validate cipherKey details
	ASSERT_EQ(withAuth.v1.cipherTextDetails.encryptDomainId, cipherKey->getDomainId());
	ASSERT_EQ(withAuth.v1.cipherTextDetails.baseCipherId, cipherKey->getBaseCipherId());
	ASSERT_EQ(withAuth.v1.cipherTextDetails.salt, cipherKey->getSalt());
	ASSERT_EQ(withAuth.v1.cipherHeaderDetails.encryptDomainId, headerCipherKey->getDomainId());
	ASSERT_EQ(withAuth.v1.cipherHeaderDetails.baseCipherId, headerCipherKey->getBaseCipherId());
	ASSERT_EQ(withAuth.v1.cipherHeaderDetails.salt, headerCipherKey->getSalt());

	Reference<BlobCipherKey> tCipherKey = cipherKeyCache->getCipherKey(withAuth.v1.cipherTextDetails.encryptDomainId,
	                                                                   withAuth.v1.cipherTextDetails.baseCipherId,
	                                                                   withAuth.v1.cipherTextDetails.salt);
	Reference<BlobCipherKey> hCipherKey = cipherKeyCache->getCipherKey(withAuth.v1.cipherHeaderDetails.encryptDomainId,
	                                                                   withAuth.v1.cipherHeaderDetails.baseCipherId,
	                                                                   withAuth.v1.cipherHeaderDetails.salt);
	ASSERT(tCipherKey->isEqual(cipherKey));
	ASSERT(hCipherKey->isEqual(headerCipherKey));
	try {

		// Switch text & header cipher keys to simulate decryption using invalid encryption keys
		Reference<DecryptBlobCipher> decryptor = DecryptBlobCipher::getDecryptor(
		    hCipherKey, tCipherKey, &withAuth.v1.iv[0], encryptMode, BlobCipherMetrics::TEST);
		decryptor->decrypt(encryptedBuf.begin(), bufLen, headerRef, arena);
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_key_check_value_mismatch);
		TraceEvent("InvalidEncryptKeyError").error(e);
	}

	TraceEvent("TestConfigurableEncryptionInvalidEncryptKeySingleAuthTokenEnd").detail("Mode", authAlgoStr);
}

void testConfigurableEncryptionInvalidEncryptKeySingleAuthModeGcm(const int minDomainId) {
	const std::string authAlgoStr = "AES-GCM";
	const EncryptAuthTokenAlgo authAlgo = EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM;
	const int algoHeaderVersion = CLIENT_KNOBS->ENCRYPT_HEADER_AES_GCM_AUTH_VERSION;
	const EncryptCipherMode encryptMode = EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM;

	TraceEvent("TestConfigurableEncryptionSingleAuthStart").detail("Mode", authAlgoStr);

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	Arena arena;
	uint8_t iv[AES_256_GCM_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_GCM_IV_LENGTH);
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	Reference<EncryptBlobCipher> encryptor =
	    EncryptBlobCipher::getEncryptor(cipherKey,
	                                    Optional<Reference<BlobCipherKey>>(),
	                                    iv,
	                                    AES_256_GCM_IV_LENGTH,
	                                    EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE,
	                                    authAlgo,
	                                    encryptMode,
	                                    BlobCipherMetrics::TEST);
	BlobCipherEncryptHeaderRef headerRef;
	StringRef encryptedBuf = encryptor->encrypt(&orgData[0], bufLen, &headerRef, arena);

	ASSERT_EQ(encryptedBuf.size(), bufLen);
	ASSERT_NE(memcmp(&orgData[0], encryptedBuf.begin(), bufLen), 0);
	ASSERT_EQ(headerRef.flagsVersion(), CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION);
	ASSERT_EQ(headerRef.algoHeaderVersion(), algoHeaderVersion);

	// validate flags
	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	ASSERT_EQ(flags.encryptMode, encryptMode);
	ASSERT_EQ(flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	ASSERT_EQ(flags.authTokenAlgo, authAlgo);

	// validate IV
	AesGcm withAuth = std::get<AesGcm>(headerRef.algoHeader);
	ASSERT_EQ(memcmp(&iv[0], &get<AesGcmV1>(withAuth.headerVersions).iv[0], AES_256_GCM_IV_LENGTH), 0);
	ASSERT_NE(memcmp(&orgData[0], encryptedBuf.begin(), bufLen), 0);
	// validate cipherKey details
	ASSERT_EQ(get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.encryptDomainId, cipherKey->getDomainId());
	ASSERT_EQ(get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.baseCipherId, cipherKey->getBaseCipherId());
	ASSERT_EQ(get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.salt, cipherKey->getSalt());

	Reference<BlobCipherKey> tCipherKey =
	    cipherKeyCache->getCipherKey(get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.encryptDomainId,
	                                 get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.baseCipherId,
	                                 get<AesGcmV1>(withAuth.headerVersions).cipherTextDetails.salt);
	ASSERT(tCipherKey->isEqual(cipherKey));
	TraceEvent("TestConfigurableEncryptionInvalidEncryptKeySingleAuthTokenEnd").detail("Mode", authAlgoStr);
}

TEST_CASE("/blobCipher") {
	DomainKeyMap domainKeyMap;
	const EncryptCipherDomainId minDomainId = 1;
	const EncryptCipherDomainId maxDomainId = deterministicRandom()->randomInt(minDomainId, minDomainId + 10) + 5;
	const EncryptCipherBaseKeyId minBaseCipherKeyId = 100;
	const EncryptCipherBaseKeyId maxBaseCipherKeyId =
	    deterministicRandom()->randomInt(minBaseCipherKeyId, minBaseCipherKeyId + 50) + 15;
	for (int dId = minDomainId; dId <= maxDomainId; dId++) {
		for (int kId = minBaseCipherKeyId; kId <= maxBaseCipherKeyId; kId++) {
			domainKeyMap[dId].emplace(
			    kId,
			    makeReference<BaseCipher>(
			        dId, kId, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max()));
		}
	}
	ASSERT_EQ(domainKeyMap.size(), maxDomainId);

	testMaxBaseCipherLen();

	testKeyCacheEssentials(domainKeyMap, minDomainId, maxDomainId, minBaseCipherKeyId);
	testKeyCacheRefreshExpireCipherKey(domainKeyMap, maxDomainId);

	testConfigurableEncryptionBlobCipherHeaderFlagsV1Ser();
	testConfigurableEncryptionAesCtrNoAuthV1Ser(minDomainId);
	testConfigurableEncryptionAesCtrWithAuthSer<AesCtrWithHmacParams>(minDomainId);
	testConfigurableEncryptionAesCtrWithAuthSer<AesCtrWithCmacParams>(minDomainId);
	testConfigurableEncryptionAesCtrWithAuthSerGcm(minDomainId);

	testConfigurableEncryptionHeaderNoAuthMode(minDomainId);
	testConfigurableEncryptionHeaderSingleAuthMode<AesCtrWithHmacParams>(minDomainId);
	testConfigurableEncryptionHeaderSingleAuthMode<AesCtrWithCmacParams>(minDomainId);
	testConfigurableEncryptionHeaderSingleAuthModeGcm(minDomainId);

	testConfigurableEncryptionNoAuthMode(minDomainId);
	testConfigurableEncryptionSingleAuthMode<AesCtrWithHmacParams>(minDomainId);
	testConfigurableEncryptionSingleAuthMode<AesCtrWithCmacParams>(minDomainId);
	testConfigurableEncryptionSingleAuthModeGcm(minDomainId);

	testConfigurableEncryptionInvalidEncryptionKeyNoAuth(minDomainId);
	testConfigurableEncryptionInvalidEncryptKeySingleAuthMode<AesCtrWithHmacParams>(minDomainId);
	testConfigurableEncryptionInvalidEncryptKeySingleAuthMode<AesCtrWithCmacParams>(minDomainId);
	testConfigurableEncryptionInvalidEncryptKeySingleAuthModeGcm(minDomainId);

	testKeyCacheCleanup(minDomainId, maxDomainId);

	return Void();
}
