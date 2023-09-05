/*
 * BlobCipher.h
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
#ifndef FDBCLIENT_BLOB_CIPHER_H
#define FDBCLIENT_BLOB_CIPHER_H
#pragma once

#include "fdbrpc/Stats.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#include "flow/FastRef.h"
#include "flow/FileIdentifier.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/Knobs.h"
#include "flow/network.h"
#include "flow/ObjectSerializer.h"
#include "flow/ObjectSerializerTraits.h"
#include "flow/Platform.h"
#include "flow/ProtocolVersion.h"
#include "flow/serialize.h"
#include "flow/Trace.h"

#include <boost/functional/hash.hpp>
#include <cinttypes>
#include <cstring>
#include <limits>
#include <memory>
#include <openssl/aes.h>
#include <openssl/cmac.h>
#include <openssl/engine.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>
#if defined(HAVE_WOLFSSL)
#include <wolfssl/options.h>
#endif

using EncryptCipherNonce = uint128_t;
static_assert(sizeof(EncryptCipherNonce) == 16, "Invalid Nonce Size");

#define AES_256_KEY_LENGTH 32
#define AES_256_CTR_IV_LENGTH 16
#define AES_256_GCM_IV_LENGTH 12
#define AES_256_GCM_TAG_LENGTH 16

constexpr const int INVALID_ENCRYPT_HEADERS_FLAG_VERSION = 0;
constexpr const int INVALID_ENCRYPT_HEADER_ALGO_HEADER_VERSION = 0;

class BlobCipherMetrics : public NonCopyable {
public:
	static BlobCipherMetrics* getInstance() {
		static BlobCipherMetrics* instance = nullptr;
		if (instance == nullptr) {
			instance = new BlobCipherMetrics;
		}
		return instance;
	}

	// Order of this enum has to match initializer of counterSets.
	enum UsageType : int {
		ALL = 0,
		TLOG,
		TLOG_POST_RESOLUTION,
		KV_MEMORY,
		KV_REDWOOD,
		BLOB_GRANULE,
		BACKUP,
		RESTORE,
		TEST,
		MAX,
	};

	static const std::unordered_map<int, std::string> usageTypeNames;

private:
	BlobCipherMetrics();

	CounterCollection cc;
	Future<Void> traceFuture;

public:
	Counter cipherKeyCacheHit;
	Counter cipherKeyCacheMiss;
	Counter cipherKeyCacheExpired;
	Counter latestCipherKeyCacheHit;
	Counter latestCipherKeyCacheMiss;
	Counter latestCipherKeyCacheNeedsRefresh;
	Counter blobMetadataCacheHit;
	Counter blobMetadataCacheMiss;
	Counter blobMetadataNeedsRefresh;
	LatencySample getBlobMetadataLatency;
	LatencySample getCipherKeysLatency;
	LatencySample getLatestCipherKeysLatency;
	ActiveCounter<int> outstandingCipherKeyRequests;
	ActiveCounter<int> outstandingLatestCipherKeysRequests;
	ActiveCounter<int> outstandingBlobMetadataRequests;
};

std::string toString(BlobCipherMetrics::UsageType type);

// Encryption operations buffer management
// Approach limits number of copies needed during encryption or decryption operations.
// For encryption EncryptBuf is allocated using client supplied Arena and provided to AES library to capture
// the ciphertext. Similarly, on decryption EncryptBuf is allocated using client supplied Arena and provided
// to the AES library to capture decipher text and passed back to the clients. Given the object passed around
// is reference-counted, it gets freed once refrenceCount goes to 0.

class EncryptBuf : public ReferenceCounted<EncryptBuf>, NonCopyable {
public:
	EncryptBuf(int size, Arena& arena) : allocSize(size), logicalSize(size) {
		if (size > 0) {
			buffer = new (arena) uint8_t[size]();
		} else {
			buffer = nullptr;
		}
	}

	int getLogicalSize() { return logicalSize; }
	void setLogicalSize(int value) {
		ASSERT(value <= allocSize);
		logicalSize = value;
	}
	uint8_t* begin() { return buffer; }

	StringRef toStringRef() { return StringRef(buffer, logicalSize); }

private:
	int allocSize;
	int logicalSize;
	uint8_t* buffer;
};

class BlobCipherKey;

#pragma pack(push, 1) // exact fit - no padding
struct BlobCipherDetails {
	constexpr static FileIdentifier file_identifier = 1945731;

	// Encryption domain boundary identifier.
	EncryptCipherDomainId encryptDomainId = INVALID_ENCRYPT_DOMAIN_ID;
	// BaseCipher encryption key identifier
	EncryptCipherBaseKeyId baseCipherId = INVALID_ENCRYPT_CIPHER_KEY_ID;
	// Random salt
	EncryptCipherRandomSalt salt = INVALID_ENCRYPT_RANDOM_SALT;

	static uint32_t getSize() {
		return sizeof(EncryptCipherDomainId) + sizeof(EncryptCipherBaseKeyId) + sizeof(EncryptCipherRandomSalt);
	}

	BlobCipherDetails() {}
	BlobCipherDetails(const EncryptCipherDomainId& dId,
	                  const EncryptCipherBaseKeyId& bId,
	                  const EncryptCipherRandomSalt& random)
	  : encryptDomainId(dId),
	    baseCipherId(bId),
	    salt(random) {}

	void validateCipherDetailsWithCipherKey(Reference<BlobCipherKey> headerCipherKey);

	bool operator==(const BlobCipherDetails& o) const {
		return encryptDomainId == o.encryptDomainId && baseCipherId == o.baseCipherId && salt == o.salt;
	}
	bool operator!=(const BlobCipherDetails& o) const { return !(*this == o); }

	bool isValid() const {
		return this->encryptDomainId != INVALID_ENCRYPT_DOMAIN_ID &&
		       this->baseCipherId != INVALID_ENCRYPT_CIPHER_KEY_ID && this->salt != INVALID_ENCRYPT_RANDOM_SALT;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainId, baseCipherId, salt);
	}
};
#pragma pack(pop)

namespace std {
template <>
struct hash<BlobCipherDetails> {
	std::size_t operator()(BlobCipherDetails const& details) const {
		std::size_t seed = 0;
		boost::hash_combine(seed, std::hash<EncryptCipherDomainId>{}(details.encryptDomainId));
		boost::hash_combine(seed, std::hash<EncryptCipherBaseKeyId>{}(details.baseCipherId));
		boost::hash_combine(seed, std::hash<EncryptCipherRandomSalt>{}(details.salt));
		return seed;
	}
};
} // namespace std

struct EncryptHeaderCipherDetails {
	BlobCipherDetails textCipherDetails;
	Optional<BlobCipherDetails> headerCipherDetails;

	EncryptHeaderCipherDetails() = default;
	EncryptHeaderCipherDetails(const BlobCipherDetails& tCipherDetails) : textCipherDetails(tCipherDetails) {}
	EncryptHeaderCipherDetails(const BlobCipherDetails& tCipherDetails, const BlobCipherDetails& hCipherDetails)
	  : textCipherDetails(tCipherDetails),
	    headerCipherDetails(hCipherDetails) {}
};

#pragma pack(push, 1) // exact fit - no padding

// Why BinarySerialization instead of ObjectSerialization?
//
// By running experiments comparing: BlobCipherEncryptHeaderFlagsV1 and AesCtr algorithms header (with/without
// authentication encryption), below table summarizes on-disk storage penalty (bytes) due to storing
// BlobCipherEncryptHeader using both formats:
//
//  ----------------------------------------------------------------------------------------------------------
// |         S.No      |   ObjFlags  |  BinaryFlags |  ObjectAlgo  | BinaryAlgo | TotalObject  | TotalBinary  |
// | ----------------- | ----------- | ------------ |  ----------- | ---------- | ------------ | ------------ |
// |   AesCtrNoAuth    |     40      |     3        |       104    |      40    |     208      |     46       |
// |   AesCtrHmacSha   |     40      |     3        |       184    |      96    |     288      |    102       |
// |   AesCtrAesCmac   |     40      |     3        |       168    |      80    |     272      |     86       |
//  ----------------------------------------------------------------------------------------------------------

struct BlobCipherEncryptHeaderFlagsV1 {
	// Serializable fields

	uint8_t encryptMode;
	uint8_t authTokenMode;
	uint8_t authTokenAlgo;

	BlobCipherEncryptHeaderFlagsV1() {}
	BlobCipherEncryptHeaderFlagsV1(const EncryptCipherMode& cipherMode,
	                               const EncryptAuthTokenMode& tokenMode,
	                               const EncryptAuthTokenAlgo& authAlgo)
	  : encryptMode(cipherMode),
	    authTokenMode(tokenMode),
	    authTokenAlgo(authAlgo) {}

	bool operator==(const BlobCipherEncryptHeaderFlagsV1& o) const {
		return encryptMode == o.encryptMode && authTokenMode == o.authTokenMode && authTokenAlgo == o.authTokenAlgo;
	}

	static Standalone<StringRef> toStringRef(const BlobCipherEncryptHeaderFlagsV1& flags, Arena& arena) {
		BinaryWriter wr(AssumeVersion(ProtocolVersion::withEncryptionAtRest()));
		wr.serializeBytes(&flags, sizeof(BlobCipherEncryptHeaderFlagsV1));
		return wr.toValue(arena);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptMode, authTokenMode, authTokenAlgo);
	}
};

// Encryption header is stored as plaintext on a persistent storage to assist reconstruction of cipher-key(s)
// for reads. FIPS compliance recommendation is to leverage cryptographic digest mechanism to generate
// 'authentication token' (crypto-secure) to protect against malicious tampering and/or bit rot/flip scenarios.
//
// Encryption header support two modes of generation 'authentication tokens':
// 1) SingleAuthTokenMode: the scheme generates single crypto-secrure auth token to protect {cipherText +
// header} payload. Scheme is geared towards optimizing cost due to crypto-secure auth-token generation,
// however, on decryption client needs to be read 'header' + 'encrypted-buffer' to validate the 'auth-token'.
// The scheme is ideal for usecases where payload represented by the encryptionHeader is not large and it is
// desirable to minimize CPU/latency penalty due to crypto-secure ops, such as: CommitProxies encrypted inline
// transactions, StorageServer encrypting pages etc.
// SOMEDAY: Another potential scheme could be 'MultiAuthTokenMode': Scheme generates separate authTokens
// for 'encrypted buffer' & 'encryption-header'. The scheme is ideal where payload represented by
// encryptionHeader is large enough such that it is desirable to optimize cost of upfront reading full
// 'encrypted buffer', compared to reading only encryptionHeader and ensuring its sanity; for instance:
// backup-files.

template <class Params>
struct AesWithAuthV1 {
	using Self = AesWithAuthV1<Params>;

	// Serializable fields

	// Text cipher encryption information
	BlobCipherDetails cipherTextDetails;
	// Text cipher Key Check Value
	EncryptCipherKeyCheckValue textKCV;
	// Header cipher encryption information
	BlobCipherDetails cipherHeaderDetails;
	// Header cipher Key Check Value
	EncryptCipherKeyCheckValue headerKCV;
	// Initialization vector
	uint8_t iv[Params::ivSize];
	// Authentication token
	uint8_t authToken[Params::authTokenSize];

	AesWithAuthV1() = default;
	AesWithAuthV1(const BlobCipherDetails& textDetails,
	              const EncryptCipherKeyCheckValue tKCV,
	              const BlobCipherDetails& headerDetails,
	              const EncryptCipherKeyCheckValue hKCV,
	              const uint8_t* ivBuf,
	              const int ivLen)
	  : cipherTextDetails(textDetails),
	    textKCV(tKCV),
	    cipherHeaderDetails(headerDetails),
	    headerKCV(hKCV) {
		ASSERT_EQ(ivLen, Params::ivSize);
		memcpy(&iv[0], ivBuf, ivLen);
		memset(&authToken[0], 0, Params::authTokenSize);
	}

	bool operator==(const Self& o) const {
		return cipherHeaderDetails == o.cipherHeaderDetails && cipherTextDetails == o.cipherTextDetails &&
		       textKCV == o.textKCV && headerKCV == o.headerKCV && memcmp(&iv[0], &o.iv[0], Params::ivSize) == 0 &&
		       memcmp(&authToken[0], &o.authToken[0], Params::authTokenSize) == 0;
	}

	static uint32_t getSize() {
		return BlobCipherDetails::getSize() * 2 + sizeof(EncryptCipherKeyCheckValue) * 2 + Params::ivSize +
		       Params::authTokenSize;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, cipherTextDetails, textKCV, cipherHeaderDetails, headerKCV);
		ar.serializeBytes(iv, Params::ivSize);
		ar.serializeBytes(authToken, Params::authTokenSize);
	}
};

struct AesGcmParams {
	static constexpr int authTokenSize = AES_256_GCM_TAG_LENGTH;
	static constexpr int ivSize = AES_256_GCM_IV_LENGTH;

	static std::string authAlgoName() { return "Gcm"; }
	static uint8_t getDefaultHeaderVersion() { return CLIENT_KNOBS->ENCRYPT_HEADER_AES_GCM_AUTH_VERSION; }
};

struct AesGcmV1 {
	using Self = AesGcmV1;

	// Serializable fields

	// Text cipher encryption information
	BlobCipherDetails cipherTextDetails;
	// Text cipher Key Check Value
	EncryptCipherKeyCheckValue textKCV;
	// Initialization vector
	uint8_t iv[AesGcmParams::ivSize];
	// Authentication token
	uint8_t authToken[AesGcmParams::authTokenSize];

	AesGcmV1() = default;
	AesGcmV1(const BlobCipherDetails& textDetails,
	         const EncryptCipherKeyCheckValue tKCV,
	         const uint8_t* ivBuf,
	         const int ivLen)
	  : cipherTextDetails(textDetails),
	    textKCV(tKCV) {
		ASSERT_EQ(ivLen, AesGcmParams::ivSize);
		memcpy(&iv[0], ivBuf, ivLen);
		memset(&authToken[0], 0, AesGcmParams::authTokenSize);
	}

	bool operator==(const Self& o) const {
		return cipherTextDetails == o.cipherTextDetails && textKCV == o.textKCV &&
		       memcmp(&iv[0], &o.iv[0], AesGcmParams::ivSize) == 0 &&
		       memcmp(&authToken[0], &o.authToken[0], AesGcmParams::authTokenSize) == 0;
	}

	static uint32_t getSize() {
		return BlobCipherDetails::getSize() + sizeof(EncryptCipherKeyCheckValue) + AesGcmParams::ivSize +
		       AesGcmParams::authTokenSize;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, cipherTextDetails, textKCV);
		ar.serializeBytes(iv, AesGcmParams::ivSize);
		ar.serializeBytes(authToken, AesGcmParams::authTokenSize);
	}
};

struct AesGcm {
	// Serializable fields

	// Algorithm header version
	uint8_t version = 1;
	// List of supported versions.
	std::variant<AesGcmV1> headerVersions;

	AesGcm() { ASSERT_EQ(1, AesGcmParams::getDefaultHeaderVersion()); }

	AesGcm(AesGcmV1& v) : headerVersions(v) {
		// Only V1 is supported
		ASSERT_EQ(1, AesGcmParams::getDefaultHeaderVersion());
	}

	static uint32_t getSize() {
		// 1 byte of std::variant index
		return AesGcmV1::getSize() + 1 + 1;
	}

	static Standalone<StringRef> toStringRef(const AesGcm& algoHeader) {
		BinaryWriter wr(AssumeVersion(ProtocolVersion::withEncryptionAtRest()));
		wr << algoHeader;
		return wr.toValue();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		if (ar.isSerializing) {
			ASSERT_EQ(1, version);
		}
		serializer(ar, version);
		if (ar.isDeserializing && version != 1) {
			TraceEvent(SevWarn, "BlobCipherEncryptHeaderUnsupportedAlgoHeaderVersion")
			    .detail("HeaderType", AesGcmParams::authAlgoName())
			    .detail("Version", version);
			throw not_implemented();
		}
		serializer(ar, headerVersions);
	}
};

template <class Params>
struct AesWithAuth {
	// Serializable fields

	// Algorithm header version
	uint8_t version = 1;
	// List of supported versions.
	union {
		AesWithAuthV1<Params> v1;
	};

	AesWithAuth() {
		// Only V1 is supported
		ASSERT_EQ(1, Params::getDefaultHeaderVersion());
	}

	AesWithAuth(AesWithAuthV1<Params>& v) : v1(v) {
		// Only V1 is supported
		ASSERT_EQ(1, Params::getDefaultHeaderVersion());
	}

	static uint32_t getSize() { return AesWithAuthV1<Params>::getSize() + 1; }

	static Standalone<StringRef> toStringRef(const AesWithAuth<Params>& algoHeader) {
		BinaryWriter wr(AssumeVersion(ProtocolVersion::withEncryptionAtRest()));
		wr << algoHeader;
		return wr.toValue();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		if (ar.isSerializing) {
			ASSERT_EQ(1, version);
		}
		serializer(ar, version);
		if (ar.isDeserializing && version != 1) {
			TraceEvent(SevWarn, "BlobCipherEncryptHeaderUnsupportedAlgoHeaderVersion")
			    .detail("HeaderType", "AesCtrWith" + Params::authAlgoName())
			    .detail("Version", version);
			throw not_implemented();
		}
		serializer(ar, v1);
	}
};

struct AesCtrWithHmacParams {
	static constexpr int authTokenSize = AUTH_TOKEN_HMAC_SHA_SIZE;
	static constexpr int ivSize = AES_256_CTR_IV_LENGTH;

	static std::string authAlgoName() { return "Hmac"; }
	static uint8_t getDefaultHeaderVersion() { return CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_HMAC_SHA_AUTH_VERSION; }
};
using AesCtrWithHmac = AesWithAuth<AesCtrWithHmacParams>;

struct AesCtrWithCmacParams {
	static constexpr int authTokenSize = AUTH_TOKEN_AES_CMAC_SIZE;
	static constexpr int ivSize = AES_256_CTR_IV_LENGTH;

	static std::string authAlgoName() { return "Cmac"; }
	static uint8_t getDefaultHeaderVersion() { return CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_AES_CMAC_AUTH_VERSION; }
};
using AesCtrWithCmac = AesWithAuth<AesCtrWithCmacParams>;

struct AesCtrNoAuthV1 {
	// Serializable fields

	// Text cipher encryption information
	BlobCipherDetails cipherTextDetails;
	// Text cipher Key Check Value
	EncryptCipherKeyCheckValue textKCV;
	// Initialization vector
	uint8_t iv[AES_256_CTR_IV_LENGTH];

	AesCtrNoAuthV1() = default;
	AesCtrNoAuthV1(const BlobCipherDetails& textDetails,
	               const EncryptCipherKeyCheckValue tKCV,
	               const uint8_t* ivBuf,
	               const int ivLen)
	  : cipherTextDetails(textDetails),
	    textKCV(tKCV) {
		ASSERT_EQ(ivLen, AES_256_CTR_IV_LENGTH);
		memcpy(&iv[0], ivBuf, ivLen);
	}

	bool operator==(const AesCtrNoAuthV1& o) const {
		return cipherTextDetails == o.cipherTextDetails && textKCV == o.textKCV &&
		       memcmp(&iv[0], &o.iv[0], AES_256_CTR_IV_LENGTH) == 0;
	}

	static uint32_t getSize() {
		return BlobCipherDetails::getSize() + sizeof(EncryptCipherKeyCheckValue) + AES_256_CTR_IV_LENGTH;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, cipherTextDetails, textKCV);
		ar.serializeBytes(iv, AES_256_CTR_IV_LENGTH);
	}
};

struct AesCtrNoAuth {
	// Serializable fields

	// Algorithm header version
	uint8_t version = 1;
	// List of supported versions.
	union {
		AesCtrNoAuthV1 v1;
	};

	AesCtrNoAuth() {
		// Only V1 is supported
		ASSERT_EQ(1, CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_NO_AUTH_VERSION);
	}

	AesCtrNoAuth(AesCtrNoAuthV1& v) : v1(v) {
		// Only V1 is supported
		ASSERT_EQ(1, CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_NO_AUTH_VERSION);
	}

	static uint32_t getSize() { return AesCtrNoAuthV1::getSize() + 1; }

	static Standalone<StringRef> toStringRef(const AesCtrNoAuth& algoHeader) {
		BinaryWriter wr(AssumeVersion(ProtocolVersion::withEncryptionAtRest()));
		wr << algoHeader;
		return wr.toValue();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		if (ar.isSerializing) {
			ASSERT_EQ(1, version);
		}
		serializer(ar, version);
		if (ar.isDeserializing && version != 1) {
			TraceEvent(SevWarn, "BlobCipherEncryptHeaderUnsupportedAlgoHeaderVersion")
			    .detail("HeaderType", "AesCtrNoAuth")
			    .detail("Version", version);
			throw not_implemented();
		}
		serializer(ar, v1);
	}
};

struct EncryptHeaderCipherKCVs {
	EncryptCipherKeyCheckValue textKCV;
	Optional<EncryptCipherKeyCheckValue> headerKCV;

	EncryptHeaderCipherKCVs() = default;
	EncryptHeaderCipherKCVs(const EncryptCipherKeyCheckValue tKCV) : textKCV(tKCV) {}
	EncryptHeaderCipherKCVs(const EncryptCipherKeyCheckValue tKCV, const EncryptCipherKeyCheckValue hKCV)
	  : textKCV(tKCV),
	    headerKCV(hKCV) {}
};

struct BlobCipherEncryptHeaderRef {
	// Currently Redwood cannot exceed this header size for configurable encryption
	static constexpr int maxHeaderSize = 112;
	// Serializable fields
	std::variant<BlobCipherEncryptHeaderFlagsV1> flags;
	std::variant<AesCtrNoAuth, AesCtrWithHmac, AesCtrWithCmac, AesGcm> algoHeader;

	BlobCipherEncryptHeaderRef() = default;
	BlobCipherEncryptHeaderRef(const BlobCipherEncryptHeaderRef& src) = default;

	static BlobCipherEncryptHeaderRef fromStringRef(const StringRef& header) {
		return BinaryReader::fromStringRef<BlobCipherEncryptHeaderRef>(
		    header, AssumeVersion(ProtocolVersion::withEncryptionAtRest()));
	}

	static Standalone<StringRef> toStringRef(const BlobCipherEncryptHeaderRef& headerRef) {
		return BinaryWriter::toValue(headerRef, AssumeVersion(ProtocolVersion::withEncryptionAtRest()));
	}

	// Routine computes EncryptHeaderSize based on input params.
	static uint32_t getHeaderSize(const int flagVersion,
	                              const int algoHeaderVersion,
	                              const EncryptCipherMode cipherMode,
	                              const EncryptAuthTokenMode authMode,
	                              const EncryptAuthTokenAlgo authAlgo);

	int flagsVersion() const { return flags.index() + 1; }

	int algoHeaderVersion() const {
		return std::visit([&](auto& h) { return h.version; }, algoHeader);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, flags, algoHeader);
	}

	const uint8_t* getIV() const;
	int getIVSize() const;
	const EncryptHeaderCipherDetails getCipherDetails() const;
	EncryptAuthTokenMode getAuthTokenMode() const;
	EncryptCipherMode getEncryptMode() const;
	EncryptCipherDomainId getDomainId() const;
	EncryptHeaderCipherKCVs getKCVs() const;
	const uint8_t* getAuthToken() const;
	EncryptionAtRestAlgo getEncryptionAtRestAlgo() const;

	// API supports following validation:
	// 1. Ensure input BlobCipherDetails (textDetails and/or headerDetails) matches with the input
	// 2. Ensure persited KCV matches with the input values
	//
	// Currently API is used by BlobGranule encryption where encryption key lookup based on persisted
	// BlobGranuleCipherKeyCtx
	void validateEncryptionHeaderDetails(const BlobCipherDetails& textCipherDetails,
	                                     const BlobCipherDetails& headerCipherDetails,
	                                     const EncryptHeaderCipherKCVs& kcvs,
	                                     const StringRef& ivRef) const;
};

#pragma pack(pop)

// This interface is in-memory representation of CipherKey used for encryption/decryption information.
// It caches base encryption key properties as well as caches the 'derived encryption' key obtained by applying
// HMAC-SHA-256 derivation technique.

class BlobCipherKey : public ReferenceCounted<BlobCipherKey>, NonCopyable {
public:
	BlobCipherKey(const EncryptCipherDomainId& domainId,
	              const EncryptCipherBaseKeyId& baseCiphId,
	              const uint8_t* baseCiph,
	              const int baseCiphLen,
	              const EncryptCipherKeyCheckValue baseCipherKCV,
	              const int64_t refreshAt,
	              int64_t expireAt);
	BlobCipherKey(const EncryptCipherDomainId& domainId,
	              const EncryptCipherBaseKeyId& baseCiphId,
	              const uint8_t* baseCiph,
	              const int baseCiphLen,
	              const EncryptCipherKeyCheckValue baseCipherKCV,
	              const EncryptCipherRandomSalt& salt,
	              const int64_t refreshAt,
	              const int64_t expireAt);

	void updateAccessAtTS() const {
		ASSERT(now() >= accessAtTS);
		accessAtTS = now();
	}

	uint8_t* data() const {
		updateAccessAtTS();
		return cipher.get();
	}
	uint8_t* rawCipher() const {
		updateAccessAtTS();
		return cipher.get();
	}
	uint8_t* rawBaseCipher() const {
		updateAccessAtTS();
		return baseCipher.get();
	}

	uint64_t getRefreshAtTS() const { return refreshAtTS; }
	uint64_t getExpireAtTS() const { return expireAtTS; }
	double getAccessAtTS() const { return accessAtTS; }
	EncryptCipherDomainId getDomainId() const { return encryptDomainId; }
	EncryptCipherRandomSalt getSalt() const { return randomSalt; }
	EncryptCipherBaseKeyId getBaseCipherId() const { return baseCipherId; }
	int getBaseCipherLen() const { return baseCipherLen; }
	EncryptCipherKeyCheckValue getBaseCipherKCV() const { return baseCipherKCV; }

	bool isEqual(const Reference<BlobCipherKey> toCompare) {
		return encryptDomainId == toCompare->getDomainId() && baseCipherId == toCompare->getBaseCipherId() &&
		       randomSalt == toCompare->getSalt() && baseCipherLen == toCompare->getBaseCipherLen() &&
		       memcmp(cipher.get(), toCompare->rawCipher(), AES_256_KEY_LENGTH) == 0 &&
		       memcmp(baseCipher.get(), toCompare->rawBaseCipher(), baseCipherLen) == 0;
	}
	EncryptCipherNonce getAndUpdateNonce(EncryptCipherNonce max_value);
	EncryptCipherNonce getNonce() { return nonce; }
	// The nonce should not be set outside of testing
	void setNonce(EncryptCipherNonce other) {
		ASSERT(g_network->isSimulated());
		this->nonce = other;
	}

	bool isCipherDetailMatches(const BlobCipherDetails& details) {
		return encryptDomainId == details.encryptDomainId && baseCipherId == details.baseCipherId &&
		       randomSalt == details.salt;
	}

	inline bool needsRefresh() {
		if (refreshAtTS == std::numeric_limits<int64_t>::max()) {
			return false;
		}
		return now() + INetwork::TIME_EPS >= refreshAtTS ? true : false;
	}

	inline bool isExpired() {
		if (expireAtTS == std::numeric_limits<int64_t>::max()) {
			return false;
		}
		return now() + INetwork::TIME_EPS >= expireAtTS ? true : false;
	}

	BlobCipherDetails details() const { return BlobCipherDetails{ encryptDomainId, baseCipherId, randomSalt }; }

	void reset();

private:
	// Encryption domain boundary identifier
	EncryptCipherDomainId encryptDomainId;
	// Base encryption cipher key properties
	std::unique_ptr<uint8_t[]> baseCipher;
	EncryptCipherKeyCheckValue baseCipherKCV;
	int baseCipherLen;
	EncryptCipherBaseKeyId baseCipherId;
	// Random salt used for encryption cipher key derivation
	EncryptCipherRandomSalt randomSalt;
	// Derived encryption cipher key
	std::unique_ptr<uint8_t[]> cipher;
	// CipherKey needs refreshAt
	int64_t refreshAtTS;
	// CipherKey is valid until
	int64_t expireAtTS;
	EncryptCipherNonce nonce;
	// Latest access timestamp
	mutable double accessAtTS;

	void initKey(const EncryptCipherDomainId& domainId,
	             const EncryptCipherBaseKeyId& baseCiphId,
	             const uint8_t* baseCiph,
	             const int baseCiphLen,
	             const EncryptCipherKeyCheckValue baseCipherKCV,
	             const EncryptCipherRandomSalt& salt,
	             const int64_t refreshAt,
	             const int64_t expireAt);
	void applyHmacSha256Derivation();
};

// This interface allows FDB processes participating in encryption to store and
// index recently used encyption cipher keys. FDB encryption has two dimensions:
// 1. Mapping on cipher encryption keys per "encryption domains"
// 2. Per encryption domain, the cipher keys are index using {baseCipherKeyId, salt} tuple.
//
// The design supports NIST recommendation of limiting lifetime of an encryption
// key. For details refer to:
// https://csrc.nist.gov/publications/detail/sp/800-57-part-1/rev-3/archive/2012-07-10
//
// Below gives a pictoral representation of in-memory datastructure implemented
// to index encryption keys:
//                  { encryptionDomain -> { {baseCipherId, salt} -> cipherKey } }
//
// Supported cache lookups schemes:
// 1. Lookup cipher based on { encryptionDomainId, baseCipherKeyId, salt } triplet.
// 2. Lookup latest cipher key for a given encryptionDomainId.
//
// Client is responsible to handle cache-miss usecase, the corrective operation
// might vary based on the calling process, for instance: EncryptKeyServer
// cache-miss shall invoke RPC to external Encryption Key Manager to fetch the
// required encryption key, however, CPs/SSs cache-miss would result in RPC to
// EncryptKeyServer to refresh the desired encryption key.

using BlobCipherKeyIdCacheKey = std::pair<EncryptCipherBaseKeyId, EncryptCipherRandomSalt>;
using BlobCipherKeyIdCacheKeyHash = boost::hash<BlobCipherKeyIdCacheKey>;
using BlobCipherKeyIdCacheMap =
    std::unordered_map<BlobCipherKeyIdCacheKey, Reference<BlobCipherKey>, BlobCipherKeyIdCacheKeyHash>;
using BlobCipherKeyIdCacheMapCItr =
    std::unordered_map<BlobCipherKeyIdCacheKey, Reference<BlobCipherKey>, BlobCipherKeyIdCacheKeyHash>::const_iterator;

struct BlobCipherKeyIdCache : ReferenceCounted<BlobCipherKeyIdCache> {
public:
	explicit BlobCipherKeyIdCache(EncryptCipherDomainId dId, size_t* sizeStat);

	BlobCipherKeyIdCacheKey getCacheKey(const EncryptCipherBaseKeyId& baseCipherId,
	                                    const EncryptCipherRandomSalt& salt);

	// API returns the last inserted cipherKey.
	// If none exists, null reference is returned.

	Reference<BlobCipherKey> getLatestCipherKey();

	// API returns cipherKey corresponding to input 'baseCipherKeyId'.
	// If none exists, null reference is returned.

	Reference<BlobCipherKey> getCipherByBaseCipherId(const EncryptCipherBaseKeyId& baseCipherKeyId,
	                                                 const EncryptCipherRandomSalt& salt);

	// API enables inserting base encryption cipher details to the BlobCipherKeyIdCache.
	// Given cipherKeys are immutable, attempting to re-insert same 'identical' cipherKey
	// is treated as a NOP (success), however, an attempt to update cipherKey would throw
	// 'encrypt_update_cipher' exception. Returns the inserted cipher key if success.
	//
	// API NOTE: Recommended usecase is to update encryption cipher-key is updated the external
	// keyManagementSolution to limit an encryption key lifetime

	Reference<BlobCipherKey> insertBaseCipherKey(const EncryptCipherBaseKeyId& baseCipherId,
	                                             const uint8_t* baseCipher,
	                                             const int baseCipherLen,
	                                             const EncryptCipherKeyCheckValue baseCipherKCV,
	                                             const int64_t refreshAt,
	                                             const int64_t expireAt);

	// API enables inserting base encryption cipher details to the BlobCipherKeyIdCache
	// Given cipherKeys are immutable, attempting to re-insert same 'identical' cipherKey
	// is treated as a NOP (success), however, an attempt to update cipherKey would throw
	// 'encrypt_update_cipher' exception. Returns the inserted cipher key if sucess.
	//
	// API NOTE: Recommended usecase is to update encryption cipher-key regeneration while performing
	// decryption. The encryptionheader would contain relevant details including: 'encryptDomainId',
	// 'baseCipherId' & 'salt'. The caller needs to fetch 'baseCipherKey' detail and re-populate KeyCache.
	// Also, the invocation will NOT update the latest cipher-key details.

	Reference<BlobCipherKey> insertBaseCipherKey(const EncryptCipherBaseKeyId& baseCipherId,
	                                             const uint8_t* baseCipher,
	                                             const int baseCipherLen,
	                                             const EncryptCipherKeyCheckValue baseCipherKCV,
	                                             const EncryptCipherRandomSalt& salt,
	                                             const int64_t refreshAt,
	                                             const int64_t expireAt);

	// API cleanup the cache by dropping all cached cipherKeys
	void cleanup();

	// API returns list of all 'cached' cipherKeys
	std::vector<Reference<BlobCipherKey>> getAllCipherKeys();

	// Return number of cipher keys in the cahce.
	size_t getSize() const { return keyIdCache.size(); }

private:
	EncryptCipherDomainId domainId;
	BlobCipherKeyIdCacheMap keyIdCache;
	Optional<EncryptCipherBaseKeyId> latestBaseCipherKeyId;
	Optional<EncryptCipherRandomSalt> latestRandomSalt;
	size_t* sizeStat; // pointer to the outer BlobCipherKeyCache size count.
};

using BlobCipherDomainCacheMap = std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKeyIdCache>>;

class BlobCipherKeyCache : NonCopyable, public ReferenceCounted<BlobCipherKeyCache> {
public:
	// Public visibility constructior ONLY to assist FlowSingleton instance creation.
	// API Note: Constructor is expected to be instantiated only in simulation mode.

	explicit BlobCipherKeyCache(bool ignored) { ASSERT(g_network->isSimulated()); }

	int getAndUpdateNonce(Reference<BlobCipherKey> key);

	// Enable clients to insert base encryption cipher details to the BlobCipherKeyCache.
	// The cipherKeys are indexed using 'baseCipherId', given cipherKeys are immutable,
	// attempting to re-insert same 'identical' cipherKey is treated as a NOP (success),
	// however, an attempt to update cipherKey would throw 'encrypt_update_cipher' exception.
	// Returns the inserted cipher key if success.
	//
	// API NOTE: Recommended use case is to update encryption cipher-key is updated the external
	// keyManagementSolution to limit an encryption key lifetime

	Reference<BlobCipherKey> insertCipherKey(const EncryptCipherDomainId& domainId,
	                                         const EncryptCipherBaseKeyId& baseCipherId,
	                                         const uint8_t* baseCipher,
	                                         const int baseCipherLen,
	                                         const EncryptCipherKeyCheckValue baseCipherKCV,
	                                         const int64_t refreshAt,
	                                         const int64_t expireAt);

	// Enable clients to insert base encryption cipher details to the BlobCipherKeyCache.
	// The cipherKeys are indexed using 'baseCipherId', given cipherKeys are immutable,
	// attempting to re-insert same 'identical' cipherKey is treated as a NOP (success),
	// however, an attempt to update cipherKey would throw 'encrypt_update_cipher' exception.
	// Returns the inserted cipher key if success.
	//
	// API NOTE: Recommended usecase is to update encryption cipher-key regeneration while performing
	// decryption. The encryptionheader would contain relevant details including: 'encryptDomainId',
	// 'baseCipherId' & 'salt'. The caller needs to fetch 'baseCipherKey' detail and re-populate KeyCache.
	// Also, the invocation will NOT update the latest cipher-key details.

	Reference<BlobCipherKey> insertCipherKey(const EncryptCipherDomainId& domainId,
	                                         const EncryptCipherBaseKeyId& baseCipherId,
	                                         const uint8_t* baseCipher,
	                                         const int baseCipherLen,
	                                         const EncryptCipherKeyCheckValue baseCipherKCV,
	                                         const EncryptCipherRandomSalt& salt,
	                                         const int64_t refreshAt,
	                                         const int64_t expireAt);

	// API returns the last insert cipherKey for a given encryption domain Id.
	// If domain Id is invalid, it would throw 'encrypt_invalid_id' exception,
	// otherwise, and if none exists, it would return null reference.

	Reference<BlobCipherKey> getLatestCipherKey(const EncryptCipherDomainId& domainId);

	// API returns cipherKey corresponding to {encryptionDomainId, baseCipherId} tuple.
	// If none exists, it would return null reference.

	Reference<BlobCipherKey> getCipherKey(const EncryptCipherDomainId& domainId,
	                                      const EncryptCipherBaseKeyId& baseCipherId,
	                                      const EncryptCipherRandomSalt& salt);

	// API returns point in time list of all 'cached' cipherKeys for a given encryption domainId.
	std::vector<Reference<BlobCipherKey>> getAllCiphers(const EncryptCipherDomainId& domainId);

	// API returns all current latest cipher keys.
	std::map<EncryptCipherDomainId, Reference<BlobCipherKey>> listLatestCipherKeys() const;

	// API enables dropping all 'cached' cipherKeys for a given encryption domain Id.
	// Useful to cleanup cache if an encryption domain gets removed/destroyed etc.
	void resetEncryptDomainId(const EncryptCipherDomainId domainId);

	// Total number of cipher keys in the cache.
	size_t getSize() const { return size; }

	static Reference<BlobCipherKeyCache> getInstance() {
		static bool cleanupRegistered = false;
		if (!cleanupRegistered) {
			// We try to avoid cipher keys appear in core dumps, so we clean them up before crash.
			// TODO(yiwu): use of MADV_DONTDUMP instead of the crash handler.
			registerCrashHandlerCallback(BlobCipherKeyCache::cleanup);
			cleanupRegistered = true;
		}
		if (g_network->isSimulated()) {
			return FlowSingleton<BlobCipherKeyCache>::getInstance(
			    []() { return makeReference<BlobCipherKeyCache>(g_network->isSimulated()); });
		} else {
			static BlobCipherKeyCache instance;
			return Reference<BlobCipherKeyCache>::addRef(&instance);
		}
	}

	// Ensures cached encryption key(s) (plaintext) never gets persisted as part
	// of FDB process/core dump.
	static void cleanup() noexcept;

private:
	BlobCipherDomainCacheMap domainCacheMap;
	size_t size = 0;

	BlobCipherKeyCache() {}
};

class Sha256KCV final : NonCopyable {
public:
	Sha256KCV();
	~Sha256KCV();

	EncryptCipherKeyCheckValue computeKCV(const uint8_t* cipher, const int len);

	static void checkEqual(const Reference<BlobCipherKey>& cipher, const EncryptCipherKeyCheckValue persited);

private:
	EVP_MD_CTX* ctx;
};

int getIVLengthFromEncryptionAtRestAlgo(EncryptionAtRestAlgo algo);

void generateCryptoSecureRandomBytes(unsigned char* buffer, int size);

#endif // FDBCLIENT_BLOB_CIPHER_H