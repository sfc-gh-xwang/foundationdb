/*
 * EncryptBlobCipher.cpp
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

#include "fdbclient/EncryptBlobCipher.h"
#include "fdbclient/BlobCipher.h"
#include "flow/EncryptUtils.h"
#include "flow/Knobs.h"
#include "flow/network.h"

// EncryptBlobCipher class methods
EncryptBlobCipher::EncryptBlobCipher(Reference<BlobCipherKey> tCipherKey,
                                     Optional<Reference<BlobCipherKey>> hCipherKey,
                                     const uint8_t* iv,
                                     const int ivLen,
                                     const EncryptAuthTokenMode mode,
                                     const EncryptAuthTokenAlgo algo,
                                     const EncryptCipherMode encryptCipherMode,
                                     BlobCipherMetrics::UsageType usageType)
  : ctx(EVP_CIPHER_CTX_new()),
    textCipherKey(tCipherKey),
    headerCipherKeyOpt(hCipherKey),
    authTokenMode(mode),
    authTokenAlgo(algo),
    encryptMode(encryptCipherMode) {
	if (encryptCipherMode == EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR) {
		ASSERT_EQ(ivLen, AES_256_CTR_IV_LENGTH);
	} else if (encryptCipherMode == EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM) {
		ASSERT_EQ(ivLen, AES_256_GCM_IV_LENGTH);
	} else {
		throw not_implemented();
	}
	this->ivRef = Standalone<StringRef>(StringRef(iv, ivLen));
}

EncryptBlobCipher::~EncryptBlobCipher() {
	if (ctx != nullptr) {
		EVP_CIPHER_CTX_free(ctx);
	}
}

Reference<EncryptBlobCipher> EncryptBlobCipher::getEncryptor(Reference<BlobCipherKey> tCipherKey,
                                                             Optional<Reference<BlobCipherKey>> hCipherKey,
                                                             EncryptionAtRestAlgo algo,
                                                             BlobCipherMetrics::UsageType usageType) {
	EncryptCipherMode encryptCipherMode = cipherModeFromEncryptionAtRestAlgo(algo);
	EncryptAuthTokenMode mode = authTokenModeFromEncryptionAtRestAlgo(algo);
	EncryptAuthTokenAlgo authAlgo = authTokenAlgoFromEncryptionAtRestAlgo(algo);
	int ivSize = getIVLengthFromEncryptionAtRestAlgo(algo);
	Standalone<StringRef> iv = makeString(ivSize);
	generateIV(algo, tCipherKey, mutateString(iv), iv.size());
	if (encryptCipherMode == EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR) {
		return makeReference<EncryptBlobCipherAes256Ctr>(
		    tCipherKey, hCipherKey, iv.begin(), iv.size(), mode, authAlgo, encryptCipherMode, usageType);
	} else if (encryptCipherMode == EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM) {
		return makeReference<EncryptBlobCipherAes256Gcm>(
		    tCipherKey, hCipherKey, iv.begin(), iv.size(), mode, authAlgo, encryptCipherMode, usageType);
	}
	// Currently only AES-CTR is supported
	throw not_implemented();
}

Reference<EncryptBlobCipher> EncryptBlobCipher::getEncryptor(Reference<BlobCipherKey> tCipherKey,
                                                             Optional<Reference<BlobCipherKey>> hCipherKey,
                                                             const uint8_t* iv,
                                                             const int ivLen,
                                                             EncryptionAtRestAlgo algo,
                                                             BlobCipherMetrics::UsageType usageType) {
	EncryptCipherMode encryptCipherMode = cipherModeFromEncryptionAtRestAlgo(algo);
	EncryptAuthTokenMode mode = authTokenModeFromEncryptionAtRestAlgo(algo);
	EncryptAuthTokenAlgo authAlgo = authTokenAlgoFromEncryptionAtRestAlgo(algo);
	if (encryptCipherMode == EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR) {
		return makeReference<EncryptBlobCipherAes256Ctr>(
		    tCipherKey, hCipherKey, iv, ivLen, mode, authAlgo, encryptCipherMode, usageType);
	} else if (encryptCipherMode == EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM) {
		return makeReference<EncryptBlobCipherAes256Gcm>(
		    tCipherKey, hCipherKey, iv, ivLen, mode, authAlgo, encryptCipherMode, usageType);
	}
	// Currently only AES-CTR is supported
	throw not_implemented();
}

void EncryptBlobCipher::setCipherAlgoHeaderNoAuthV1(const BlobCipherEncryptHeaderFlagsV1& flags,
                                                    BlobCipherEncryptHeaderRef* headerRef) {
	ASSERT_EQ(flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE);

	AesCtrNoAuthV1 aesCtrNoAuth(
	    BlobCipherDetails(textCipherKey->getDomainId(), textCipherKey->getBaseCipherId(), textCipherKey->getSalt()),
	    textCipherKey->getBaseCipherKCV(),
	    ivRef.begin(),
	    ivRef.size());
	headerRef->algoHeader = AesCtrNoAuth(aesCtrNoAuth);
}

void EncryptBlobCipher::setCipherAlgoHeaderV1(const uint8_t* ciphertext,
                                              const int ciphertextLen,
                                              const BlobCipherEncryptHeaderFlagsV1& flags,
                                              BlobCipherEncryptHeaderRef* headerRef) {
	ASSERT_EQ(1,
	          getEncryptCurrentAlgoHeaderVersion((EncryptAuthTokenMode)flags.authTokenMode,
	                                             (EncryptAuthTokenAlgo)flags.authTokenAlgo));

	if (flags.authTokenMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		// GCM should always be authenticated
		ASSERT_NE(flags.encryptMode, EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM);
		setCipherAlgoHeaderNoAuthV1(flags, headerRef);
	} else {
		ASSERT(headerCipherKeyOpt.present() && headerCipherKeyOpt.get().isValid());
		setCipherAlgoHeaderWithAuthV1(
		    ciphertext, ciphertextLen, flags, headerRef, (EncryptAuthTokenAlgo)flags.authTokenAlgo);
	}
}

void EncryptBlobCipher::updateEncryptHeaderFlagsV1(BlobCipherEncryptHeaderRef* headerRef,
                                                   BlobCipherEncryptHeaderFlagsV1* flags) {

	// Populate encryption header flags details
	flags->encryptMode = encryptMode;
	flags->authTokenMode = authTokenMode;
	flags->authTokenAlgo = authTokenAlgo;
	headerRef->flags = *flags;
}

void EncryptBlobCipher::updateEncryptHeader(const uint8_t* ciphertext,
                                            const int ciphertextLen,
                                            BlobCipherEncryptHeaderRef* headerRef) {
	ASSERT_LE(CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION, std::numeric_limits<uint8_t>::max());
	ASSERT_EQ(1, CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION);

	// update header flags
	BlobCipherEncryptHeaderFlagsV1 flags;
	updateEncryptHeaderFlagsV1(headerRef, &flags);

	// update cipher algo header
	int algoHeaderVersion = getEncryptCurrentAlgoHeaderVersion(authTokenMode, authTokenAlgo);
	ASSERT_EQ(algoHeaderVersion, 1);
	setCipherAlgoHeaderV1(ciphertext, ciphertextLen, flags, headerRef);
}

// EncryptBlobCipherAes256Gcm class methods
EncryptBlobCipherAes256Gcm::EncryptBlobCipherAes256Gcm(Reference<BlobCipherKey> tCipherKey,
                                                       Optional<Reference<BlobCipherKey>> hCipherKeyOpt,
                                                       const uint8_t* cipherIV,
                                                       const int ivLen,
                                                       const EncryptAuthTokenMode mode,
                                                       const EncryptAuthTokenAlgo algo,
                                                       const EncryptCipherMode encryptCipherMode,
                                                       BlobCipherMetrics::UsageType usageType)
  : EncryptBlobCipher(tCipherKey, hCipherKeyOpt, cipherIV, ivLen, mode, algo, encryptCipherMode, usageType) {
	ASSERT_EQ(encryptCipherMode, EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM);
	ASSERT_EQ(mode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	ASSERT_EQ(algo, EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM);
	ASSERT_EQ(ivLen, AES_256_GCM_IV_LENGTH);
	init();
}

void EncryptBlobCipherAes256Gcm::init() {
	ASSERT(textCipherKey.isValid());
	// GCM is authenticated encryption
	ASSERT(headerCipherKeyOpt.present() && headerCipherKeyOpt.get().isValid());
	if (!isEncryptHeaderAuthTokenDetailsValid(authTokenMode, authTokenAlgo) ||
	    (authTokenMode != EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE) ||
	    (authTokenAlgo != EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM)) {
		TraceEvent(SevWarn, "InvalidAuthTokenDetails")
		    .detail("TokenMode", authTokenMode)
		    .detail("TokenAlgo", authTokenAlgo);
		throw internal_error();
	}
	if (ctx == nullptr) {
		throw encrypt_ops_error();
	}
}

StringRef EncryptBlobCipherAes256Gcm::encrypt(const uint8_t* plaintext,
                                              const int plaintextLen,
                                              BlobCipherEncryptHeaderRef* headerRef,
                                              Arena& arena,
                                              double* encryptTime) {
	double startTime = 0.0;
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING && encryptTime) {
		startTime = timer_monotonic();
	}

	if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1) {
		throw encrypt_ops_error();
	}
	if (EVP_EncryptInit_ex(ctx, nullptr, nullptr, textCipherKey.getPtr()->data(), ivRef.begin()) != 1) {
		throw encrypt_ops_error();
	}

	StringRef encryptBuf = makeString(plaintextLen, arena);
	uint8_t* ciphertext = mutateString(encryptBuf);

	int bytes{ 0 };
	if (EVP_EncryptUpdate(ctx, ciphertext, &bytes, plaintext, plaintextLen) != 1) {
		TraceEvent(SevWarn, "BlobCipherEncryptUpdateFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	int finalBytes{ 0 };
	if (EVP_EncryptFinal_ex(ctx, ciphertext + bytes, &finalBytes) != 1) {
		TraceEvent(SevWarn, "BlobCipherEncryptFinalFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	// Padding is not needed for AES GCM mode, so EncryptUpdate() should encrypt all the data at once.
	if ((bytes + finalBytes) != plaintextLen) {
		TraceEvent(SevWarn, "BlobCipherEncryptUnexpectedCipherLen")
		    .detail("PlaintextLen", plaintextLen)
		    .detail("EncryptedBufLen", bytes + finalBytes);
		throw encrypt_ops_error();
	}

	ASSERT(isEncryptHeaderAuthTokenDetailsValid(authTokenMode, authTokenAlgo));
	updateEncryptHeader(ciphertext, plaintextLen, headerRef);
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING && encryptTime) {
		*encryptTime = timer_monotonic() - startTime;
	}

	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM,
	           "ConfigurableEncryption: Encryption with AES_GCM Auth token generation");

	return encryptBuf.substr(0, plaintextLen);
}

void EncryptBlobCipherAes256Gcm::encryptInplace(uint8_t* plaintext,
                                                const int plaintextLen,
                                                BlobCipherEncryptHeaderRef* headerRef,
                                                double* encryptTime) {
	throw not_implemented();
}

bool EncryptBlobCipherAes256Gcm::supportsInplaceEncryption() {
	// TODO: Verify if AES-GCM supports inplace encryption
	return false;
}

template <class Params>
void EncryptBlobCipherAes256Gcm::setCipherAlgoHeaderWithAuthV1(const uint8_t* ciphertext,
                                                               const int ciphertextLen,
                                                               const BlobCipherEncryptHeaderFlagsV1& flags,
                                                               BlobCipherEncryptHeaderRef* headerRef) {
	ASSERT_EQ(flags.authTokenAlgo, EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM);
	ASSERT_EQ(flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	bool isGcm = std::is_same_v<Params, AesGcmParams>;
	ASSERT(isGcm);

	AesWithAuthV1<Params> algoHeader(
	    BlobCipherDetails(textCipherKey->getDomainId(), textCipherKey->getBaseCipherId(), textCipherKey->getSalt()),
	    textCipherKey->getBaseCipherKCV(),
	    BlobCipherDetails(headerCipherKeyOpt.get()->getDomainId(),
	                      headerCipherKeyOpt.get()->getBaseCipherId(),
	                      headerCipherKeyOpt.get()->getSalt()),
	    headerCipherKeyOpt.get()->getBaseCipherKCV(),
	    ivRef.begin(),
	    ivRef.size());
	// compute tag
	if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, AES_256_GCM_TAG_LENGTH, &algoHeader.authToken[0]) != 1) {
		throw encrypt_ops_error();
	}
	headerRef->algoHeader = AesWithAuth(algoHeader);
}

void EncryptBlobCipherAes256Gcm::setCipherAlgoHeaderWithAuthV1(const uint8_t* ciphertext,
                                                               const int ciphertextLen,
                                                               const BlobCipherEncryptHeaderFlagsV1& flags,
                                                               BlobCipherEncryptHeaderRef* headerRef,
                                                               const EncryptAuthTokenAlgo authTokenAlgo) {
	ASSERT_EQ(flags.authTokenAlgo, EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM);
	ASSERT_EQ(flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	setCipherAlgoHeaderWithAuthV1<AesGcmParams>(ciphertext, ciphertextLen, flags, headerRef);
}

// EncryptBlobCipherAes256Ctr class methods
EncryptBlobCipherAes256Ctr::EncryptBlobCipherAes256Ctr(Reference<BlobCipherKey> tCipherKey,
                                                       Optional<Reference<BlobCipherKey>> hCipherKeyOpt,
                                                       const uint8_t* cipherIV,
                                                       const int ivLen,
                                                       const EncryptAuthTokenMode mode,
                                                       const EncryptAuthTokenAlgo algo,
                                                       const EncryptCipherMode encryptCipherMode,
                                                       BlobCipherMetrics::UsageType usageType)
  : EncryptBlobCipher(tCipherKey, hCipherKeyOpt, cipherIV, ivLen, mode, algo, encryptCipherMode, usageType) {
	ASSERT_EQ(ivLen, AES_256_CTR_IV_LENGTH);
	ASSERT(encryptCipherMode == EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR);
	init();
}

void EncryptBlobCipherAes256Ctr::init() {
	ASSERT(textCipherKey.isValid());
	if (isAuthenticatedEncryption(encryptionAlgoFromString(FLOW_KNOBS->ENCRYPTION_AT_REST_ALGO))) {
		ASSERT(headerCipherKeyOpt.present() && headerCipherKeyOpt.get().isValid());
	}

	if (!isEncryptHeaderAuthTokenDetailsValid(authTokenMode, authTokenAlgo)) {
		TraceEvent(SevWarn, "InvalidAuthTokenDetails")
		    .detail("TokenMode", authTokenMode)
		    .detail("TokenAlgo", authTokenAlgo);
		throw internal_error();
	}

	if (ctx == nullptr) {
		throw encrypt_ops_error();
	}
}

template <class Params>
void EncryptBlobCipherAes256Ctr::setCipherAlgoHeaderWithAuthV1(
    const uint8_t* ciphertext,
    const int ciphertextLen,
    const BlobCipherEncryptHeaderFlagsV1& flags,
    BlobCipherEncryptHeaderRef* headerRef) { // Construct algorithm specific details except 'authToken', serialize the
	                                         // details into 'headerRef' to allow
	// authToken generation
	AesWithAuthV1<Params> algoHeader(
	    BlobCipherDetails(textCipherKey->getDomainId(), textCipherKey->getBaseCipherId(), textCipherKey->getSalt()),
	    textCipherKey->getBaseCipherKCV(),
	    BlobCipherDetails(headerCipherKeyOpt.get()->getDomainId(),
	                      headerCipherKeyOpt.get()->getBaseCipherId(),
	                      headerCipherKeyOpt.get()->getSalt()),
	    headerCipherKeyOpt.get()->getBaseCipherKCV(),
	    ivRef.begin(),
	    ivRef.size());
	headerRef->algoHeader = AesWithAuth(algoHeader);
	// compute the authentication token
	Standalone<StringRef> serialized = BlobCipherEncryptHeaderRef::toStringRef(*headerRef);
	uint8_t computed[Params::authTokenSize]{
		0,
	};
	computeAuthToken({ { ciphertext, ciphertextLen }, { serialized.begin(), serialized.size() } },
	                 headerCipherKeyOpt.get()->rawCipher(),
	                 AES_256_KEY_LENGTH,
	                 &computed[0],
	                 (EncryptAuthTokenAlgo)flags.authTokenAlgo,
	                 AUTH_TOKEN_MAX_SIZE);
	memcpy(&algoHeader.authToken[0], &computed[0], Params::authTokenSize);

	// Populate headerRef algorithm specific header details
	headerRef->algoHeader = algoHeader;
}

void EncryptBlobCipherAes256Ctr::setCipherAlgoHeaderWithAuthV1(const uint8_t* ciphertext,
                                                               const int ciphertextLen,
                                                               const BlobCipherEncryptHeaderFlagsV1& flags,
                                                               BlobCipherEncryptHeaderRef* headerRef,
                                                               const EncryptAuthTokenAlgo authTokenAlgo) {
	ASSERT_EQ(flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	if (flags.authTokenAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC) {
		setCipherAlgoHeaderWithAuthV1<AesCtrWithCmacParams>(ciphertext, ciphertextLen, flags, headerRef);
	} else {
		ASSERT_EQ(flags.authTokenAlgo, ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA);
		setCipherAlgoHeaderWithAuthV1<AesCtrWithHmacParams>(ciphertext, ciphertextLen, flags, headerRef);
	}
}

StringRef EncryptBlobCipherAes256Ctr::encrypt(const uint8_t* plaintext,
                                              const int plaintextLen,
                                              BlobCipherEncryptHeaderRef* headerRef,
                                              Arena& arena,
                                              double* encryptTime) {
	double startTime = 0.0;
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING && encryptTime) {
		startTime = timer_monotonic();
	}

	if (EVP_EncryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, nullptr, nullptr) != 1) {
		throw encrypt_ops_error();
	}
	if (EVP_EncryptInit_ex(ctx, nullptr, nullptr, textCipherKey.getPtr()->data(), ivRef.begin()) != 1) {
		throw encrypt_ops_error();
	}

	StringRef encryptBuf = makeString(plaintextLen, arena);
	uint8_t* ciphertext = mutateString(encryptBuf);

	int bytes{ 0 };
	if (EVP_EncryptUpdate(ctx, ciphertext, &bytes, plaintext, plaintextLen) != 1) {
		TraceEvent(SevWarn, "BlobCipherEncryptUpdateFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	// Padding is not needed for AES CTR mode, so EncryptUpdate() should encrypt all the data at once.
	if (bytes != plaintextLen) {
		TraceEvent(SevWarn, "BlobCipherEncryptUnexpectedCipherLen")
		    .detail("PlaintextLen", plaintextLen)
		    .detail("EncryptedBufLen", bytes);
		throw encrypt_ops_error();
	}

	// EVP_CIPHER_CTX_reset(ctx) is called after EncryptUpdate() to make sure the same encryptor
	// `EncryptBlobCipherAes256Ctr` could be reused to encrypt multiple text. Otherwise,
	// ctx = EVP_CIPHER_CTX_new() is required before calling encrypt().

	// Ensure encryption header authToken details sanity
	ASSERT(isEncryptHeaderAuthTokenDetailsValid(authTokenMode, authTokenAlgo));
	updateEncryptHeader(ciphertext, plaintextLen, headerRef);
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING && encryptTime) {
		*encryptTime = timer_monotonic() - startTime;
	}

	CODE_PROBE(authTokenMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	           "ConfigurableEncryption: Encryption with Auth token generation disabled");
	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
	           "ConfigurableEncryption: Encryption with HMAC_SHA Auth token generation");
	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC,
	           "ConfigurableEncryption: Encryption with AES_CMAC Auth token generation");

	return encryptBuf.substr(0, plaintextLen);
}

void EncryptBlobCipherAes256Ctr::encryptInplace(uint8_t* plaintext,
                                                const int plaintextLen,
                                                BlobCipherEncryptHeaderRef* headerRef,
                                                double* encryptTime) {
	ASSERT(supportsInplaceEncryption());
	double startTime = 0.0;
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING && encryptTime) {
		startTime = timer_monotonic();
	}

	if (EVP_EncryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, nullptr, nullptr) != 1) {
		throw encrypt_ops_error();
	}
	if (EVP_EncryptInit_ex(ctx, nullptr, nullptr, textCipherKey.getPtr()->data(), ivRef.begin()) != 1) {
		throw encrypt_ops_error();
	}

	int bytes{ 0 };
	if (EVP_EncryptUpdate(ctx, plaintext, &bytes, plaintext, plaintextLen) != 1) {
		TraceEvent(SevWarn, "BlobCipherInplaceEncryptUpdateFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	// Padding should be 0 for AES CTR mode, so encryptUpdate() should encrypt all the data
	if (bytes != plaintextLen) {
		TraceEvent(SevWarn, "BlobCipherInplaceEncryptUnexpectedCipherLen")
		    .detail("PlaintextLen", plaintextLen)
		    .detail("EncryptedBufLen", bytes);
		throw encrypt_ops_error();
	}

	// Ensure encryption header authToken details sanity
	ASSERT(isEncryptHeaderAuthTokenDetailsValid(authTokenMode, authTokenAlgo));
	updateEncryptHeader(plaintext, plaintextLen, headerRef);
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING && encryptTime) {
		*encryptTime = timer_monotonic() - startTime;
	}

	CODE_PROBE(authTokenMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	           "encryptInplace: ConfigurableEncryption: Encryption with Auth token generation disabled");
	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
	           "encryptInplace: ConfigurableEncryption: Encryption with HMAC_SHA Auth token generation");
	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC,
	           "encryptInplace: ConfigurableEncryption: Encryption with AES_CMAC Auth token generation");
}

bool EncryptBlobCipherAes256Ctr::supportsInplaceEncryption() {
	return true;
}

// DecryptBlobCipher class methods

DecryptBlobCipher::DecryptBlobCipher(Reference<BlobCipherKey> tCipherKey,
                                     Optional<Reference<BlobCipherKey>> hCipherKeyOpt,
                                     const uint8_t* iv,
                                     BlobCipherMetrics::UsageType usageType)
  : ctx(EVP_CIPHER_CTX_new()),
    textCipherKey(tCipherKey),
    headerCipherKeyOpt(hCipherKeyOpt),
    iv(iv),
    authTokensValidationDone(false) {}

void DecryptBlobCipher::validateEncryptHeaderFlagsV1(const uint32_t headerVersion,
                                                     const BlobCipherEncryptHeaderFlagsV1& flags) {
	// validate header flag sanity
	if ((flags.encryptMode != EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR &&
	     flags.encryptMode != EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM) ||
	    !isEncryptHeaderAuthTokenModeValid((EncryptAuthTokenMode)flags.authTokenMode)) {
		TraceEvent(SevWarn, "BlobCipherVerifyEncryptBlobHeader")
		    .detail("HeaderVersion", headerVersion)
		    .detail("ExpectedVersion", CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION)
		    .detail("EncryptCipherMode", flags.encryptMode)
		    .detail("ExpectedCipherMode", EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR)
		    .detail("EncryptHeaderAuthTokenMode", flags.authTokenMode);

		CODE_PROBE(true, "ConfigurableEncryption: Encryption header metadata mismatch");

		throw encrypt_header_metadata_mismatch();
	}
}

void DecryptBlobCipher::vaidateEncryptHeaderCipherKCVs(const BlobCipherEncryptHeaderRef& headerRef,
                                                       const BlobCipherEncryptHeaderFlagsV1& flags) {
	const EncryptHeaderCipherKCVs kcvs = headerRef.getKCVs();
	Sha256KCV::checkEqual(textCipherKey, kcvs.textKCV);
	if (flags.authTokenMode != ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		if (!kcvs.headerKCV.present()) {
			TraceEvent(SevWarnAlways, "MissingHeaderKCV");
			throw encrypt_key_check_value_mismatch();
		}
		Sha256KCV::checkEqual(headerCipherKeyOpt.get(), kcvs.headerKCV.get());
	}
}

void DecryptBlobCipher::validateEncryptHeader(const uint8_t* ciphertext,
                                              const int ciphertextLen,
                                              const BlobCipherEncryptHeaderRef& headerRef,
                                              EncryptAuthTokenMode* authTokenMode,
                                              EncryptAuthTokenAlgo* authTokenAlgo) {
	// FlagsVersion is computed based on std::variant available index
	ASSERT_EQ(headerRef.flagsVersion(), 1);

	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	validateEncryptHeaderFlagsV1(headerRef.flagsVersion(), flags);
	vaidateEncryptHeaderCipherKCVs(headerRef, flags);
	validateAuthTokensV1(ciphertext, ciphertextLen, flags, headerRef);

	*authTokenMode = (EncryptAuthTokenMode)flags.authTokenMode;
	*authTokenAlgo = (EncryptAuthTokenAlgo)flags.authTokenAlgo;
}

Reference<DecryptBlobCipher> DecryptBlobCipher::getDecryptor(Reference<BlobCipherKey> tCipherKey,
                                                             Optional<Reference<BlobCipherKey>> hCipherKey,
                                                             const uint8_t* iv,
                                                             const EncryptionAtRestAlgo encryptAlgo,
                                                             BlobCipherMetrics::UsageType usageType) {
	if (encryptAlgo == EncryptionAtRestAlgo::AES_CTR || encryptAlgo == EncryptionAtRestAlgo::AES_CTR_CMAC ||
	    encryptAlgo == EncryptionAtRestAlgo::AES_CTR_HMAC) {
		return makeReference<DecryptBlobCipherAes256Ctr>(tCipherKey, hCipherKey, iv, usageType);
	} else if (encryptAlgo == EncryptionAtRestAlgo::AES_GCM) {
		return makeReference<DecryptBlobCipherAes256Gcm>(tCipherKey, hCipherKey, iv, usageType);
	} else {
		throw not_implemented();
	}
}

DecryptBlobCipher::~DecryptBlobCipher() {
	if (ctx != nullptr) {
		EVP_CIPHER_CTX_free(ctx);
	}
}

// DecryptBlobCipherAes256Gcm class methods
DecryptBlobCipherAes256Gcm::DecryptBlobCipherAes256Gcm(Reference<BlobCipherKey> tCipherKey,
                                                       Optional<Reference<BlobCipherKey>> hCipherKeyOpt,
                                                       const uint8_t* iv,
                                                       BlobCipherMetrics::UsageType usageType)
  : DecryptBlobCipher(tCipherKey, hCipherKeyOpt, iv, usageType) {
	if (ctx == nullptr) {
		throw encrypt_ops_error();
	}
}

bool DecryptBlobCipherAes256Gcm::supportsInplaceDecryption() {
	// TODO: Check if AES-GCM allows inplace decryption
	return false;
}

StringRef DecryptBlobCipherAes256Gcm::decrypt(const uint8_t* ciphertext,
                                              const int ciphertextLen,
                                              const BlobCipherEncryptHeaderRef& headerRef,
                                              Arena& arena,
                                              double* decryptTime) {
	double startTime = 0.0;
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING && decryptTime) {
		startTime = timer_monotonic();
	}

	if (!EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr)) {
		throw encrypt_ops_error();
	}
	if (!EVP_DecryptInit_ex(ctx, nullptr, nullptr, textCipherKey.getPtr()->data(), iv)) {
		throw encrypt_ops_error();
	}

	EncryptAuthTokenMode authTokenMode;
	EncryptAuthTokenAlgo authTokenAlgo;
	validateEncryptHeader(ciphertext, ciphertextLen, headerRef, &authTokenMode, &authTokenAlgo);
	ASSERT_EQ(authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	ASSERT_EQ(authTokenAlgo, EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM);

	StringRef decrypted = makeString(ciphertextLen, arena);

	uint8_t* plaintext = mutateString(decrypted);
	int bytesDecrypted{ 0 };
	if (!EVP_DecryptUpdate(ctx, plaintext, &bytesDecrypted, ciphertext, ciphertextLen)) {
		TraceEvent(SevWarn, "BlobCipherDecryptUpdateFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	uint8_t* tag = const_cast<uint8_t*>(headerRef.getAuthToken());
	ASSERT(tag != nullptr);
	if (!EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, AES_256_GCM_TAG_LENGTH, tag)) {
		TraceEvent(SevWarn, "BlobCipherDecryptTagSetFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	int finalBlobBytes{ 0 };
	if (EVP_DecryptFinal_ex(ctx, plaintext + bytesDecrypted, &finalBlobBytes) <= 0) {
		TraceEvent(SevWarn, "BlobCipherDecryptionFinalFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	if ((bytesDecrypted + finalBlobBytes) != ciphertextLen) {
		TraceEvent(SevWarn, "BlobCipherDecryptUnexpectedPlaintextLen")
		    .detail("CiphertextLen", ciphertextLen)
		    .detail("DecryptedBufLen", bytesDecrypted + finalBlobBytes);
		throw encrypt_ops_error();
	}

	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING && decryptTime) {
		*decryptTime = timer_monotonic() - startTime;
	}

	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM,
	           "ConfigurableEncryption: Decryption with AES_GCM Auth token generation");

	return decrypted.substr(0, ciphertextLen);
}

void DecryptBlobCipherAes256Gcm::decryptInplace(uint8_t* ciphertext,
                                                const int ciphertextLen,
                                                const BlobCipherEncryptHeaderRef& headerRef,
                                                double* decryptTime) {
	throw not_implemented();
}

void DecryptBlobCipherAes256Gcm::validateAuthTokensV1(const uint8_t*,
                                                      const int,
                                                      const BlobCipherEncryptHeaderFlagsV1&,
                                                      const BlobCipherEncryptHeaderRef&) {
	// GCM Auth token validation is done post-decryption
}

// DecryptBlobCipherAes256Ctr class methods

DecryptBlobCipherAes256Ctr::DecryptBlobCipherAes256Ctr(Reference<BlobCipherKey> tCipherKey,
                                                       Optional<Reference<BlobCipherKey>> hCipherKeyOpt,
                                                       const uint8_t* iv,
                                                       BlobCipherMetrics::UsageType usageType)
  : DecryptBlobCipher(tCipherKey, hCipherKeyOpt, iv, usageType) {
	if (ctx == nullptr) {
		throw encrypt_ops_error();
	}
}

bool DecryptBlobCipherAes256Ctr::supportsInplaceDecryption() {
	return true;
}

StringRef DecryptBlobCipherAes256Ctr::decrypt(const uint8_t* ciphertext,
                                              const int ciphertextLen,
                                              const BlobCipherEncryptHeaderRef& headerRef,
                                              Arena& arena,
                                              double* decryptTime) {
	double startTime = 0.0;
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING && decryptTime) {
		startTime = timer_monotonic();
	}

	if (!EVP_DecryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, nullptr, nullptr)) {
		throw encrypt_ops_error();
	}
	if (!EVP_DecryptInit_ex(ctx, nullptr, nullptr, textCipherKey.getPtr()->data(), iv)) {
		throw encrypt_ops_error();
	}

	EncryptAuthTokenMode authTokenMode;
	EncryptAuthTokenAlgo authTokenAlgo;
	validateEncryptHeader(ciphertext, ciphertextLen, headerRef, &authTokenMode, &authTokenAlgo);

	StringRef decrypted = makeString(ciphertextLen, arena);

	uint8_t* plaintext = mutateString(decrypted);
	int bytesDecrypted{ 0 };
	if (!EVP_DecryptUpdate(ctx, plaintext, &bytesDecrypted, ciphertext, ciphertextLen)) {
		TraceEvent(SevWarn, "BlobCipherDecryptUpdateFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	if (bytesDecrypted != ciphertextLen) {
		TraceEvent(SevWarn, "BlobCipherDecryptUnexpectedPlaintextLen")
		    .detail("CiphertextLen", ciphertextLen)
		    .detail("DecryptedBufLen", bytesDecrypted);
		throw encrypt_ops_error();
	}

	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING && decryptTime) {
		*decryptTime = timer_monotonic() - startTime;
	}

	CODE_PROBE(authTokenMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	           "ConfigurableEncryption: Decryption with Auth token generation disabled");
	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
	           "ConfigurableEncryption: Decryption with HMAC_SHA Auth token generation");
	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC,
	           "ConfigurableEncryption: Decryption with AES_CMAC Auth token generation");

	return decrypted.substr(0, ciphertextLen);
}

void DecryptBlobCipherAes256Ctr::decryptInplace(uint8_t* ciphertext,
                                                const int ciphertextLen,
                                                const BlobCipherEncryptHeaderRef& headerRef,
                                                double* decryptTime) {
	ASSERT(supportsInplaceDecryption());
	double startTime = 0.0;
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING && decryptTime) {
		startTime = timer_monotonic();
	}

	if (!EVP_DecryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, nullptr, nullptr)) {
		throw encrypt_ops_error();
	}
	if (!EVP_DecryptInit_ex(ctx, nullptr, nullptr, textCipherKey.getPtr()->data(), iv)) {
		throw encrypt_ops_error();
	}

	EncryptAuthTokenMode authTokenMode;
	EncryptAuthTokenAlgo authTokenAlgo;
	validateEncryptHeader(ciphertext, ciphertextLen, headerRef, &authTokenMode, &authTokenAlgo);

	int bytesDecrypted{ 0 };
	if (!EVP_DecryptUpdate(ctx, ciphertext, &bytesDecrypted, ciphertext, ciphertextLen)) {
		TraceEvent(SevWarn, "BlobCipherDecryptUpdateFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	// Padding should be 0 for AES CTR mode, so DecryptUpdate() should decrypt all the data
	if (bytesDecrypted != ciphertextLen) {
		TraceEvent(SevWarn, "BlobCipherDecryptUnexpectedPlaintextLen")
		    .detail("CiphertextLen", ciphertextLen)
		    .detail("DecryptedBufLen", bytesDecrypted);
		throw encrypt_ops_error();
	}

	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING && decryptTime) {
		*decryptTime = timer_monotonic() - startTime;
	}

	CODE_PROBE(authTokenMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	           "decryptInplace: ConfigurableEncryption: Decryption with Auth token generation disabled");
	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
	           "decryptInplace: ConfigurableEncryption: Decryption with HMAC_SHA Auth token generation");
	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC,
	           "decryptInplace: ConfigurableEncryption: Decryption with AES_CMAC Auth token generation");
}

template <class Params>
void DecryptBlobCipherAes256Ctr::validateAuthTokenV1(const uint8_t* ciphertext,
                                                     const int ciphertextLen,
                                                     const BlobCipherEncryptHeaderFlagsV1& flags,
                                                     const BlobCipherEncryptHeaderRef& headerRef) {
	ASSERT_EQ(flags.encryptMode, ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	ASSERT_LE(Params::authTokenSize, AUTH_TOKEN_MAX_SIZE);
	ASSERT(headerCipherKeyOpt.present() && headerCipherKeyOpt.get().isValid());

	Arena tmpArena;
	uint8_t persisted[Params::authTokenSize];
	uint8_t computed[Params::authTokenSize]{
		0,
	};

	// prepare the payload {cipherText + encryptionHeader}
	// ensure the 'authToken' is reset before computing the 'authentication token'
	BlobCipherEncryptHeaderRef headerRefCopy = BlobCipherEncryptHeaderRef(headerRef);

	AesWithAuth<Params> algoHeaderCopy = std::get<AesWithAuth<Params>>(headerRefCopy.algoHeader);
	// preserve the 'persisted' token for future validation before reseting the field
	memcpy(&persisted[0], &algoHeaderCopy.v1.authToken[0], Params::authTokenSize);
	memset(&algoHeaderCopy.v1.authToken[0], 0, Params::authTokenSize);

	headerRefCopy.algoHeader = algoHeaderCopy;
	Standalone<StringRef> serializedHeader = BlobCipherEncryptHeaderRef::toStringRef(headerRefCopy);
	computeAuthToken({ { ciphertext, ciphertextLen }, { serializedHeader.begin(), serializedHeader.size() } },
	                 headerCipherKeyOpt.get()->rawCipher(),
	                 AES_256_KEY_LENGTH,
	                 &computed[0],
	                 (EncryptAuthTokenAlgo)flags.authTokenAlgo,
	                 AUTH_TOKEN_MAX_SIZE);

	if (memcmp(&persisted[0], &computed[0], Params::authTokenSize) != 0) {
		// pragma: allowlist nextline secret
		TraceEvent(SevWarn, "BlobCipherVerifyEncryptBlobHeaderAuthTokenMismatch")
		    .detail("HeaderFlagsVersion", headerRef.flagsVersion())
		    .detail("HeaderMode", flags.encryptMode)
		    .detail("SingleAuthToken", StringRef(tmpArena, persisted, Params::authTokenSize))
		    .detail("ComputedSingleAuthToken", StringRef(tmpArena, computed, Params::authTokenSize));

		CODE_PROBE(flags.authTokenAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
		           "ConfigurableEncryption: AuthToken value mismatch - HMAC_SHA auth token generation");
		CODE_PROBE(flags.authTokenAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC,
		           "ConfigurableEncryption: AuthToken value mismatch - AES_CMAC auth token generation");

		throw encrypt_header_authtoken_mismatch();
	}
}

void DecryptBlobCipherAes256Ctr::validateHeaderSingleAuthTokenV1(const uint8_t* ciphertext,
                                                                 const int ciphertextLen,
                                                                 const BlobCipherEncryptHeaderFlagsV1& flags,
                                                                 const BlobCipherEncryptHeaderRef& headerRef) {
	// prepare the payload {cipherText + encryptionHeader}
	// ensure the 'authToken' is reset before computing the 'authentication token'

	if (flags.authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC) {
		validateAuthTokenV1<AesCtrWithCmacParams>(ciphertext, ciphertextLen, flags, headerRef);
	} else {
		ASSERT_EQ(flags.authTokenAlgo, EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA);
		validateAuthTokenV1<AesCtrWithHmacParams>(ciphertext, ciphertextLen, flags, headerRef);
	}
}

void DecryptBlobCipherAes256Ctr::validateAuthTokensV1(const uint8_t* ciphertext,
                                                      const int ciphertextLen,
                                                      const BlobCipherEncryptHeaderFlagsV1& flags,
                                                      const BlobCipherEncryptHeaderRef& headerRef) {
	if (flags.authTokenMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		// No embedded 'authToken'; do nothing
		return;
	}

	ASSERT_EQ(flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	validateHeaderSingleAuthTokenV1(ciphertext, ciphertextLen, flags, headerRef);
	authTokensValidationDone = true;
}

// HmacSha256DigestGen class methods

HmacSha256DigestGen::HmacSha256DigestGen(const unsigned char* key, size_t len) : ctx(HMAC_CTX_new()) {
	if (!HMAC_Init_ex(ctx, key, len, EVP_sha256(), nullptr)) {
		throw encrypt_ops_error();
	}
}

HmacSha256DigestGen::~HmacSha256DigestGen() {
	if (ctx != nullptr) {
		HMAC_CTX_free(ctx);
	}
}

unsigned int HmacSha256DigestGen::digest(const std::vector<std::pair<const uint8_t*, size_t>>& payload,
                                         unsigned char* buf,
                                         unsigned int bufLen) {
	ASSERT_EQ(bufLen, HMAC_size(ctx));

	for (const auto& p : payload) {
		if (HMAC_Update(ctx, p.first, p.second) != 1) {
			throw encrypt_ops_error();
		}
	}

	unsigned int digestLen = 0;
	if (HMAC_Final(ctx, buf, &digestLen) != 1) {
		throw encrypt_ops_error();
	}

	CODE_PROBE(true, "HMAC_SHA Digest generation");

	return digestLen;
}

// Aes256CtrCmacDigestGen methods
Aes256CmacDigestGen::Aes256CmacDigestGen(const unsigned char* key, size_t keylen) : ctx(CMAC_CTX_new()) {
	ASSERT_EQ(keylen, AES_256_KEY_LENGTH);

	if (ctx == nullptr) {
		throw encrypt_ops_error();
	}
	if (!CMAC_Init(ctx, key, keylen, EVP_aes_256_cbc(), NULL)) {
		throw encrypt_ops_error();
	}
}

size_t Aes256CmacDigestGen::digest(const std::vector<std::pair<const uint8_t*, size_t>>& payload,
                                   uint8_t* digest,
                                   int digestlen) {
	ASSERT(ctx != nullptr);
	ASSERT_GE(digestlen, AUTH_TOKEN_AES_CMAC_SIZE);

	for (const auto& p : payload) {
		if (!CMAC_Update(ctx, p.first, p.second)) {
			throw encrypt_ops_error();
		}
	}
	size_t ret;
	if (!CMAC_Final(ctx, digest, &ret)) {
		throw encrypt_ops_error();
	}

	return ret;
}

Aes256CmacDigestGen::~Aes256CmacDigestGen() {
	if (ctx != nullptr) {
		CMAC_CTX_free(ctx);
	}
}

void computeAuthToken(const std::vector<std::pair<const uint8_t*, size_t>>& payload,
                      const uint8_t* key,
                      const int keyLen,
                      unsigned char* digestBuf,
                      const EncryptAuthTokenAlgo algo,
                      unsigned int digestBufMaxSz) {
	ASSERT_EQ(digestBufMaxSz, AUTH_TOKEN_MAX_SIZE);
	ASSERT_EQ(keyLen, AES_256_KEY_LENGTH);
	ASSERT(isEncryptHeaderAuthTokenAlgoValid(algo));

	int authTokenSz = getEncryptHeaderAuthTokenSize(algo);
	ASSERT_LE(authTokenSz, AUTH_TOKEN_MAX_SIZE);

	if (algo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA) {
		ASSERT_EQ(authTokenSz, AUTH_TOKEN_HMAC_SHA_SIZE);

		HmacSha256DigestGen hmacGenerator(key, keyLen);
		unsigned int digestLen = hmacGenerator.digest(payload, digestBuf, authTokenSz);

		ASSERT_EQ(digestLen, authTokenSz);
	} else if (algo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC) {
		ASSERT_EQ(authTokenSz, AUTH_TOKEN_AES_CMAC_SIZE);

		Aes256CmacDigestGen cmacGenerator(key, keyLen);
		size_t digestLen = cmacGenerator.digest(payload, digestBuf, authTokenSz);

		ASSERT_EQ(digestLen, authTokenSz);
	} else {
		throw not_implemented();
	}
}

EncryptAuthTokenMode getEncryptAuthTokenMode(const EncryptAuthTokenMode mode) {
	// Override mode if authToken isn't enabled
	return isAuthenticatedEncryption(encryptionAlgoFromString(FLOW_KNOBS->ENCRYPTION_AT_REST_ALGO))
	           ? mode
	           : EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE;
}

int getEncryptCurrentAlgoHeaderVersion(const EncryptAuthTokenMode mode, const EncryptAuthTokenAlgo algo) {
	if (mode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		return CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_NO_AUTH_VERSION;
	} else {
		ASSERT_EQ(mode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
		if (algo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC) {
			return CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_AES_CMAC_AUTH_VERSION;
		} else if (algo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM) {
			return CLIENT_KNOBS->ENCRYPT_HEADER_AES_GCM_AUTH_VERSION;
		} else {
			ASSERT_EQ(algo, ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA);
			return CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_HMAC_SHA_AUTH_VERSION;
		}
	}
}

void generateIV(EncryptionAtRestAlgo algo, Reference<BlobCipherKey> textCipherKey, uint8_t* iv, int ivLen) {
	EncryptCipherNonce maxValue = 0;
	if (algo == EncryptionAtRestAlgo::AES_CTR || algo == EncryptionAtRestAlgo::AES_CTR_CMAC ||
	    algo == EncryptionAtRestAlgo::AES_CTR_HMAC) {
		ASSERT_EQ(ivLen, AES_256_CTR_IV_LENGTH);
		maxValue = std::numeric_limits<uint128_t>::max();
	} else if (algo == EncryptionAtRestAlgo::AES_GCM) {
		ASSERT_EQ(ivLen, AES_256_GCM_IV_LENGTH);
		maxValue = std::numeric_limits<uint96_t>::max();
	} else {
		throw not_implemented();
	}
	EncryptCipherNonce nonce = textCipherKey->getAndUpdateNonce(maxValue);
	ASSERT(nonce <= maxValue);
	memcpy(iv, &nonce, ivLen);
}