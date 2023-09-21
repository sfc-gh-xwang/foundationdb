/*
 * EncryptBlobCipher.h
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
#ifndef FDBCLIENT_ENCRYPT_BLOB_CIPHER_H
#define FDBCLIENT_ENCRYPT_BLOB_CIPHER_H
#pragma once

#include "fdbrpc/Stats.h"

#include "fdbclient/Knobs.h"
#include "fdbclient/BlobCipher.h"

// This interface enables data block encryption. An invocation to encrypt() will
// do two things:
// 1) generate encrypted ciphertext for given plaintext input.
// 2) generate BlobCipherEncryptHeader (including the 'header authTokens') and persit for decryption on reads.
class EncryptBlobCipher : public ReferenceCounted<EncryptBlobCipher>, NonCopyable {
public:
	static Reference<EncryptBlobCipher> getEncryptor(Reference<BlobCipherKey> tCipherKey,
	                                                 Optional<Reference<BlobCipherKey>> hCipherKey,
	                                                 EncryptionAtRestAlgo algo,
	                                                 BlobCipherMetrics::UsageType usageType,
	                                                 bool incrementCounters = true);
	static Reference<EncryptBlobCipher> getEncryptor(Reference<BlobCipherKey> tCipherKey,
	                                                 Optional<Reference<BlobCipherKey>> hCipherKey,
	                                                 const uint8_t* iv,
	                                                 const int ivLen,
	                                                 EncryptionAtRestAlgo algo,
	                                                 BlobCipherMetrics::UsageType usageType,
	                                                 bool incrementCounters = true);
	EncryptBlobCipher(Reference<BlobCipherKey> tCipherKey,
	                  Optional<Reference<BlobCipherKey>> hCipherKey,
	                  const uint8_t* iv,
	                  const int ivLen,
	                  const EncryptAuthTokenMode mode,
	                  const EncryptAuthTokenAlgo algo,
	                  const EncryptCipherMode encryptCipherMode,
	                  BlobCipherMetrics::UsageType usageType,
	                  bool incrementCounters);
	virtual ~EncryptBlobCipher();

	virtual bool supportsInplaceEncryption() = 0;

	virtual StringRef encrypt(const uint8_t*,
	                          const int,
	                          BlobCipherEncryptHeaderRef*,
	                          Arena&,
	                          double* encryptTime = nullptr) = 0;

	virtual void encryptInplace(uint8_t* plaintext,
	                            const int plaintextLen,
	                            BlobCipherEncryptHeaderRef* headerRef,
	                            double* encryptTime = nullptr) = 0;

	StringRef getIVRef() { return ivRef; }

protected:
	virtual void init() = 0;

	void updateEncryptHeader(const uint8_t*, const int, BlobCipherEncryptHeaderRef* headerRef);
	void updateEncryptHeaderFlagsV1(BlobCipherEncryptHeaderRef* headerRef, BlobCipherEncryptHeaderFlagsV1* flags);
	void setCipherAlgoHeaderV1(const uint8_t*,
	                           const int,
	                           const BlobCipherEncryptHeaderFlagsV1&,
	                           BlobCipherEncryptHeaderRef*);
	void setCipherAlgoHeaderNoAuthV1(const BlobCipherEncryptHeaderFlagsV1&, BlobCipherEncryptHeaderRef*);
	virtual void setCipherAlgoHeaderWithAuthV1(const uint8_t*,
	                                           const int,
	                                           const BlobCipherEncryptHeaderFlagsV1&,
	                                           BlobCipherEncryptHeaderRef*,
	                                           const EncryptAuthTokenAlgo) = 0;

	EVP_CIPHER_CTX* ctx;
	Reference<BlobCipherKey> textCipherKey;
	Optional<Reference<BlobCipherKey>> headerCipherKeyOpt;
	EncryptAuthTokenMode authTokenMode;
	EncryptAuthTokenAlgo authTokenAlgo;
	EncryptCipherMode encryptMode;
	Standalone<StringRef> ivRef;
	BlobCipherMetrics::UsageType usageType;
	bool incrementCounters;
};

class EncryptBlobCipherAes256Ctr final : public EncryptBlobCipher {
public:
	EncryptBlobCipherAes256Ctr(Reference<BlobCipherKey> tCipherKey,
	                           Optional<Reference<BlobCipherKey>> hCipherKey,
	                           const uint8_t* iv,
	                           const int ivLen,
	                           const EncryptAuthTokenMode mode,
	                           const EncryptAuthTokenAlgo algo,
	                           const EncryptCipherMode encryptCipherMode,
	                           BlobCipherMetrics::UsageType usageType,
	                           bool incrementCounters);

	// TODO: Think about how header version evolution will work
	static constexpr uint8_t ENCRYPT_HEADER_VERSION = 1;

	StringRef encrypt(const uint8_t*,
	                  const int,
	                  BlobCipherEncryptHeaderRef*,
	                  Arena&,
	                  double* encryptTime = nullptr) override;

	bool supportsInplaceEncryption() override;

	void encryptInplace(uint8_t* plaintext,
	                    const int plaintextLen,
	                    BlobCipherEncryptHeaderRef* headerRef,
	                    double* encryptTime = nullptr) override;

private:
	template <class Params>
	void setCipherAlgoHeaderWithAuthV1(const uint8_t*,
	                                   const int,
	                                   const BlobCipherEncryptHeaderFlagsV1&,
	                                   BlobCipherEncryptHeaderRef*);

	void setCipherAlgoHeaderWithAuthV1(const uint8_t*,
	                                   const int,
	                                   const BlobCipherEncryptHeaderFlagsV1&,
	                                   BlobCipherEncryptHeaderRef*,
	                                   const EncryptAuthTokenAlgo) override;
	void init() override;
};

class EncryptBlobCipherAes256Gcm final : public EncryptBlobCipher {
public:
	EncryptBlobCipherAes256Gcm(Reference<BlobCipherKey> tCipherKey,
	                           Optional<Reference<BlobCipherKey>> hCipherKey,
	                           const uint8_t* iv,
	                           const int ivLen,
	                           const EncryptAuthTokenMode mode,
	                           const EncryptAuthTokenAlgo algo,
	                           const EncryptCipherMode encryptCipherMode,
	                           BlobCipherMetrics::UsageType usageType,
	                           bool incrementCounters);

	// TODO: Think about how header version evolution will work
	static constexpr uint8_t ENCRYPT_HEADER_VERSION = 1;

	StringRef encrypt(const uint8_t*,
	                  const int,
	                  BlobCipherEncryptHeaderRef*,
	                  Arena&,
	                  double* encryptTime = nullptr) override;

	bool supportsInplaceEncryption() override;

	void encryptInplace(uint8_t* plaintext,
	                    const int plaintextLen,
	                    BlobCipherEncryptHeaderRef* headerRef,
	                    double* encryptTime = nullptr) override;

private:
	void setCipherAlgoHeaderWithAuthV1(const uint8_t*,
	                                   const int,
	                                   const BlobCipherEncryptHeaderFlagsV1&,
	                                   BlobCipherEncryptHeaderRef*,
	                                   const EncryptAuthTokenAlgo) override;
	void init() override;
};

// This interface enable data block decryption. An invocation to decrypt() would generate
// 'plaintext' for a given 'ciphertext' input, the caller needs to supply BlobCipherEncryptHeader.

class DecryptBlobCipher : public ReferenceCounted<DecryptBlobCipher>, NonCopyable {
public:
	static Reference<DecryptBlobCipher> getDecryptor(Reference<BlobCipherKey> tCipherKey,
	                                                 Optional<Reference<BlobCipherKey>> hCipherKey,
	                                                 const uint8_t* iv,
	                                                 const EncryptionAtRestAlgo encryptAlgo,
	                                                 BlobCipherMetrics::UsageType usageType,
	                                                 bool incrementCounters = true);

	DecryptBlobCipher(Reference<BlobCipherKey> tCipherKey,
	                  Optional<Reference<BlobCipherKey>> hCipherKey,
	                  const uint8_t* iv,
	                  BlobCipherMetrics::UsageType usageType,
	                  bool incrementCounters);
	virtual ~DecryptBlobCipher();

	virtual StringRef decrypt(const uint8_t* ciphertext,
	                          const int ciphertextLen,
	                          const BlobCipherEncryptHeaderRef& headerRef,
	                          Arena&,
	                          double* decryptTime = nullptr) = 0;

	virtual void decryptInplace(uint8_t* ciphertext,
	                            const int ciphertextLen,
	                            const BlobCipherEncryptHeaderRef& headerRef,
	                            double* decryptTime = nullptr) = 0;

	virtual bool supportsInplaceDecryption() = 0;

protected:
	EVP_CIPHER_CTX* ctx;
	Reference<BlobCipherKey> textCipherKey;
	Optional<Reference<BlobCipherKey>> headerCipherKeyOpt;
	const uint8_t* iv;
	bool authTokensValidationDone;
	BlobCipherMetrics::UsageType usageType;
	bool incrementCounters;

	// API is resposible to validate persisted EncryptionHeader sanity, it does following checks
	// 1. Parse and validate EncryptionHeaderFlags (version compliant checks)
	// 2. Parse and validate KCVs
	// 3. Parse and validate auth-tokens if applicable.
	//
	// Routine is invoked for every decryption buffer request and is done transparently to the caller

	void validateEncryptHeader(const uint8_t*,
	                           const int,
	                           const BlobCipherEncryptHeaderRef&,
	                           EncryptAuthTokenMode*,
	                           EncryptAuthTokenAlgo*);
	virtual void validateEncryptHeaderFlagsV1(const uint32_t, const BlobCipherEncryptHeaderFlagsV1&) = 0;
	virtual void validateAuthTokensV1(const uint8_t*,
	                                  const int,
	                                  const BlobCipherEncryptHeaderFlagsV1&,
	                                  const BlobCipherEncryptHeaderRef&) = 0;
	void vaidateEncryptHeaderCipherKCVs(const BlobCipherEncryptHeaderRef&, const BlobCipherEncryptHeaderFlagsV1&);
};

class DecryptBlobCipherAes256Gcm final : public DecryptBlobCipher {
public:
	DecryptBlobCipherAes256Gcm(Reference<BlobCipherKey> tCipherKey,
	                           Optional<Reference<BlobCipherKey>> hCipherKey,
	                           const uint8_t* iv,
	                           BlobCipherMetrics::UsageType usageType,
	                           bool incrementCounters);

	StringRef decrypt(const uint8_t* ciphertext,
	                  const int ciphertextLen,
	                  const BlobCipherEncryptHeaderRef& headerRef,
	                  Arena&,
	                  double* decryptTime = nullptr) override;

	void decryptInplace(uint8_t* ciphertext,
	                    const int ciphertextLen,
	                    const BlobCipherEncryptHeaderRef& headerRef,
	                    double* decryptTime = nullptr) override;

	bool supportsInplaceDecryption() override;

private:
	void validateEncryptHeaderFlagsV1(const uint32_t, const BlobCipherEncryptHeaderFlagsV1&) override;
	void validateAuthTokensV1(const uint8_t*,
	                          const int,
	                          const BlobCipherEncryptHeaderFlagsV1&,
	                          const BlobCipherEncryptHeaderRef&) override;
};

class DecryptBlobCipherAes256Ctr final : public DecryptBlobCipher {
public:
	DecryptBlobCipherAes256Ctr(Reference<BlobCipherKey> tCipherKey,
	                           Optional<Reference<BlobCipherKey>> hCipherKey,
	                           const uint8_t* iv,
	                           BlobCipherMetrics::UsageType usageType,
	                           bool incrementCounters);

	StringRef decrypt(const uint8_t* ciphertext,
	                  const int ciphertextLen,
	                  const BlobCipherEncryptHeaderRef& headerRef,
	                  Arena&,
	                  double* decryptTime = nullptr) override;

	void decryptInplace(uint8_t* ciphertext,
	                    const int ciphertextLen,
	                    const BlobCipherEncryptHeaderRef& headerRef,
	                    double* decryptTime = nullptr) override;

	bool supportsInplaceDecryption() override;

private:
	void validateEncryptHeaderFlagsV1(const uint32_t, const BlobCipherEncryptHeaderFlagsV1&) override;
	void validateAuthTokensV1(const uint8_t*,
	                          const int,
	                          const BlobCipherEncryptHeaderFlagsV1&,
	                          const BlobCipherEncryptHeaderRef&) override;
	void validateHeaderSingleAuthTokenV1(const uint8_t*,
	                                     const int,
	                                     const BlobCipherEncryptHeaderFlagsV1&,
	                                     const BlobCipherEncryptHeaderRef&);
	template <class Params>
	void validateAuthTokenV1(const uint8_t* ciphertext,
	                         const int ciphertextLen,
	                         const BlobCipherEncryptHeaderFlagsV1&,
	                         const BlobCipherEncryptHeaderRef&);
};

class HmacSha256DigestGen final : NonCopyable {
public:
	HmacSha256DigestGen(const unsigned char* key, size_t len);
	~HmacSha256DigestGen();
	HMAC_CTX* getCtx() const { return ctx; }
	unsigned int digest(const std::vector<std::pair<const uint8_t*, size_t>>& payload,
	                    unsigned char* buf,
	                    unsigned int bufLen);

private:
	HMAC_CTX* ctx;
};

class Aes256CmacDigestGen final : NonCopyable {
public:
	Aes256CmacDigestGen(const unsigned char* key, size_t len);
	~Aes256CmacDigestGen();
	CMAC_CTX* getCtx() const { return ctx; }
	size_t digest(const std::vector<std::pair<const uint8_t*, size_t>>& payload, uint8_t* digest, int digestlen);

private:
	CMAC_CTX* ctx;
};

void computeAuthToken(const std::vector<std::pair<const uint8_t*, size_t>>& payload,
                      const uint8_t* key,
                      const int keyLen,
                      unsigned char* digestBuf,
                      const EncryptAuthTokenAlgo algo,
                      unsigned int digestMaxBufSz);

EncryptAuthTokenMode getEncryptAuthTokenMode(const EncryptAuthTokenMode mode);
int getEncryptCurrentAlgoHeaderVersion(const EncryptAuthTokenMode mode, const EncryptAuthTokenAlgo algo);
void generateIV(EncryptionAtRestAlgo algo, Reference<BlobCipherKey> textCipherKey, uint8_t* iv, int ivLen);
inline void incrementEncryptOrDecryptOps(Counter& counter);

#endif // FDBCLIENT_ENCRYPT_BLOB_CIPHER_H