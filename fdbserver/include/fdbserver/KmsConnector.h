/*
 * KmsConnector.h
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

#ifndef FDBSERVER_KMS_CONNECTOR_H
#define FDBSERVER_KMS_CONNECTOR_H
#pragma once

#include "fdbclient/BlobMetadataUtils.h"

#include "flow/EncryptUtils.h"

#include <any>

struct KmsConnLookupKeyIdsInfo {
	// Encryption at-rest relies on partioning database to define encryption domains, one such possible domain is
	// "Tenants". It is possible that KMS assigned 'baseCipherId' embedded 'encryption domain information', hence, make
	// the field optional.
	Optional<EncryptCipherDomainId> domainId;
	EncryptCipherBaseKeyId baseCipherId;

	KmsConnLookupKeyIdsInfo() : domainId(INVALID_ENCRYPT_DOMAIN_ID), baseCipherId(INVALID_ENCRYPT_CIPHER_KEY_ID) {}
	explicit KmsConnLookupKeyIdsInfo(const Optional<EncryptCipherDomainId> dId, const EncryptCipherBaseKeyId bCId)
	  : domainId(dId),
	    baseCipherId(bCId) {}

	bool operator==(const KmsConnLookupKeyIdsInfo& info) const {
		if (domainId.present()) {
			if (!info.domainId.present() || domainId.get() != info.domainId.get()) {
				return false;
			}
		}
		return baseCipherId == info.baseCipherId;
	}
};

struct KmsCipherKeyDetailsRef {
	EncryptCipherDomainId encryptDomainId;
	EncryptCipherBaseKeyId encryptKeyId;
	StringRef encryptKey;
	EncryptCipherKeyCheckValue encryptKCV;
	Optional<int64_t> refreshAfterSec;
	Optional<int64_t> expireAfterSec;

	KmsCipherKeyDetailsRef()
	  : encryptDomainId(INVALID_ENCRYPT_DOMAIN_ID),
	    encryptKeyId(INVALID_ENCRYPT_CIPHER_KEY_ID),
	    encryptKey(StringRef()) {}
	explicit KmsCipherKeyDetailsRef(Arena& arena,
	                                EncryptCipherDomainId dId,
	                                EncryptCipherBaseKeyId keyId,
	                                StringRef key,
	                                EncryptCipherKeyCheckValue keyKCV)
	  : encryptDomainId(dId),
	    encryptKeyId(keyId),
	    encryptKey(StringRef(arena, key)),
	    encryptKCV(keyKCV),
	    refreshAfterSec(Optional<int64_t>()),
	    expireAfterSec(Optional<int64_t>()) {}
	explicit KmsCipherKeyDetailsRef(Arena& arena,
	                                EncryptCipherDomainId dId,
	                                EncryptCipherBaseKeyId keyId,
	                                StringRef key,
	                                EncryptCipherKeyCheckValue keyKCV,
	                                Optional<int64_t> refAfterSec,
	                                Optional<int64_t> expAfterSec)
	  : encryptDomainId(dId),
	    encryptKeyId(keyId),
	    encryptKey(StringRef(arena, key)),
	    encryptKCV(keyKCV),
	    refreshAfterSec(refAfterSec),
	    expireAfterSec(expAfterSec) {}

	bool operator==(const KmsCipherKeyDetailsRef& toCompare) {
		return encryptDomainId == toCompare.encryptDomainId && encryptKeyId == toCompare.encryptKeyId &&
		       encryptKey.compare(toCompare.encryptKey) == 0 && encryptKCV == toCompare.encryptKCV;
	}
};

// FDB encryption needs to interact with external Key Management Services (KMS) solutions to lookup/refresh encryption
// keys. KmsConnector interface is an abstract interface enabling implementing specialized KMS connector
// implementations.
// FDB KMSConnector implementation should inherit from KmsConnector and implement pure virtual function,
// EncryptKeyProxyServer instantiates desired implementation object based on SERVER_KNOB->KMS_CONNECTOR_TYPE knob.

class KmsConnector : public NonCopyable {
public:
	KmsConnector(const std::string& conStr) : connectorStr(conStr) {}
	virtual ~KmsConnector() {}

	virtual Future<Standalone<VectorRef<KmsCipherKeyDetailsRef>>> fetchEncryptionKeysByDomainIds(
	    std::any data,
	    const std::vector<EncryptCipherDomainId>& domainIds,
	    Optional<UID> debugId = Optional<UID>()) = 0;
	virtual Future<Standalone<VectorRef<KmsCipherKeyDetailsRef>>> fetchEncryptionKeysByKeyIds(
	    std::any data,
	    const std::vector<KmsConnLookupKeyIdsInfo>& encryptKeyInfos,
	    Optional<UID> debugId = Optional<UID>()) = 0;
	virtual Future<Standalone<VectorRef<BlobMetadataDetailsRef>>> fetchBlobMetadata(
	    std::any data,
	    const std::vector<EncryptCipherDomainId>& domainIds,
	    Optional<UID> debugId = Optional<UID>()) = 0;

	std::string getConnectorStr() const { return connectorStr; }

protected:
	std::string connectorStr;
};

#endif
