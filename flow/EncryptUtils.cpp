/*
 * EncryptUtils.cpp
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

#include "flow/EncryptUtils.h"
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/Trace.h"

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

EncryptCipherMode encryptModeFromString(const std::string& modeStr) {
	if (modeStr == "NONE") {
		return ENCRYPT_CIPHER_MODE_NONE;
	} else if (modeStr == "AES-256-CTR") {
		return ENCRYPT_CIPHER_MODE_AES_256_CTR;
	} else if (modeStr == "AES-256-GCM") {
		return ENCRYPT_CIPHER_MODE_AES_256_GCM;
	} else {
		TraceEvent("EncryptModeFromString").detail("Mode", modeStr);
		throw not_implemented();
	}
}

std::string getEncryptDbgTraceKey(std::string_view prefix,
                                  EncryptCipherDomainId domainId,
                                  Optional<EncryptCipherBaseKeyId> baseCipherId) {
	// Construct the TraceEvent field key ensuring its uniqueness and compliance to TraceEvent field validator and log
	// parsing tools
	if (baseCipherId.present()) {
		boost::format fmter("%s.%lld.%llu");
		return boost::str(boost::format(fmter % prefix % domainId % baseCipherId.get()));
	} else {
		boost::format fmter("%s.%lld");
		return boost::str(boost::format(fmter % prefix % domainId));
	}
}

std::string getEncryptDbgTraceKeyWithTS(std::string_view prefix,
                                        EncryptCipherDomainId domainId,
                                        EncryptCipherBaseKeyId baseCipherId,
                                        int64_t refAfterTS,
                                        int64_t expAfterTS) {
	// Construct the TraceEvent field key ensuring its uniqueness and compliance to TraceEvent field validator and log
	// parsing tools
	boost::format fmter("%s.%lld.%llu.%lld.%lld");
	return boost::str(boost::format(fmter % prefix % domainId % baseCipherId % refAfterTS % expAfterTS));
}

int getEncryptHeaderAuthTokenSize(int algo) {
	switch (algo) {
	case ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA:
		return 32;
	case ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC:
		return 16;
	default:
		throw not_implemented();
	}
}

bool isEncryptHeaderAuthTokenAlgoValid(const EncryptAuthTokenAlgo algo) {
	return algo >= EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_NONE &&
	       algo < EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_LAST;
}

bool isEncryptHeaderAuthTokenModeValid(const EncryptAuthTokenMode mode) {
	return mode >= EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE &&
	       mode < EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_LAST;
}

bool isEncryptHeaderAuthTokenDetailsValid(const EncryptAuthTokenMode mode, const EncryptAuthTokenAlgo algo) {
	if (!isEncryptHeaderAuthTokenModeValid(mode) || !isEncryptHeaderAuthTokenAlgoValid(algo) ||
	    (mode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE &&
	     algo != EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_NONE) ||
	    (mode != EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE &&
	     algo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_NONE)) {
		return false;
	}
	return true;
}

EncryptAuthTokenMode getRandomAuthTokenMode() {
	return deterministicRandom()->coinflip() ? EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE
	                                         : EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE;
}

EncryptAuthTokenAlgo getRandomAuthTokenAlgo() {
	EncryptAuthTokenAlgo algo = deterministicRandom()->coinflip()
	                                ? EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC
	                                : EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA;

	return algo;
}

bool isReservedEncryptDomain(EncryptCipherDomainId domainId) {
	return domainId == SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID || domainId == ENCRYPT_HEADER_DOMAIN_ID ||
	       domainId == FDB_DEFAULT_ENCRYPT_DOMAIN_ID;
}

bool isEncryptHeaderDomain(EncryptCipherDomainId domainId) {
	return domainId == ENCRYPT_HEADER_DOMAIN_ID;
}

bool isCipherModeSupported(EncryptCipherMode mode) {
	return mode >= ENCRYPT_CIPHER_MODE_AES_256_CTR && mode < ENCRYPT_CIPHER_MODE_LAST;
}

EncryptionAtRestAlgo encryptionAlgoFromString(const std::string& algoStr) {
	if (algoStr == "AES-256-CTR") {
		return EncryptionAtRestAlgo::AES_CTR;
	} else if (algoStr == "AES-256-CMAC") {
		return EncryptionAtRestAlgo::AES_CTR_CMAC;
	} else if (algoStr == "AES-256-HMAC") {
		return EncryptionAtRestAlgo::AES_CTR_HMAC;
	} else if (algoStr == "AES-256-GCM") {
		return EncryptionAtRestAlgo::AES_GCM;
	}
	TraceEvent(SevWarnAlways, "UnrecognizedEncryptionAtRestAlgo").detail("AlgoStr", algoStr);
	throw not_implemented();
}

EncryptAuthTokenAlgo authTokenAlgoFromEncryptionAtRestAlgo(EncryptionAtRestAlgo algo) {
	if (algo == EncryptionAtRestAlgo::AES_CTR) {
		return EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_NONE;
	} else if (algo == EncryptionAtRestAlgo::AES_CTR_CMAC) {
		return EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC;
	} else if (algo == EncryptionAtRestAlgo::AES_CTR_HMAC) {
		return EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA;
	} else if (algo == EncryptionAtRestAlgo::AES_GCM) {
		return EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_GCM;
	}
	TraceEvent(SevWarnAlways, "UnrecognizedEncryptionAtRestAlgo").detail("Algo", algo);
	throw not_implemented();
}

EncryptCipherMode cipherModeFromEncryptionAtRestAlgo(EncryptionAtRestAlgo algo) {
	if (algo == EncryptionAtRestAlgo::AES_CTR || algo == EncryptionAtRestAlgo::AES_CTR_CMAC ||
	    algo == EncryptionAtRestAlgo::AES_CTR_HMAC) {
		return EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR;
	} else if (algo == EncryptionAtRestAlgo::AES_GCM) {
		return EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_GCM;
	}
	TraceEvent(SevWarnAlways, "UnrecognizedEncryptionAtRestAlgo").detail("Algo", algo);
	throw not_implemented();
}

EncryptAuthTokenMode authTokenModeFromEncryptionAtRestAlgo(EncryptionAtRestAlgo algo) {
	if (algo == EncryptionAtRestAlgo::AES_GCM || algo == EncryptionAtRestAlgo::AES_CTR_CMAC ||
	    algo == EncryptionAtRestAlgo::AES_CTR_HMAC) {
		return EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE;
	} else if (algo == EncryptionAtRestAlgo::AES_CTR) {
		return EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE;
	}
	TraceEvent(SevWarnAlways, "UnrecognizedEncryptionAtRestAlgo").detail("Algo", algo);
	throw not_implemented();
}

bool isAuthenticatedEncryption(EncryptionAtRestAlgo algo) {
	if (algo == EncryptionAtRestAlgo::AES_GCM || algo == EncryptionAtRestAlgo::AES_CTR_CMAC ||
	    algo == EncryptionAtRestAlgo::AES_CTR_HMAC) {
		return true;
	} else if (algo == EncryptionAtRestAlgo::AES_CTR) {
		return false;
	}
	TraceEvent(SevWarnAlways, "UnrecognizedEncryptionAtRestAlgo").detail("Algo", algo);
	throw not_implemented();
}

bool needsHeaderCipherKey(EncryptionAtRestAlgo algo) {
	return isAuthenticatedEncryption(algo) && algo != EncryptionAtRestAlgo::AES_GCM;
}

std::string getRandomAuthenticatedEncryptionAtRestAlgo() {
	std::vector<std::string> encryptionAlgos = { "AES-256-CMAC", "AES-256-HMAC", "AES-256-GCM" };
	// Test GCM with a higher probability (90% of the time)
	if (deterministicRandom()->random01() < 0.9) {
		return "AES-256-GCM";
	} else if (deterministicRandom()->coinflip()) {
		return "AES-256-CMAC";
	}
	return "AES-256-HMAC";
}