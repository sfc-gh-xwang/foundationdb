/*
 * RESTKmsConnectorUtils.cpp
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

#include "fdbserver/RESTKmsConnectorUtils.h"
#include "fdbclient/RESTUtils.h"
#include "fdbrpc/HTTP.h"
#include "flow/EncryptUtils.h"

#include <boost/algorithm/string/trim.hpp>

#include "flow/actorcompiler.h" // This must be the last #include

namespace RESTKmsConnectorUtils {
const char* HTTP_CONTENT_TYPE = "application/json";
const char* HTTP_ACCEPT = "application/json";

const char* BASE_CIPHER_ID_TAG = "base_cipher_id";
const char* BASE_CIPHER_TAG = "base_cipher";
const char* CIPHER_KEY_DETAILS_TAG = "cipher_key_details";
const char* ENCRYPT_DOMAIN_ID_TAG = "encrypt_domain_id";
const char* REFRESH_AFTER_SEC = "refresh_after_sec";
const char* EXPIRE_AFTER_SEC = "expire_after_sec";
const char* ERROR_TAG = "error";
const char* ERROR_MSG_TAG = "err_msg";
const char* ERROR_CODE_TAG = "err_code";
const char* KMS_URLS_TAG = "kms_urls";
const char* REFRESH_KMS_URLS_TAG = "refresh_kms_urls";
const char* REQUEST_VERSION_TAG = "version";
const char* VALIDATION_TOKENS_TAG = "validation_tokens";
const char* VALIDATION_TOKEN_NAME_TAG = "token_name";
const char* VALIDATION_TOKEN_VALUE_TAG = "token_value";
const char* DEBUG_UID_TAG = "debug_uid";

const char* TOKEN_NAME_FILE_SEP = "$";
const char* TOKEN_TUPLE_SEP = ",";
const char DISCOVER_URL_FILE_URL_SEP = '\n';

const char* BLOB_METADATA_DETAILS_TAG = "blob_metadata_details";
const char* BLOB_METADATA_DOMAIN_ID_TAG = "domain_id";
const char* BLOB_METADATA_LOCATIONS_TAG = "locations";
const char* BLOB_METADATA_LOCATION_ID_TAG = "id";
const char* BLOB_METADATA_LOCATION_PATH_TAG = "path";

const int INVALID_REQUEST_VERSION = 0;

void addVersionToDoc(rapidjson::Document& doc, const int requestVersion) {
	rapidjson::Value version;
	version.SetInt(requestVersion);

	rapidjson::Value versionKey(REQUEST_VERSION_TAG, doc.GetAllocator());
	doc.AddMember(versionKey, version, doc.GetAllocator());
}

void addLatestDomainDetailsToDoc(rapidjson::Document& doc,
                                 const char* rootTagName,
                                 const char* idTagName,
                                 const EncryptCipherDomainIdVec& domainIds) {
	rapidjson::Value keyIdDetails(rapidjson::kArrayType);
	for (const auto domId : domainIds) {
		rapidjson::Value keyIdDetail(rapidjson::kObjectType);

		rapidjson::Value key(idTagName, doc.GetAllocator());
		rapidjson::Value domainId;
		domainId.SetInt64(domId);
		keyIdDetail.AddMember(key, domId, doc.GetAllocator());

		keyIdDetails.PushBack(keyIdDetail, doc.GetAllocator());
	}
	rapidjson::Value memberKey(rootTagName, doc.GetAllocator());
	doc.AddMember(memberKey, keyIdDetails, doc.GetAllocator());
}

void addBaseCipherIdDomIdToDoc(rapidjson::Document& doc,
                               rapidjson::Value& keyIdDetails,
                               const EncryptCipherBaseKeyId baseCipherId,
                               const Optional<EncryptCipherDomainId> domainId) {
	rapidjson::Value keyIdDetail(rapidjson::kObjectType);

	// Add 'base_cipher_id'
	rapidjson::Value key(BASE_CIPHER_ID_TAG, doc.GetAllocator());
	rapidjson::Value baseKeyId;
	baseKeyId.SetUint64(baseCipherId);
	keyIdDetail.AddMember(key, baseKeyId, doc.GetAllocator());

	if (domainId.present()) {
		// Add 'encrypt_domain_id'
		key.SetString(ENCRYPT_DOMAIN_ID_TAG, doc.GetAllocator());
		rapidjson::Value domId;
		domId.SetInt64(domainId.get());
		keyIdDetail.AddMember(key, domId, doc.GetAllocator());
	}

	// push above object to the array
	keyIdDetails.PushBack(keyIdDetail, doc.GetAllocator());
}

void addValidationTokensSectionToJsonDoc(rapidjson::Document& doc, const ValidationTokenMap& tokenMap) {
	// Append "validationTokens" as json array
	rapidjson::Value validationTokens(rapidjson::kArrayType);

	for (const auto& token : tokenMap) {
		rapidjson::Value validationToken(rapidjson::kObjectType);

		// Add "name" - token name
		rapidjson::Value key(VALIDATION_TOKEN_NAME_TAG, doc.GetAllocator());
		rapidjson::Value tokenName(token.second.name.data(), doc.GetAllocator());
		validationToken.AddMember(key, tokenName, doc.GetAllocator());

		// Add "value" - token value
		key.SetString(VALIDATION_TOKEN_VALUE_TAG, doc.GetAllocator());
		rapidjson::Value tokenValue;
		tokenValue.SetString(token.second.value.data(), token.second.value.size(), doc.GetAllocator());
		validationToken.AddMember(key, tokenValue, doc.GetAllocator());

		validationTokens.PushBack(validationToken, doc.GetAllocator());
	}

	// Append 'validation_token[]' to the parent document
	rapidjson::Value memberKey(VALIDATION_TOKENS_TAG, doc.GetAllocator());
	doc.AddMember(memberKey, validationTokens, doc.GetAllocator());
}

void addRefreshKmsUrlsSectionToJsonDoc(rapidjson::Document& doc, const bool refreshKmsUrls) {
	rapidjson::Value key(REFRESH_KMS_URLS_TAG, doc.GetAllocator());
	rapidjson::Value refreshUrls;
	refreshUrls.SetBool(refreshKmsUrls);

	// Append 'refresh_kms_urls' object to the parent document
	doc.AddMember(key, refreshUrls, doc.GetAllocator());
}

void addDebugUidSectionToJsonDoc(rapidjson::Document& doc, Optional<UID> dbgId) {
	if (!dbgId.present()) {
		// Debug id not present; do nothing
		return;
	}
	rapidjson::Value key(DEBUG_UID_TAG, doc.GetAllocator());
	rapidjson::Value debugIdVal;
	const std::string dbgIdStr = dbgId.get().toString();
	debugIdVal.SetString(dbgIdStr.data(), dbgIdStr.size(), doc.GetAllocator());

	// Append 'debug_uid' object to the parent document
	doc.AddMember(key, debugIdVal, doc.GetAllocator());
}

Optional<ErrorDetail> getError(const rapidjson::Document& doc) {
	// Check if response has error
	if (doc.HasMember(ERROR_TAG) && !doc[ERROR_TAG].IsNull()) {
		ErrorDetail details;

		if (doc[ERROR_TAG].HasMember(ERROR_MSG_TAG) && doc[ERROR_TAG][ERROR_MSG_TAG].IsString()) {
			details.errorMsg.resize(doc[ERROR_TAG][ERROR_MSG_TAG].GetStringLength());
			memcpy(const_cast<char*>(details.errorMsg.data()),
			       doc[ERROR_TAG][ERROR_MSG_TAG].GetString(),
			       doc[ERROR_TAG][ERROR_MSG_TAG].GetStringLength());
		}
		if (doc[ERROR_TAG].HasMember(ERROR_CODE_TAG) && doc[ERROR_TAG][ERROR_CODE_TAG].IsString()) {
			details.errorCode.resize(doc[ERROR_TAG][ERROR_CODE_TAG].GetStringLength());
			memcpy(const_cast<char*>(details.errorCode.data()),
			       doc[ERROR_TAG][ERROR_CODE_TAG].GetString(),
			       doc[ERROR_TAG][ERROR_CODE_TAG].GetStringLength());
		}
		return details;
	}
	return Optional<ErrorDetail>();
}

void removeTrailingChar(std::string& str, char c) {
	while (!str.empty() && str[str.length() - 1] == c) {
		str.erase(str.length() - 1);
	}
}

ACTOR Future<Void> _procureValidationTokensFromFiles(Reference<RESTKmsConnectorCtx> ctx, std::string details) {
	Standalone<StringRef> detailsRef(details);
	if (details.empty()) {
		TraceEvent("RESTValidationTokenEmptyFileDetails", ctx->uid).log();
		throw encrypt_invalid_kms_config();
	}

	TraceEvent("RESTValidationToken", ctx->uid).detail("DetailsStr", details);

	state std::unordered_map<std::string, std::string> tokenFilePathMap;
	loop {
		StringRef name = detailsRef.eat(TOKEN_NAME_FILE_SEP);
		if (name.empty()) {
			break;
		}
		StringRef path = detailsRef.eat(TOKEN_TUPLE_SEP);
		if (path.empty()) {
			TraceEvent("RESTValidationTokenFileDetailsMalformed", ctx->uid).detail("FileDetails", details);
			throw operation_failed();
		}

		std::string tokenName = boost::trim_copy(name.toString());
		std::string tokenFile = boost::trim_copy(path.toString());
		if (!fileExists(tokenFile)) {
			TraceEvent("RESTValidationTokenFileNotFound", ctx->uid)
			    .detail("TokenName", tokenName)
			    .detail("Filename", tokenFile);
			throw encrypt_invalid_kms_config();
		}

		tokenFilePathMap.emplace(tokenName, tokenFile);
		TraceEvent("RESTValidationToken", ctx->uid).detail("FName", tokenName).detail("Filename", tokenFile);
	}

	// Clear existing cached validation tokens
	ctx->validationTokenMap.clear();

	// Enumerate all token files and extract details
	state uint64_t tokensPayloadSize = 0;
	for (const auto& item : tokenFilePathMap) {
		state std::string tokenName = item.first;
		state std::string tokenFile = item.second;
		state Reference<IAsyncFile> tFile = wait(IAsyncFileSystem::filesystem()->open(
		    tokenFile, IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0644));

		state int64_t fSize = wait(tFile->size());
		if (fSize > SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE) {
			TraceEvent(SevWarnAlways, "RESTValidationTokenFileTooLarge", ctx->uid)
			    .detail("FileName", tokenFile)
			    .detail("Size", fSize)
			    .detail("MaxAllowedSize", SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE);
			throw file_too_large();
		}

		tokensPayloadSize += fSize;
		if (tokensPayloadSize > SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE) {
			TraceEvent(SevWarnAlways, "RESTValidationTokenPayloadTooLarge", ctx->uid)
			    .detail("MaxAllowedSize", SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE);
			throw value_too_large();
		}

		state Standalone<StringRef> buff = makeString(fSize);
		int bytesRead = wait(uncancellable(holdWhile(buff, tFile->read(mutateString(buff), fSize, 0))));
		if (bytesRead != fSize) {
			TraceEvent(SevError, "RESTDiscoveryKmsUrlFileReadShort", ctx->uid)
			    .detail("Filename", tokenFile)
			    .detail("Expected", fSize)
			    .detail("Actual", bytesRead);
			throw io_error();
		}

		// Populate validation token details
		ValidationTokenCtx tokenCtx =
		    ValidationTokenCtx(tokenName, ValidationTokenSource::VALIDATION_TOKEN_SOURCE_FILE);
		tokenCtx.value.resize(fSize);
		memcpy(tokenCtx.value.data(), buff.begin(), fSize);
		tokenCtx.filePath = tokenFile;

		if (SERVER_KNOBS->REST_KMS_CONNECTOR_REMOVE_TRAILING_NEWLINE) {
			removeTrailingChar(tokenCtx.value, '\n');
		}

		// NOTE: avoid logging token-value to prevent token leaks in log files..
		TraceEvent("RESTValidationTokenReadFile", ctx->uid)
		    .detail("TokenName", tokenCtx.name)
		    .detail("TokenSize", tokenCtx.value.size())
		    .detail("TokenFilePath", tokenCtx.filePath.get())
		    .detail("TotalPayloadSize", tokensPayloadSize);

		ctx->validationTokenMap.emplace(tokenName, std::move(tokenCtx));
	}

	return Void();
}

ACTOR Future<Void> _procureValidationTokens(Reference<RESTKmsConnectorCtx> ctx) {
	std::string_view mode{ SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MODE };

	if (mode.compare("file") == 0) {
		wait(_procureValidationTokensFromFiles(ctx, SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_DETAILS));
	} else {
		throw not_implemented();
	}

	return Void();
}

Future<Void> procureValidationTokensFromFiles(Reference<RESTKmsConnectorUtils::RESTKmsConnectorCtx> ctx,
                                              std::string details) {
	return _procureValidationTokensFromFiles(ctx, details);
}
Future<Void> procureValidationTokens(Reference<RESTKmsConnectorUtils::RESTKmsConnectorCtx> ctx) {
	return _procureValidationTokens(ctx);
}

} // namespace RESTKmsConnectorUtils
