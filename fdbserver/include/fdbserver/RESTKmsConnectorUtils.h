/*
 * RESTKmsConnectorUtils.h
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

#ifndef FDBSERVER_REST_KMS_CONNECTOR_UTILS_H
#define FDBSERVER_REST_KMS_CONNECTOR_UTILS_H
#pragma once

#include "fdbclient/RESTClient.h"

#include "fdbrpc/HTTP.h"
#include "fdbrpc/Stats.h"

#include "fdbserver/Knobs.h"

#include "flow/ActorCollection.h"
#include "flow/EncryptUtils.h"
#include "flow/flow.h"

#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace RESTKmsConnectorUtils {

extern const char* HTTP_CONTENT_TYPE;
extern const char* HTTP_ACCEPT;

extern const char* BASE_CIPHER_ID_TAG;
extern const char* BASE_CIPHER_TAG;
extern const char* CIPHER_KEY_DETAILS_TAG;
extern const char* ENCRYPT_DOMAIN_ID_TAG;
extern const char* REFRESH_AFTER_SEC;
extern const char* EXPIRE_AFTER_SEC;
extern const char* ERROR_TAG;
extern const char* ERROR_MSG_TAG;
extern const char* ERROR_CODE_TAG;
extern const char* KMS_URLS_TAG;
extern const char* REFRESH_KMS_URLS_TAG;
extern const char* REQUEST_VERSION_TAG;
extern const char* VALIDATION_TOKENS_TAG;
extern const char* VALIDATION_TOKEN_NAME_TAG;
extern const char* VALIDATION_TOKEN_VALUE_TAG;
extern const char* DEBUG_UID_TAG;

extern const char* TOKEN_NAME_FILE_SEP;
extern const char* TOKEN_TUPLE_SEP;
extern const char DISCOVER_URL_FILE_URL_SEP;

extern const char* BLOB_METADATA_DETAILS_TAG;
extern const char* BLOB_METADATA_DOMAIN_ID_TAG;
extern const char* BLOB_METADATA_LOCATIONS_TAG;
extern const char* BLOB_METADATA_LOCATION_ID_TAG;
extern const char* BLOB_METADATA_LOCATION_PATH_TAG;

extern const int INVALID_REQUEST_VERSION;

enum class ValidationTokenSource {
	VALIDATION_TOKEN_SOURCE_FILE = 1,
	VALIDATION_TOKEN_SOURCE_LAST // Always the last element
};

struct ErrorDetail {
	std::string errorCode;
	std::string errorMsg;

	ErrorDetail() {}
	ErrorDetail(const std::string& code, const std::string& msg) : errorCode(code), errorMsg(msg) {}

	bool isEqual(const ErrorDetail& toCompare) const {
		return errorCode.compare(toCompare.errorCode) == 0 && errorMsg.compare(toCompare.errorMsg) == 0;
	}
};

struct ValidationTokenCtx {
	std::string name;
	std::string value;
	ValidationTokenSource source;
	Optional<std::string> filePath;

	explicit ValidationTokenCtx(const std::string& n, ValidationTokenSource s)
	  : name(n),
	    value(""),
	    source(s),
	    filePath(Optional<std::string>()),
	    readTS(now()) {}
	double getReadTS() const { return readTS; }

private:
	double readTS; // Approach assists refreshing token based on time of creation
};
using ValidationTokenMap = std::unordered_map<std::string, ValidationTokenCtx>;

// Routine to determine penalty for cached KMSUrl based on unresponsive KMS behavior observed in recent past. The
// routine is desgined to assign a maximum penalty if KMS responses are unacceptable in very recent past, with time the
// the penalty weight deteorates (matches real world outage OR server overload scenario)

struct KmsUrlPenaltyParams {
	static double penalty(int64_t timeSinceLastPenalty) { return continuousTimeDecay(1.0, 0.1, timeSinceLastPenalty); }
};

template <class Params>
struct KmsUrlCtx {
	enum class PenaltyType { TIMEOUT = 1, MALFORMED_RESPONSE = 2 };

	std::string url;
	uint64_t nRequests;
	uint64_t nFailedResponses;
	uint64_t nResponseParseFailures;
	double unresponsivenessPenalty;
	double unresponsivenessPenaltyTS;

	KmsUrlCtx()
	  : url(""),
	    nRequests(0),
	    nFailedResponses(0),
	    nResponseParseFailures(0),
	    unresponsivenessPenalty(0.0),
	    unresponsivenessPenaltyTS(0) {}
	explicit KmsUrlCtx(const std::string& u)
	  : url(u),
	    nRequests(0),
	    nFailedResponses(0),
	    nResponseParseFailures(0),
	    unresponsivenessPenalty(0.0),
	    unresponsivenessPenaltyTS(0) {}

	bool operator==(const KmsUrlCtx& toCompare) const { return url.compare(toCompare.url) == 0; }

	void refreshUnresponsivenessPenalty() {
		if (unresponsivenessPenaltyTS == 0) {
			return;
		}
		int64_t timeSinceLastPenalty = now() - unresponsivenessPenaltyTS;
		unresponsivenessPenalty = Params::penalty(timeSinceLastPenalty);
	}

	void penalize(const PenaltyType type) {
		if (type == PenaltyType::TIMEOUT) {
			nFailedResponses++;
			unresponsivenessPenaltyTS = now();
		} else {
			ASSERT_EQ(type, PenaltyType::MALFORMED_RESPONSE);
			nResponseParseFailures++;
		}
	}

	std::string toString() const {
		return fmt::format(
		    "{} {} {} {} {}", url, nRequests, nFailedResponses, nResponseParseFailures, unresponsivenessPenalty);
	}
};

// Current implementation is designed to favor the most-preferable KMS for all outbound requests allowing leveraging KMS
// implemented caching if supported
//
// TODO: Implement load-balancing requests to available KMS servers maintaining prioritized KMS server list based on
// observed errors/connection failures/timeouts etc.

template <class Params>
struct KmsUrlStore {
	void sort() {
		std::sort(kmsUrls.begin(), kmsUrls.end(), [](const KmsUrlCtx<Params>& l, const KmsUrlCtx<Params>& r) {
			// Sort the available URLs based on following rules:
			// 1. URL with higher unresponsiveness-penalty are least preferred
			// 2. Among URLs with same unresponsiveness-penalty weight, URLs with more number of failed-respones are
			// less preferrred
			// 3. Lastly, URLs with more malformed response messages are less preferred

			if (l.unresponsivenessPenalty != r.unresponsivenessPenalty) {
				return l.unresponsivenessPenalty < r.unresponsivenessPenalty;
			}
			if (l.nFailedResponses != r.nFailedResponses) {
				return l.nFailedResponses < r.nFailedResponses;
			}
			return l.nResponseParseFailures < r.nResponseParseFailures;
		});
	}

	void penalize(const KmsUrlCtx<Params>& toPenalize, const typename KmsUrlCtx<Params>::PenaltyType type) {
		bool found = false;
		for (KmsUrlCtx<Params>& urlCtx : kmsUrls) {
			if (urlCtx == toPenalize) {
				urlCtx.penalize(type);
				found = true;
				break;
			}
		}
		ASSERT(found);

		// update the penalties
		for (auto& url : kmsUrls) {
			url.refreshUnresponsivenessPenalty();
		}

		if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::DEBUG) {
			std::string details;
			for (const auto& url : kmsUrls) {
				details.append(fmt::format("[ {} ], ", url.toString()));
			}
			TraceEvent("RESTKmsUrlStoreBeforeSort")
			    .detail("Details", details)
			    .detail("Penalize", toPenalize.toString());
		}

		// Reshuffle the URLs
		sort();

		if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::DEBUG) {
			std::string details;
			for (const auto& url : kmsUrls) {
				details.append(fmt::format("[ {} ], ", url.toString()));
			}
			TraceEvent("RESTKmsUrlStoreAfterSort").detail("Details", details);
		}
	}

	std::vector<KmsUrlCtx<Params>> kmsUrls;
};

struct RESTKmsConnectorCtx : public ReferenceCounted<RESTKmsConnectorCtx> {
	UID uid;
	KmsUrlStore<KmsUrlPenaltyParams> kmsUrlStore;
	double lastKmsUrlsRefreshTs;
	double lastKmsUrlDiscoverTS;
	bool kmsStable;
	Reference<RESTClient> restClient;
	ValidationTokenMap validationTokenMap;

	CounterCollection kmsConnectorMetrics;

	Counter numMalformedResponses;

	RESTKmsConnectorCtx(ActorCollection& actors)
	  : uid(deterministicRandom()->randomUniqueID()),
	    lastKmsUrlsRefreshTs(0),
	    lastKmsUrlDiscoverTS(0.0),
	    kmsStable(true),
	    restClient(makeReference<RESTClient>()),
	    kmsConnectorMetrics("KMSConnectorMetrics", uid.toString()),
	    numMalformedResponses("KMSNumMalformedResponses", kmsConnectorMetrics) {
		actors.add(kmsConnectorMetrics.traceCounters(
		    "RESTKmsConnectorMetrics", uid, SERVER_KNOBS->KMS_LOGGING_INTERVAL, "RESTKmsConnectorMetrics"));
	}
	explicit RESTKmsConnectorCtx(const UID& id, ActorCollection& actors)
	  : uid(id),
	    lastKmsUrlsRefreshTs(0),
	    lastKmsUrlDiscoverTS(0.0),
	    kmsStable(true),
	    restClient(makeReference<RESTClient>()),
	    kmsConnectorMetrics("KMSConnectorMetrics", uid.toString()),
	    numMalformedResponses("KMSNumMalformedResponses", kmsConnectorMetrics) {
		actors.add(kmsConnectorMetrics.traceCounters(
		    "RESTKmsConnectorMetrics", uid, SERVER_KNOBS->KMS_LOGGING_INTERVAL, "RESTKmsConnectorMetrics"));
	}
};

void addVersionToDoc(rapidjson::Document& doc, const int requestVersion);
void addLatestDomainDetailsToDoc(rapidjson::Document& doc,
                                 const char* rootTagName,
                                 const char* idTagName,
                                 const EncryptCipherDomainIdVec& domainIds);
void addBaseCipherIdDomIdToDoc(rapidjson::Document& doc,
                               rapidjson::Value& keyIdDetails,
                               const EncryptCipherBaseKeyId baseCipherId,
                               const Optional<EncryptCipherDomainId> domainId);
void addValidationTokensSectionToJsonDoc(rapidjson::Document& doc, const ValidationTokenMap& tokenMap);
void addRefreshKmsUrlsSectionToJsonDoc(rapidjson::Document& doc, const bool refreshKmsUrls);
void addDebugUidSectionToJsonDoc(rapidjson::Document& doc, Optional<UID> dbgId);

Optional<ErrorDetail> getError(const rapidjson::Document& doc);

void removeTrailingChar(std::string& str, char c);
Future<Void> procureValidationTokensFromFiles(Reference<RESTKmsConnectorCtx> ctx, std::string details);
Future<Void> procureValidationTokens(Reference<RESTKmsConnectorCtx> ctx);

} // namespace RESTKmsConnectorUtils

#endif
