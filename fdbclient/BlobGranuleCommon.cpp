/*
 * BlobGranuleCommon.cpp
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

#include "fdbclient/BlobGranuleCommon.h"

BlobGranuleSummaryRef summarizeGranuleChunk(Arena& ar, const BlobGranuleChunkRef& chunk) {
	BlobGranuleSummaryRef summary;
	ASSERT(chunk.snapshotFile.present());
	ASSERT(chunk.snapshotVersion != invalidVersion);
	ASSERT(chunk.includedVersion >= chunk.snapshotVersion);
	ASSERT(chunk.newDeltas.empty());

	if (chunk.tenantPrefix.present()) {
		summary.keyRange = KeyRangeRef(ar, chunk.keyRange.removePrefix(chunk.tenantPrefix.get()));
	} else {
		summary.keyRange = KeyRangeRef(ar, chunk.keyRange);
	}

	summary.snapshotVersion = chunk.snapshotVersion;
	summary.snapshotSize = chunk.snapshotFile.get().length;
	summary.deltaVersion = chunk.includedVersion;
	summary.deltaSize = 0;
	for (auto& it : chunk.deltaFiles) {
		summary.deltaSize += it.length;
	}

	return summary;
}

// a slightly better api might be that the byte limit for the initial request is a hard limit, but the byte limit for
// the stream is a soft limit (can go over by 1 version)
void readGranuleDeltas(GranuleDeltas& srcDeltas,
                       GranuleDeltas& destDeltas,
                       Arena& destArena,
                       Version beginVersion,
                       Version readVersion,
                       Version& includedVersion,
                       int64_t byteLimit,
                       int64_t* byteCount) {
	if (srcDeltas.empty()) {
		includedVersion = readVersion;
		return;
	}
	MutationsAndVersionRef* mutationIt = srcDeltas.begin();
	if (beginVersion > srcDeltas.back().version) {
		CODE_PROBE(true, "beginVersion pruning all in-memory mutations");
		mutationIt = srcDeltas.end();
	} else if (beginVersion > srcDeltas.front().version) {
		// binary search for beginVersion
		CODE_PROBE(true, "beginVersion pruning some in-memory mutations");
		mutationIt = std::lower_bound(srcDeltas.begin(),
		                              srcDeltas.end(),
		                              MutationsAndVersionRef(beginVersion, 0),
		                              MutationsAndVersionRef::OrderByVersion());
	}

	// add mutations to response
	*byteCount = 0;
	int versionsIncluded = 0;
	while (mutationIt != srcDeltas.end()) {
		if (mutationIt->version > readVersion) {
			// printf("DBG:   reading granule deltas read full\n");
			CODE_PROBE(true, "readVersion pruning some in-memory mutations");
			includedVersion = readVersion;
			break;
		}
		// FIXME: could avoid expectedSize on mutation if byte limit is max, but that is no longer the common case
		// if we did not reach the end, and adding the next mutation would put us over the byte
		// limit, don't add it and reply with lower includedVersion
		// except if byteLimit > 0 but the first mutation's size > byteLimit, then we still need to make progress
		*byteCount += mutationIt->expectedSize();
		if (*byteCount > byteLimit && (byteLimit == 0 || versionsIncluded != 0)) {
			break;
		}
		destDeltas.push_back_deep(destArena, *mutationIt);
		includedVersion = mutationIt->version;
		mutationIt++;
		versionsIncluded++;
	}
	if (mutationIt == srcDeltas.end()) {
		includedVersion = readVersion;
	}
}

BlobGranuleFileEncryptionKeys getEncryptBlobCipherKey(const BlobGranuleCipherKeysCtx& cipherKeysCtx) {
	BlobGranuleFileEncryptionKeys eKeys;

	// Cipher key reconstructed is 'never' inserted into BlobCipherKey cache, choose 'neverExpire'
	eKeys.textCipherKey = makeReference<BlobCipherKey>(cipherKeysCtx.textCipherKey.encryptDomainId,
	                                                   cipherKeysCtx.textCipherKey.baseCipherId,
	                                                   cipherKeysCtx.textCipherKey.baseCipher.begin(),
	                                                   cipherKeysCtx.textCipherKey.baseCipher.size(),
	                                                   cipherKeysCtx.textCipherKey.baseCipherKCV,
	                                                   cipherKeysCtx.textCipherKey.salt,
	                                                   std::numeric_limits<int64_t>::max(),
	                                                   std::numeric_limits<int64_t>::max());
	eKeys.headerCipherKey = makeReference<BlobCipherKey>(cipherKeysCtx.headerCipherKey.encryptDomainId,
	                                                     cipherKeysCtx.headerCipherKey.baseCipherId,
	                                                     cipherKeysCtx.headerCipherKey.baseCipher.begin(),
	                                                     cipherKeysCtx.headerCipherKey.baseCipher.size(),
	                                                     cipherKeysCtx.headerCipherKey.baseCipherKCV,
	                                                     cipherKeysCtx.headerCipherKey.salt,
	                                                     std::numeric_limits<int64_t>::max(),
	                                                     std::numeric_limits<int64_t>::max());

	return eKeys;
}

void printGranuleChunks(const VectorRef<BlobGranuleChunkRef>& chunks) {
	printf("Chunks:\n");
	for (auto& chunk : chunks) {
		printf("[%s - %s)\n", chunk.keyRange.begin.printable().c_str(), chunk.keyRange.end.printable().c_str());

		printf("  SnapshotFile:\n    %s\n",
		       chunk.snapshotFile.present() ? chunk.snapshotFile.get().toString().c_str() : "<none>");
		printf("  DeltaFiles:\n");
		for (auto& df : chunk.deltaFiles) {
			printf("    %s\n", df.toString().c_str());
		}
		printf("  Deltas: (%d)", chunk.newDeltas.size());
		if (chunk.newDeltas.size() > 0) {
			fmt::print(" with version [{0} - {1}]",
			           chunk.newDeltas[0].version,
			           chunk.newDeltas[chunk.newDeltas.size() - 1].version);
		}
		fmt::print("  IncludedVersion: {}\n", chunk.includedVersion);
	}
	printf("\n");
}