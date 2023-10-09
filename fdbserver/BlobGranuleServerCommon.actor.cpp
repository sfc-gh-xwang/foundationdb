/*
 * BlobGranuleServerCommon.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/BulkLoad.h"
#include "fdbclient/Tuple.h"
#include "flow/Error.h"
#include "fmt/format.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Arena.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include

// serialize change feed key as UID bytes, to use 16 bytes on disk
Key granuleIDToCFKey(UID granuleID) {
	BinaryWriter wr(Unversioned());
	wr << granuleID;
	return wr.toValue();
}

// parse change feed key back to UID, to be human-readable
UID cfKeyToGranuleID(Key cfKey) {
	return BinaryReader::fromStringRef<UID>(cfKey, Unversioned());
}

// Gets the latest granule history node for range that was persisted
ACTOR Future<Optional<GranuleHistory>> getLatestGranuleHistory(Transaction* tr, KeyRange range) {
	state KeyRange historyRange = blobGranuleHistoryKeyRangeFor(range);
	state RangeReadResult result = wait(tr->getRange(historyRange, 1, Snapshot::False, Reverse::True));

	ASSERT(result.size() <= 1);

	Optional<GranuleHistory> history;
	if (!result.empty()) {
		std::pair<KeyRange, Version> decodedKey = decodeBlobGranuleHistoryKey(result[0].key);
		ASSERT(range == decodedKey.first);
		history = GranuleHistory(range, decodedKey.second, decodeBlobGranuleHistoryValue(result[0].value));
	}
	return history;
}

// Gets the files based on the file key range [startKey, endKey)
// and populates the files object accordingly
ACTOR Future<Void> readGranuleFiles(Transaction* tr, Key* startKey, Key endKey, GranuleFiles* files, UID granuleID) {

	loop {
		int lim = BUGGIFY ? 2 : 1000;
		RangeReadResult res = wait(tr->getRange(KeyRangeRef(*startKey, endKey), lim));
		for (auto& it : res) {
			UID gid;
			uint8_t fileType;
			Version version;

			Standalone<StringRef> filename;
			int64_t offset;
			int64_t length;
			int64_t fullFileLength;
			int64_t logicalSize;
			Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta;

			std::tie(gid, version, fileType) = decodeBlobGranuleFileKey(it.key);
			ASSERT(gid == granuleID);

			std::tie(filename, offset, length, fullFileLength, logicalSize, cipherKeysMeta) =
			    decodeBlobGranuleFileValue(it.value);

			BlobFileIndex idx(
			    version, filename.toString(), offset, length, fullFileLength, logicalSize, cipherKeysMeta);
			if (fileType == 'S') {
				ASSERT(files->snapshotFiles.empty() || files->snapshotFiles.back().version < idx.version);
				files->snapshotFiles.push_back(idx);
			} else {
				ASSERT(fileType == 'D');
				ASSERT(files->deltaFiles.empty() || files->deltaFiles.back().version < idx.version);
				files->deltaFiles.push_back(idx);
			}
		}
		if (res.more) {
			*startKey = keyAfter(res.back().key);
		} else {
			break;
		}
	}
	return Void();
}

// Wrapper around readGranuleFiles
// Gets all files belonging to the granule with id granule ID
ACTOR Future<GranuleFiles> loadHistoryFiles(Database cx, UID granuleID) {
	state KeyRange range = blobGranuleFileKeyRangeFor(granuleID);
	state Key startKey = range.begin;
	state GranuleFiles files;
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(readGranuleFiles(&tr, &startKey, range.end, &files, granuleID));
			return files;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<ForcedPurgeState> getForcePurgedState(Transaction* tr, KeyRange keyRange) {
	// because map is coalesced, if the result returns more than 1, they must be alternating
	RangeResult values =
	    wait(krmGetRanges(tr, blobGranuleForcePurgedKeys.begin, keyRange, 3, GetRangeLimits::BYTE_LIMIT_UNLIMITED));

	ASSERT(!values.empty());
	if (values.size() > 2) {
		ASSERT(values[0].value != values[1].value);
		return ForcedPurgeState::SomePurged;
	} else {
		return values[0].value == "1"_sr ? ForcedPurgeState::AllPurged : ForcedPurgeState::NonePurged;
	}
}

// Normally a beginVersion != 0 means the caller wants all mutations between beginVersion and readVersion, instead of
// the latest snapshot before readVersion + deltas after the snapshot. When canCollapse is set, the beginVersion is
// essentially just an optimization hint. The caller is still concerned with reconstructing rows at readVersion, it just
// knows it doesn't need anything before beginVersion.
// Normally this can eliminate the need for a snapshot and just return a small amount of deltas. But in a highly active
// key range, the granule may have a snapshot file at version X, where beginVersion < X <= readVersion. In this case, if
// the number of bytes in delta files between beginVersion and X is larger than the snapshot file at version X, it is
// strictly more efficient (in terms of files and bytes read) to just use the snapshot file at version X instead.
//
// To assist BlobGranule file (snapshot and/or delta) file encryption, the routine while populating snapshot and/or
// delta files, constructs BlobFilePointerRef->cipherKeysMeta field. Approach avoids this method to be defined as an
// ACTOR, as fetching desired EncryptionKey may potentially involve reaching out to EncryptKeyProxy or external KMS.
void GranuleFiles::getFiles(Version beginVersion,
                            Version readVersion,
                            bool canCollapse,
                            BlobGranuleChunkRef& chunk,
                            Arena& replyArena,
                            int64_t& deltaBytesCounter,
                            bool summarize) const {
	BlobFileIndex dummyIndex; // for searching
	ASSERT(!snapshotFiles.empty());
	// if beginVersion == 0 or we can collapse, find the latest snapshot <= readVersion
	auto snapshotF = snapshotFiles.end();
	if (beginVersion == 0 || canCollapse) {
		dummyIndex.version = readVersion;
		snapshotF = std::lower_bound(snapshotFiles.begin(), snapshotFiles.end(), dummyIndex);
		if (snapshotF == snapshotFiles.end() || snapshotF->version > readVersion) {
			ASSERT(snapshotF != snapshotFiles.begin());
			snapshotF--;
		}
		ASSERT(snapshotF != snapshotFiles.end());
		ASSERT(snapshotF->version <= readVersion);
	}

	auto deltaF = deltaFiles.end();
	if (beginVersion > 0) {
		dummyIndex.version = beginVersion;
		deltaF = std::lower_bound(deltaFiles.begin(), deltaFiles.end(), dummyIndex);
		if (canCollapse) {
			ASSERT(snapshotF != snapshotFiles.end());
			// If we can collapse, see if delta files up to snapshotVersion are smaller or larger than snapshotBytes in
			// total
			auto deltaFCopy = deltaF;
			int64_t snapshotBytes = snapshotF->length;
			while (deltaFCopy != deltaFiles.end() && deltaFCopy->version <= snapshotF->version && snapshotBytes > 0) {
				snapshotBytes -= deltaFCopy->length;
				deltaFCopy++;
			}
			// if delta files contain the same or more bytes as the snapshot with collapse, do the collapse
			if (snapshotBytes > 0) {
				// don't collapse, clear snapshotF and just do delta files
				snapshotF = snapshotFiles.end();
			} else {
				// do snapshot instead of previous deltas
				dummyIndex.version = snapshotF->version;
				deltaF = std::upper_bound(deltaFiles.begin(), deltaFiles.end(), dummyIndex);
				ASSERT(deltaF == deltaFiles.end() || deltaF->version > snapshotF->version);
			}
		}
	} else {
		dummyIndex.version = snapshotF->version;
		deltaF = std::upper_bound(deltaFiles.begin(), deltaFiles.end(), dummyIndex);
		ASSERT(deltaF == deltaFiles.end() || deltaF->version > snapshotF->version);
	}

	Version lastIncluded = invalidVersion;
	if (snapshotF != snapshotFiles.end()) {
		chunk.snapshotVersion = snapshotF->version;
		chunk.snapshotFile =
		    BlobFilePointerRef(replyArena,
		                       summarize ? "" : snapshotF->filename,
		                       snapshotF->offset,
		                       snapshotF->length,
		                       snapshotF->fullFileLength,
		                       snapshotF->version,
		                       summarize ? Optional<BlobGranuleCipherKeysMeta>() : snapshotF->cipherKeysMeta);
		lastIncluded = chunk.snapshotVersion;
	} else {
		chunk.snapshotVersion = invalidVersion;
	}

	while (deltaF != deltaFiles.end() && deltaF->version < readVersion) {
		chunk.deltaFiles.emplace_back_deep(replyArena,
		                                   summarize ? "" : deltaF->filename,
		                                   deltaF->offset,
		                                   deltaF->length,
		                                   deltaF->fullFileLength,
		                                   deltaF->version,
		                                   summarize ? Optional<BlobGranuleCipherKeysMeta>() : deltaF->cipherKeysMeta);
		deltaBytesCounter += deltaF->length;
		ASSERT(lastIncluded < deltaF->version);
		lastIncluded = deltaF->version;
		deltaF++;
	}
	// include last delta file that passes readVersion, if it exists
	if (deltaF != deltaFiles.end() &&
	    ((!summarize && lastIncluded < readVersion) || (summarize && deltaF->version == readVersion))) {
		chunk.deltaFiles.emplace_back_deep(replyArena,
		                                   deltaF->filename,
		                                   deltaF->offset,
		                                   deltaF->length,
		                                   deltaF->fullFileLength,
		                                   deltaF->version,
		                                   deltaF->cipherKeysMeta);
		deltaBytesCounter += deltaF->length;
		lastIncluded = deltaF->version;
	}
	chunk.includedVersion = lastIncluded;
}

static std::string makeTestFileName(Version v) {
	return "test" + std::to_string(v);
}

static BlobFileIndex makeTestFile(Version v, int64_t len) {
	return BlobFileIndex(v, makeTestFileName(v), 0, len, len, len);
}

static void checkFile(int expectedVersion, const BlobFilePointerRef& actualFile) {
	ASSERT(makeTestFileName(expectedVersion) == actualFile.filename.toString());
}

static void checkFiles(const GranuleFiles& f,
                       Version beginVersion,
                       Version readVersion,
                       bool canCollapse,
                       Optional<int> expectedSnapshotVersion,
                       std::vector<int> expectedDeltaVersions) {
	Arena a;
	BlobGranuleChunkRef chunk;
	int64_t deltaBytes = 0;
	f.getFiles(beginVersion, readVersion, canCollapse, chunk, a, deltaBytes, false);
	fmt::print("results({0}, {1}, {2}):\nEXPECTED:\n    snapshot={3}\n    deltas ({4}):\n",
	           beginVersion,
	           readVersion,
	           canCollapse ? "T" : "F",
	           expectedSnapshotVersion.present() ? makeTestFileName(expectedSnapshotVersion.get()).c_str() : "<N/A>",
	           expectedDeltaVersions.size());
	for (int d : expectedDeltaVersions) {
		fmt::print("        {}\n", makeTestFileName(d));
	}
	fmt::print("ACTUAL:\n    snapshot={0}\n    deltas ({1}):\n",
	           chunk.snapshotFile.present() ? chunk.snapshotFile.get().filename.toString().c_str() : "<N/A>",
	           chunk.deltaFiles.size());
	for (auto& it : chunk.deltaFiles) {
		fmt::print("        {}\n", it.filename.toString());
	}
	printf("\n\n\n");
	ASSERT(expectedSnapshotVersion.present() == chunk.snapshotFile.present());
	if (expectedSnapshotVersion.present()) {
		checkFile(expectedSnapshotVersion.get(), chunk.snapshotFile.get());
	}
	ASSERT(expectedDeltaVersions.size() == chunk.deltaFiles.size());
	for (int i = 0; i < expectedDeltaVersions.size(); i++) {
		checkFile(expectedDeltaVersions[i], chunk.deltaFiles[i]);
	}
}

/*
 * Files:
 * S @ 100 (10 bytes)
 * D @ 150 (5 bytes)
 * D @ 200 (6 bytes)
 * S @ 200 (15 bytes)
 * D @ 250 (7 bytes)
 * D @ 300 (8 bytes)
 * S @ 300 (10 bytes)
 * D @ 350 (4 bytes)
 */
TEST_CASE("/blobgranule/server/common/granulefiles") {
	// simple cases first

	// single snapshot file, no deltas
	GranuleFiles files;
	files.snapshotFiles.push_back(makeTestFile(100, 10));

	printf("Just snapshot\n");

	checkFiles(files, 0, 100, false, 100, {});
	checkFiles(files, 0, 200, false, 100, {});

	printf("Small test\n");
	// add delta files with re-snapshot at end
	files.deltaFiles.push_back(makeTestFile(150, 5));
	files.deltaFiles.push_back(makeTestFile(200, 6));
	files.snapshotFiles.push_back(makeTestFile(200, 15));

	// check different read versions with beginVersion=0
	checkFiles(files, 0, 100, false, 100, {});
	checkFiles(files, 0, 101, false, 100, { 150 });
	checkFiles(files, 0, 149, false, 100, { 150 });
	checkFiles(files, 0, 150, false, 100, { 150 });
	checkFiles(files, 0, 151, false, 100, { 150, 200 });
	checkFiles(files, 0, 199, false, 100, { 150, 200 });
	checkFiles(files, 0, 200, false, 200, {});
	checkFiles(files, 0, 300, false, 200, {});

	// Test all cases of beginVersion + readVersion. Because delta files are smaller than snapshot at 200, this should
	// be the same with and without collapse
	checkFiles(files, 100, 200, false, Optional<int>(), { 150, 200 });
	checkFiles(files, 100, 300, false, Optional<int>(), { 150, 200 });
	checkFiles(files, 101, 199, false, Optional<int>(), { 150, 200 });
	checkFiles(files, 149, 151, false, Optional<int>(), { 150, 200 });
	checkFiles(files, 149, 150, false, Optional<int>(), { 150 });
	checkFiles(files, 150, 151, false, Optional<int>(), { 150, 200 });
	checkFiles(files, 151, 200, false, Optional<int>(), { 200 });

	checkFiles(files, 100, 200, true, Optional<int>(), { 150, 200 });
	checkFiles(files, 100, 300, true, Optional<int>(), { 150, 200 });
	checkFiles(files, 101, 199, true, Optional<int>(), { 150, 200 });
	checkFiles(files, 149, 151, true, Optional<int>(), { 150, 200 });
	checkFiles(files, 149, 150, true, Optional<int>(), { 150 });
	checkFiles(files, 150, 151, true, Optional<int>(), { 150, 200 });
	checkFiles(files, 151, 200, true, Optional<int>(), { 200 });

	printf("Larger test\n");
	// add more delta files and snapshots to check collapse logic
	files.deltaFiles.push_back(makeTestFile(250, 7));
	files.deltaFiles.push_back(makeTestFile(300, 8));
	files.snapshotFiles.push_back(makeTestFile(300, 10));
	files.deltaFiles.push_back(makeTestFile(350, 4));

	checkFiles(files, 0, 300, false, 300, {});
	checkFiles(files, 0, 301, false, 300, { 350 });
	checkFiles(files, 0, 400, false, 300, { 350 });

	// check delta files without collapse

	checkFiles(files, 100, 301, false, Optional<int>(), { 150, 200, 250, 300, 350 });
	checkFiles(files, 100, 300, false, Optional<int>(), { 150, 200, 250, 300 });
	checkFiles(files, 100, 251, false, Optional<int>(), { 150, 200, 250, 300 });
	checkFiles(files, 100, 250, false, Optional<int>(), { 150, 200, 250 });

	checkFiles(files, 151, 300, false, Optional<int>(), { 200, 250, 300 });
	checkFiles(files, 151, 301, false, Optional<int>(), { 200, 250, 300, 350 });
	checkFiles(files, 151, 400, false, Optional<int>(), { 200, 250, 300, 350 });

	checkFiles(files, 201, 300, false, Optional<int>(), { 250, 300 });
	checkFiles(files, 201, 301, false, Optional<int>(), { 250, 300, 350 });
	checkFiles(files, 201, 400, false, Optional<int>(), { 250, 300, 350 });

	checkFiles(files, 251, 300, false, Optional<int>(), { 300 });
	checkFiles(files, 251, 301, false, Optional<int>(), { 300, 350 });
	checkFiles(files, 251, 400, false, Optional<int>(), { 300, 350 });
	checkFiles(files, 301, 400, false, Optional<int>(), { 350 });
	checkFiles(files, 351, 400, false, Optional<int>(), {});

	// check with collapse
	// these 2 collapse because the delta files at 150+200+250+300 are larger than the snapshot at 300
	checkFiles(files, 100, 301, true, 300, { 350 });
	checkFiles(files, 100, 300, true, 300, {});
	// these 2 don't collapse because 150+200 delta files are smaller than the snapshot at 200
	checkFiles(files, 100, 251, true, Optional<int>(), { 150, 200, 250, 300 });
	checkFiles(files, 100, 250, true, Optional<int>(), { 150, 200, 250 });

	// these 3 do collapse because the delta files at 200+250+300 are larger than the snapshot at 300
	checkFiles(files, 151, 300, true, 300, {});
	checkFiles(files, 151, 301, true, 300, { 350 });
	checkFiles(files, 151, 400, true, 300, { 350 });

	// these 3 do collapse because the delta files at 250+300 are larger than the snapshot at 300
	checkFiles(files, 201, 300, true, 300, {});
	checkFiles(files, 201, 301, true, 300, { 350 });
	checkFiles(files, 201, 400, true, 300, { 350 });

	// these don't collapse because the delta file at 300 is smaller than the snapshot at 300
	checkFiles(files, 251, 300, true, Optional<int>(), { 300 });
	checkFiles(files, 251, 301, true, Optional<int>(), { 300, 350 });
	checkFiles(files, 251, 400, true, Optional<int>(), { 300, 350 });
	checkFiles(files, 301, 400, true, Optional<int>(), { 350 });
	checkFiles(files, 351, 400, true, Optional<int>(), {});

	return Void();
}

static void checkSummary(const GranuleFiles& f,
                         Version summaryVersion,
                         Version expectedSnapshotVersion,
                         int64_t expectedSnapshotSize,
                         Version expectedDeltaVersion,
                         Version expectedDeltaSize) {
	Arena fileArena, summaryArena;
	BlobGranuleChunkRef chunk;
	int64_t deltaBytes = 0;
	f.getFiles(0, summaryVersion, true, chunk, fileArena, deltaBytes, true);

	BlobGranuleSummaryRef summary = summarizeGranuleChunk(summaryArena, chunk);

	ASSERT(expectedSnapshotVersion == summary.snapshotVersion);
	ASSERT(expectedSnapshotSize == summary.snapshotSize);
	ASSERT(expectedDeltaVersion == summary.deltaVersion);
	ASSERT(expectedDeltaSize == summary.deltaSize);
	ASSERT(deltaBytes == expectedDeltaSize);
}

/*
 * This should technically be in client unit tests but we don't have a unit test there
 * Files:
 * S @ 100 (10 bytes)
 * D @ 150 (5 bytes)
 * D @ 200 (6 bytes)
 */
TEST_CASE("/blobgranule/server/common/granulesummary") {
	GranuleFiles files;
	files.snapshotFiles.push_back(makeTestFile(100, 10));
	files.deltaFiles.push_back(makeTestFile(150, 5));
	files.deltaFiles.push_back(makeTestFile(200, 6));

	checkSummary(files, 100, 100, 10, 100, 0);
	checkSummary(files, 149, 100, 10, 100, 0);
	checkSummary(files, 150, 100, 10, 150, 5);
	checkSummary(files, 199, 100, 10, 150, 5);
	checkSummary(files, 200, 100, 10, 200, 11);
	checkSummary(files, 700, 100, 10, 200, 11);

	return Void();
}

// FIXME: if credentials can expire, refresh periodically
ACTOR Future<Void> loadBlobMetadataForTenants(BGTenantMap* self, std::vector<BlobMetadataDomainId> tenantsToLoad) {
	ASSERT(SERVER_KNOBS->BG_METADATA_SOURCE == "tenant");
	ASSERT(!tenantsToLoad.empty());
	state EKPGetLatestBlobMetadataRequest req;
	state double retrySleep = 0.1;
	for (const auto tenantId : tenantsToLoad) {
		req.domainIds.emplace_back(tenantId);
	}

	// FIXME: if one tenant gets an error, don't kill whole process
	state double startTime = now();
	state UID prevEKPID = UID();
	state Future<EKPGetLatestBlobMetadataReply> requestFuture = Never();
	state ActiveCounter<int>::Releaser holdingRequestCounter =
	    BlobCipherMetrics::getInstance()->outstandingBlobMetadataRequests.take(1);
	loop {
		try {
			if (self->dbInfo.isValid() && self->dbInfo->get().client.encryptKeyProxy.present()) {
				if (self->dbInfo->get().client.encryptKeyProxy.get().myId != prevEKPID) {
					prevEKPID = self->dbInfo->get().client.encryptKeyProxy.get().myId;
					req.reply.reset();
					requestFuture = brokenPromiseToNever(
					    self->dbInfo->get().client.encryptKeyProxy.get().getLatestBlobMetadata.getReply(req));
				}
			} else {
				prevEKPID = UID();
				requestFuture = Never();
			}
			choose {
				when(EKPGetLatestBlobMetadataReply rep = wait(requestFuture)) {
					ASSERT(rep.blobMetadataDetails.size() <= req.domainIds.size());
					// not guaranteed to be in same order in the request as the response
					for (auto& metadata : rep.blobMetadataDetails) {
						auto info = self->tenantInfoById.find(metadata.domainId);
						if (info == self->tenantInfoById.end()) {
							continue;
						}
						auto dataEntry = self->tenantData.rangeContaining(info->second.prefix);
						ASSERT(dataEntry.begin() == info->second.prefix);
						dataEntry.cvalue()->updateBStore(metadata);
					}
					double elapsed = now() - startTime;
					BlobCipherMetrics::getInstance()->getBlobMetadataLatency.addMeasurement(elapsed);

					if (rep.blobMetadataDetails.size() == req.domainIds.size()) {
						return Void();
					}

					// if not all tenants included in response, or on error, retry with missing ones
					std::unordered_set<BlobMetadataDomainId> missingIds;
					for (auto& id : req.domainIds) {
						missingIds.insert(id);
					}
					for (auto& metadata : rep.blobMetadataDetails) {
						missingIds.erase(metadata.domainId);
					}
					ASSERT(missingIds.size() > 0);

					TraceEvent(SevWarn, "BlobMetadataFetchMissingTenants")
					    .suppressFor(30.0)
					    .detail("Count", missingIds.size());
					CODE_PROBE(true, "blob metadata fetch missing tenants");

					req.domainIds.clear();
					for (auto& id : missingIds) {
						req.domainIds.push_back(id);
					}

					// redo request
					// reset request on error
					prevEKPID = UID();
					requestFuture = Never();
				}
				when(wait(self->dbInfo->onChange())) {
					// reset retry sleep
					retrySleep = 0.1;
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}
			CODE_PROBE(true, "blob metadata fetch error");
			TraceEvent(SevWarn, "BlobMetadataFetchError").errorUnsuppressed(e).suppressFor(30.0);
			// need to reset request on error
			prevEKPID = UID();
			requestFuture = Never();
		}
		wait(delay(retrySleep));
		retrySleep = std::min(10.0, retrySleep * 1.5);
	}
}

Future<Void> loadBlobMetadataForTenant(BGTenantMap* self, BlobMetadataDomainId domainId) {
	std::vector<BlobMetadataDomainId> toLoad;
	toLoad.push_back(domainId);
	return loadBlobMetadataForTenants(self, toLoad);
}

// list of tenants that may or may not already exist
void BGTenantMap::addTenants(std::vector<std::pair<int64_t, TenantMapEntry>> tenants) {
	for (auto entry : tenants) {
		if (tenantInfoById.insert({ entry.first, entry.second }).second) {
			auto r = makeReference<GranuleTenantData>(entry.second);
			tenantData.insert(KeyRangeRef(entry.second.prefix, entry.second.prefix.withSuffix(normalKeys.end)), r);
			if (SERVER_KNOBS->BG_METADATA_SOURCE != "tenant") {
				r->startedLoadingBStore = true;
				r->bstoreLoaded.send(Void());
			}
		}
	}
}

// TODO: implement
void BGTenantMap::removeTenants(std::vector<int64_t> tenantIds) {
	throw not_implemented();
}

Optional<TenantMapEntry> BGTenantMap::getTenantById(int64_t id) {
	auto tenant = tenantInfoById.find(id);
	if (tenant == tenantInfoById.end()) {
		return {};
	} else {
		return tenant->second;
	}
}

// FIXME: batch requests for refresh?
// FIXME: don't double fetch if multiple accesses to refreshing/expired metadata
// FIXME: log warning if after refresh, data is still expired!
ACTOR Future<Reference<GranuleTenantData>> getDataForGranuleActor(BGTenantMap* self,
                                                                  KeyRange keyRange,
                                                                  bool needBStore) {
	state int loopCount = 0;
	loop {
		loopCount++;
		auto tenantRange = self->tenantData.rangeContaining(keyRange.begin);
		ASSERT(tenantRange.begin() <= keyRange.begin);
		ASSERT(tenantRange.end() >= keyRange.end);
		state Reference<GranuleTenantData> tenant = tenantRange.cvalue();

		if (!tenant.isValid()) {
			return tenant;
		} else if (needBStore &&
		           (!tenant->startedLoadingBStore || (tenant->bstore.isValid() && tenant->bstore->isExpired()))) {
			++BlobCipherMetrics::getInstance()->blobMetadataCacheMiss;
			tenant->startedLoadingBStore = true;
			CODE_PROBE(true, "re-fetching expired blob metadata");

			// even if this actor gets cancelled, we marked it as startedLoading, so finish the load in the actor
			// collection
			Future<Void> loadFuture = loadBlobMetadataForTenant(self, tenant->entry.id);
			self->addActor.send(loadFuture);
			wait(loadFuture);

			ASSERT(tenant->bstore.isValid());
			if (!tenant->bstore->isExpired()) {
				return tenant;
			}

			if (tenant->bstore->isExpired() && loopCount > 1) {
				TraceEvent(SevWarn, "BlobMetadataStillExpired").suppressFor(5.0).detail("LoopCount", loopCount);
				wait(delay(0.001));
			}
		} else {
			if (needBStore) {
				++BlobCipherMetrics::getInstance()->blobMetadataCacheHit;
				// handle refresh in background if tenant needs refresh
				if (tenant->bstore.isValid() && tenant->bstore->needsRefresh()) {
					++BlobCipherMetrics::getInstance()->blobMetadataNeedsRefresh;
					Future<Void> reload = loadBlobMetadataForTenant(self, tenant->entry.id);
					self->addActor.send(reload);
				}
			}

			return tenant;
		}
	}
}

// TODO: handle case where tenant isn't loaded yet
Future<Reference<GranuleTenantData>> BGTenantMap::getDataForGranule(const KeyRangeRef& keyRange, bool needBStore) {
	return getDataForGranuleActor(this, keyRange, needBStore);
}

ACTOR Future<Void> loadBGTenantMap(BGTenantMap* tenantData, Transaction* tr) {
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenantResults;
			wait(store(tenantResults,
			           TenantMetadata::tenantMap().getRange(tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1)));
			ASSERT(tenantResults.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER && !tenantResults.more);
			tenantData->addTenants(tenantResults.results);
			TraceEvent("LoadedBGTenantMap").detail("Size", tenantResults.results.size());
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Reference<BlobConnectionProvider>> loadBStoreForTenant(BGTenantMap* tenantData, KeyRange keyRange) {
	if (SERVER_KNOBS->BG_METADATA_SOURCE == "tenant") {
		state int retryCount = 0;
		loop {
			state Reference<GranuleTenantData> data;
			wait(store(data, tenantData->getDataForGranule(keyRange)));
			if (data.isValid()) {
				wait(data->bstoreLoaded.getFuture());
				wait(delay(0));
				return data->bstore;
			} else {
				CODE_PROBE(true, "bstore for unknown tenant", probe::decoration::rare);
				// Assume not loaded yet, just wait a bit. Could do sophisticated mechanism but will redo tenant
				// loading to be versioned anyway. 10 retries means it's likely not a transient race with
				// loading tenants, and instead a persistent issue.
				retryCount++;
				TraceEvent(retryCount <= 10 ? SevDebug : SevWarn, "UnknownTenantForGranule")
				    .detail("KeyRange", keyRange);
				wait(delay(0.1));
			}
		}
	} else if (!SERVER_KNOBS->BG_URL.empty()) {
		return BlobConnectionProvider::newBlobConnectionProvider(SERVER_KNOBS->BG_URL);
	} else {
		TraceEvent(SevError, "MissingBlobStoreConfig").detail("KeyRange", keyRange);
		throw restore_error();
	}
}

// ensure sizes are valid, all ids are unique, all key ranges are within bulk load key range, and key ranges are all
// disjoint
// FIXME: for large number of files, maybe break up with yields?
void validateBulkLoadWrittenPointers(const BulkLoadID& id,
                                     const BulkLoadInfo& info,
                                     const VectorRef<BulkLoadWrittenFileRef>& files,
                                     int truncateOffset,
                                     bool isSnowtramAware) {
	std::vector<KeyRange> ranges;
	std::unordered_set<BulkLoadFileID> fileIds;
	ranges.reserve(files.size());
	fileIds.reserve(files.size());
	for (int i = 0; i < files.size(); i++) {
		auto& p = files[i];
		if (p.logicalSize < 0 || p.physicalSize < 0) {
			TraceEvent("BulkLoadInvalidPointerSize")
			    .detail("LoadID", id)
			    .detail("FileID", p.fileId)
			    .detail("LogicalSize", p.logicalSize)
			    .detail("PhysicalSize", p.physicalSize);
			throw bulk_load_invalid_file_metadata();
		}
		if (!info.keyRange.contains(p.keyRange)) {
			TraceEvent("BulkLoadInvalidPointerNotInsideLoadRange")
			    .detail("LoadID", id)
			    .detail("FileID", p.fileId)
			    .detail("LoadRange", info.keyRange)
			    .detail("PointerRange", p.keyRange);
			// file key range not inside bulk load key range
			throw bulk_load_invalid_file_metadata();
		}

		// Clients provide unaligned keys in the written file metadata.
		// Truncate & align the start key, so that the checks below use
		// aligned keys. This ensures checking that granules aren't split in between
		// aligned keys.
		Key truncatedStartKey = BlobGranuleKeyHelpers::truncateKey(p.keyRange.begin, truncateOffset, isSnowtramAware);
		KeyRange truncatedKeyRange(KeyRangeRef(truncatedStartKey, p.keyRange.end));
		ranges.push_back(truncatedKeyRange);
		if (!fileIds.insert(p.fileId).second) {
			// duplicate file ids
			TraceEvent("BulkLoadInvalidPointerDuplicateFileID").detail("LoadID", id).detail("FileID", p.fileId);
			throw bulk_load_invalid_file_metadata();
		}
	}
	std::sort(ranges.begin(), ranges.end(), KeyRangeRef::ArbitraryOrder());
	for (int i = 1; i < ranges.size(); i++) {
		// meta-check: validate sort worked
		ASSERT(ranges[i - 1].begin <= ranges[i].begin);
		if (ranges[i - 1].end > ranges[i].begin) {
			TraceEvent("BulkLoadInvalidPointerOverlappingKeyRanges")
			    .detail("LoadID", id)
			    .detail("FileRange", ranges[i])
			    .detail("OtherRange", ranges[i - 1]);
			// non-disjoint key ranges
			throw bulk_load_invalid_file_metadata();
		}
	}
}

// ----- Unit Tests + Helper Functions for testing validateBulkLoadWrittenPointers -----
// helper to convert std::vector to VectorRef
void validateBulkLoadWrittenPointers(const BulkLoadID& id,
                                     const BulkLoadInfo& info,
                                     const std::vector<BulkLoadWrittenFileRef>& files,
                                     int truncateOffset,
                                     bool isSnowtramAware) {
	Standalone<VectorRef<BulkLoadWrittenFileRef>> vRef;
	vRef.reserve(vRef.arena(), files.size());
	for (auto& it : files) {
		vRef.push_back(vRef.arena(), it);
	}
	return validateBulkLoadWrittenPointers(id, info, vRef, truncateOffset, isSnowtramAware);
}

BulkLoadWrittenFileRef createPtr(const BulkLoadInfo& info, Arena& ar) {
	BulkLoadWrittenFileRef ptr;
	UID fileUID = deterministicRandom()->randomUniqueID();
	ptr.fileId = StringRef(ar, fileUID.toString());
	ptr.keyRange = info.keyRange;
	ptr.logicalSize = 1;
	ptr.physicalSize = 1;
	return ptr;
}

void checkKeyRanges(bool expectSuccess,
                    const BulkLoadID& id,
                    const BulkLoadInfo& info,
                    const std::vector<KeyRangeRef>& ranges,
                    int truncateOffset,
                    bool isSnowtramAware) {
	Standalone<VectorRef<BulkLoadWrittenFileRef>> ptrs;
	ptrs.reserve(ptrs.arena(), ranges.size());
	for (auto& it : ranges) {
		BulkLoadWrittenFileRef ptr = createPtr(info, ptrs.arena());
		ptr.keyRange = it;
		ptrs.push_back(ptrs.arena(), ptr);
	}

	try {
		validateBulkLoadWrittenPointers(id, info, ptrs, truncateOffset, isSnowtramAware);
		ASSERT(expectSuccess);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_bulk_load_invalid_file_metadata);
		ASSERT(!expectSuccess);
	}
}

Key getKeyWithUserType(const std::string& key, const std::string& userTypeStr) {
	Tuple::UserTypeStr udt(0x41, userTypeStr);
	return Tuple::makeTuple(key, udt).pack();
}

TEST_CASE("/fdbserver/bulkload/validateBulkLoadWrittenPointers") {
	state Arena ar;
	state Standalone<StringRef> keyA;
	state Standalone<StringRef> keyB;
	state Standalone<StringRef> keyC;
	state Standalone<StringRef> keyD;
	state Standalone<StringRef> keyZ;
	state BulkLoadID id = "bulkloadid"_sr;
	state BulkLoadInfo info;

	state std::vector<bool> keysWithUdt(true, false);
	for (auto useUdt : keysWithUdt) {
		if (useUdt) {
			keyA = getKeyWithUserType("a", "0");
			keyB = getKeyWithUserType("b", "0");
			keyC = getKeyWithUserType("c", "0");
			keyD = getKeyWithUserType("d", "0");
			keyZ = getKeyWithUserType("z", "0");
		} else {
			keyA = "a"_sr;
			keyB = "b"_sr;
			keyC = "c"_sr;
			keyD = "d"_sr;
			keyZ = "z"_sr;
		}
		info.keyRange = KeyRangeRef(keyA, keyD);

		state int truncateOffset = useUdt ? 1 : 0;

		// check default pointer is valid
		{
			BulkLoadWrittenFileRef ptr = createPtr(info, ar);
			validateBulkLoadWrittenPointers(id, info, { ptr }, truncateOffset, false);
			checkKeyRanges(true, id, info, { KeyRangeRef(keyA, keyB) }, truncateOffset, false);
		}

		// check invalid logical/physical size
		{
			try {
				BulkLoadWrittenFileRef ptr = createPtr(info, ar);
				ptr.logicalSize = -1;
				validateBulkLoadWrittenPointers(id, info, { ptr }, truncateOffset, false);
				ASSERT(false);
			} catch (Error& e) {
				ASSERT(e.code() == error_code_bulk_load_invalid_file_metadata);
			}
		}

		{
			try {
				BulkLoadWrittenFileRef ptr = createPtr(info, ar);
				ptr.physicalSize = -1;
				validateBulkLoadWrittenPointers(id, info, { ptr }, truncateOffset, false);
				ASSERT(false);
			} catch (Error& e) {
				ASSERT(e.code() == error_code_bulk_load_invalid_file_metadata);
			}
		}

		// check duplicate file ids
		{
			try {
				BulkLoadWrittenFileRef ptr = createPtr(info, ar);
				validateBulkLoadWrittenPointers(id, info, { ptr, ptr }, truncateOffset, false);
				ASSERT(false);
			} catch (Error& e) {
				ASSERT(e.code() == error_code_bulk_load_invalid_file_metadata);
			}
		}

		// check ranges outside of bulk load range
		{
			checkKeyRanges(false, id, info, { KeyRangeRef(""_sr, keyZ) }, truncateOffset, false);
			checkKeyRanges(false, id, info, { KeyRangeRef(""_sr, keyA) }, truncateOffset, false);
			checkKeyRanges(false, id, info, { KeyRangeRef(""_sr, keyB) }, truncateOffset, false);
			checkKeyRanges(false, id, info, { KeyRangeRef(""_sr, keyD) }, truncateOffset, false);
			checkKeyRanges(false, id, info, { KeyRangeRef(keyA, keyZ) }, truncateOffset, false);
			checkKeyRanges(false, id, info, { KeyRangeRef(keyB, keyZ) }, truncateOffset, false);
			checkKeyRanges(false, id, info, { KeyRangeRef(keyZ, "\xff"_sr) }, truncateOffset, false);
		}

		// check overlapping ranges within bulk load range
		{

			checkKeyRanges(
			    false, id, info, { KeyRangeRef(keyA, keyC), KeyRangeRef(keyB, keyD) }, truncateOffset, false);
			checkKeyRanges(
			    false, id, info, { KeyRangeRef(keyA, keyD), KeyRangeRef(keyB, keyC) }, truncateOffset, false);
			// reverse order
			checkKeyRanges(
			    false, id, info, { KeyRangeRef(keyB, keyD), KeyRangeRef(keyA, keyC) }, truncateOffset, false);
			checkKeyRanges(
			    false, id, info, { KeyRangeRef(keyB, keyC), KeyRangeRef(keyA, keyD) }, truncateOffset, false);
		}

		// check valid subsets of ranges
		{
			// one range or subranges
			checkKeyRanges(true, id, info, { KeyRangeRef(keyA, keyD) }, truncateOffset, false);
			checkKeyRanges(true, id, info, { KeyRangeRef(keyA, keyB) }, truncateOffset, false);
			checkKeyRanges(true, id, info, { KeyRangeRef(keyB, keyC) }, truncateOffset, false);
			checkKeyRanges(true, id, info, { KeyRangeRef(keyC, keyD) }, truncateOffset, false);
			// non-connecting but multiple
			checkKeyRanges(true, id, info, { KeyRangeRef(keyA, keyB), KeyRangeRef(keyC, keyD) }, truncateOffset, false);
			checkKeyRanges(true, id, info, { KeyRangeRef(keyC, keyD), KeyRangeRef(keyA, keyB) }, truncateOffset, false);
			// multiple connecting
			checkKeyRanges(true,
			               id,
			               info,
			               { KeyRangeRef(keyA, keyB), KeyRangeRef(keyB, keyC), KeyRangeRef(keyC, keyD) },
			               truncateOffset,
			               false);
			checkKeyRanges(true,
			               id,
			               info,
			               { KeyRangeRef(keyB, keyC), KeyRangeRef(keyA, keyB), KeyRangeRef(keyC, keyD) },
			               truncateOffset,
			               false);
			checkKeyRanges(true,
			               id,
			               info,
			               { KeyRangeRef(keyC, keyD), KeyRangeRef(keyB, keyC), KeyRangeRef(keyA, keyB) },
			               truncateOffset,
			               false);
		}
	}
	state Standalone<StringRef> keyA0 = getKeyWithUserType("a", "0");
	state Standalone<StringRef> keyB1 = getKeyWithUserType("b", "1");
	state Standalone<StringRef> keyB2 = getKeyWithUserType("b", "2");
	state Standalone<StringRef> keyC0 = getKeyWithUserType("c", "0");
	state Standalone<StringRef> keyD0 = getKeyWithUserType("c", "0");
	// Check splits in between truncated keys.
	{
		info.keyRange = KeyRangeRef(keyA0, keyD0);
		checkKeyRanges(false, id, info, { KeyRangeRef(keyA0, keyB1), KeyRangeRef(keyB1, keyC0) }, 1, false);
		checkKeyRanges(false, id, info, { KeyRangeRef(keyA0, keyB1), KeyRangeRef(keyB2, keyC0) }, 1, false);
	}

	return Void();
}

namespace BlobGranuleKeyHelpers {

Key truncateKey(KeyRef key, int truncateOffset, bool isSnowtramAware) {
	if (truncateOffset <= 0) {
		return key;
	}
	Key truncatedKey = key;
	// if snowtram aware, tuple must parse and not be truncated, but maintain non-snowtram-aware case for backwards
	// compatibility
	Tuple t = Tuple::unpackUserType(key, !isSnowtramAware);
	if (isSnowtramAware) {
		bool snowtramKey = t.isSnowtramKey();
		if (snowtramKey && t.isSnowtramDataOrIndexSpace()) {
			CODE_PROBE(true, "snowtram-aware alignKeys found snowtram key to truncate");
			ASSERT(truncateOffset == 1);
		} else {
			CODE_PROBE(snowtramKey, "found snowtram key in non-data/index space");
			// if not valid snowtram key in the data or index space, don't truncate
			// We expect TST entries to hit this, but if any user key does that's bad
			CODE_PROBE(true, "snowtram-aware alignKeys found non-snowtram key, skipping truncate");
			truncateOffset = t.size();
		}
	}

	if (t.size() > truncateOffset) {
		Tuple t2 = t.subTuple(0, t.size() - truncateOffset);
		truncatedKey = t2.pack();
	}
	return truncatedKey;
}

bool canSplit(KeyRef prevKey, KeyRef nextKey, int truncateOffset, bool isSnowtramAware) {
	Key truncatedNextKey = truncateKey(nextKey, truncateOffset, isSnowtramAware);
	Key truncatedPrevKey = truncateKey(prevKey, truncateOffset, isSnowtramAware);
	// Allow splitting only if this truncated key is greater than the previous.
	if (truncatedNextKey > truncatedPrevKey) {
		return true;
	}
	return false;
}

} // namespace BlobGranuleKeyHelpers

TEST_CASE("/fdbserver/bulkload/truncateKey") {
	state Key key = getKeyWithUserType("TestKey", "UserType");
	state Key expectedKey = Tuple::makeTuple("TestKey"_sr).pack();

	// Verify that truncation works.
	state Key truncated = BlobGranuleKeyHelpers::truncateKey(key, 1, false);
	ASSERT_EQ(truncated, expectedKey);

	// No truncation with 0 offset.
	truncated = BlobGranuleKeyHelpers::truncateKey(key, 0, false);
	ASSERT_EQ(truncated, key);

	// Snowtram aware truncation does nothing on non-snowtram keys.
	truncated = BlobGranuleKeyHelpers::truncateKey(key, 1, true);
	ASSERT_EQ(truncated, key);

	// Snowtram aware compaction does nothing on non-data or index space keys
	state Tuple snowtramNonDataIndexTuple = Tuple::makeTuple(/*table id=*/1,
	                                                         /*table incarnation=*/2,
	                                                         /*space enum */ 1,
	                                                         "TestKey"_sr,
	                                                         Tuple::UserTypeStr(0x41, "UserTypeStr"_sr));
	state Key fakeSnowtramKey = snowtramNonDataIndexTuple.pack();
	truncated = BlobGranuleKeyHelpers::truncateKey(fakeSnowtramKey, 1, true);
	ASSERT_EQ(truncated, fakeSnowtramKey);

	// Snowtram aware truncation on snowtram keys.
	state Tuple snowtramTuple =
	    Tuple::makeTuple(/*table id=*/1, /*table incarnation=*/2, /*space enum */ 2, "TestKey"_sr);
	expectedKey = snowtramTuple.pack();
	fakeSnowtramKey = snowtramTuple.append(Tuple::UserTypeStr(0x41, "UserTypeStr"_sr)).pack();
	truncated = BlobGranuleKeyHelpers::truncateKey(fakeSnowtramKey, 1, true);
	ASSERT_EQ(truncated, expectedKey);

	return Void();
}

TEST_CASE("/fdbserver/bulkload/canSplit") {
	state Key keyA1 = getKeyWithUserType("a", "1");
	state Key keyA5 = getKeyWithUserType("a", "5");

	// Don't split the same aligned key.
	ASSERT_EQ(BlobGranuleKeyHelpers::canSplit(keyA1, keyA5, 1, false), false);

	// Don't split keys smaller or equal to than the previous, with and without truncation being enabled.
	state Key keyB1 = getKeyWithUserType("b", "1");
	state Key keyB5 = getKeyWithUserType("b", "5");
	for (bool truncateOffset : { 0, 1 }) {
		ASSERT_EQ(BlobGranuleKeyHelpers::canSplit(keyB1, keyA5, truncateOffset, false), false);
		ASSERT_EQ(BlobGranuleKeyHelpers::canSplit(keyB5, keyA1, truncateOffset, false), false);
		ASSERT_EQ(BlobGranuleKeyHelpers::canSplit(keyA5, keyA5, truncateOffset, false), false);
	}

	// Allow splitting if truncated next key > previous.
	ASSERT_EQ(BlobGranuleKeyHelpers::canSplit(keyA5, keyB1, 1, false), true);
	ASSERT_EQ(BlobGranuleKeyHelpers::canSplit(keyA1, keyB5, 1, false), true);

	// Allow splitting unaligned keys if truncation is disabled.
	ASSERT_EQ(BlobGranuleKeyHelpers::canSplit(keyB1, keyB5, 0, false), true);

	// Raw key comparison if truncation is disabled.
	ASSERT_EQ(BlobGranuleKeyHelpers::canSplit("abc"_sr, "abc1"_sr, 0, false), true);
	ASSERT_EQ(BlobGranuleKeyHelpers::canSplit("abc1"_sr, "abc"_sr, 0, false), false);
	ASSERT_EQ(BlobGranuleKeyHelpers::canSplit("abc"_sr, "abc"_sr, 0, false), false);

	return Void();
}
