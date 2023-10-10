/*
 * BulkLoadApiImpl.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/BulkLoadApiImpl.h"
#include "fdbclient/BlobGranuleApiImpl.h"
#include "fdbclient/ApiRequestHandler.h"
#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<ApiResult> startBulkLoadImpl(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBStartBulkLoadRequest>();
	BulkLoadID id = wait(db->startBulkLoad((KeyRangeRef&)input->key_range, tenant));

	auto res = TypedApiResult<FDBBulkLoadIDResult>::create();
	Arena& arena = res.arena();
	arena.dependsOn(id.arena());

	auto resp = res.getPtr();
	resp->id.begin = id.begin();
	resp->id.length = id.size();
	return res;
}

Future<ApiResult> startBulkLoad(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return startBulkLoadImpl(db, req, tenant);
}

ACTOR Future<ApiResult> bulkLoadAllFilesRegisteredImpl(DatabaseContext* db,
                                                       ApiRequest req,
                                                       Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBBulkLoadAllFilesRegisteredRequest>();
	BulkLoadID id((StringRef&)input->id);
	wait(db->bulkLoadAllFilesRegistered(id, tenant));

	return TypedApiResult<FDBVoidResult>::create();
}

Future<ApiResult> bulkLoadAllFilesRegistered(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return bulkLoadAllFilesRegisteredImpl(db, req, tenant);
}

ACTOR Future<ApiResult> abortBulkLoadImpl(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBAbortBulkLoadRequest>();
	BulkLoadID id((StringRef&)input->id);
	wait(db->abortBulkLoad(id, tenant));

	return TypedApiResult<FDBVoidResult>::create();
}

Future<ApiResult> abortBulkLoad(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return abortBulkLoadImpl(db, req, tenant);
}

ACTOR Future<ApiResult> cleanupBulkLoadImpl(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBCleanupBulkLoadRequest>();
	BulkLoadID id((StringRef&)input->id);
	wait(db->cleanupBulkLoad(id, tenant));

	return TypedApiResult<FDBVoidResult>::create();
}

Future<ApiResult> cleanupBulkLoad(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return cleanupBulkLoadImpl(db, req, tenant);
}

ACTOR Future<ApiResult> waitBulkLoadCompleteImpl(DatabaseContext* db,
                                                 ApiRequest req,
                                                 Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBWaitBulkLoadCompleteRequest>();
	BulkLoadID id((StringRef&)input->id);
	wait(db->waitBulkLoadComplete(id, tenant));

	return TypedApiResult<FDBVoidResult>::create();
}

Future<ApiResult> waitBulkLoadComplete(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return waitBulkLoadCompleteImpl(db, req, tenant);
}

void nativeToApiFileToWrite(FDBBulkLoadFileToWrite* dest, const BulkLoadFileToWriteRef& source, Arena& ar) {
	dest->file_id.begin = source.fileId.begin();
	dest->file_id.length = source.fileId.size();
	dest->filename.begin = source.fileName.begin();
	dest->filename.length = source.fileName.size();
	dest->target_size_bytes = source.targetSize;
	dest->file_type = static_cast<uint8_t>(source.type);
	dest->file_format = static_cast<uint8_t>(source.format);
	dest->compression = static_cast<uint8_t>(source.compression);

	// FIXME: actually set tenant prefix!
	dest->tenant_prefix.present = false;
	if (source.tenantPrefix.present()) {
		dest->tenant_prefix.present = true;
		dest->tenant_prefix.prefix.key = source.tenantPrefix->begin();
		dest->tenant_prefix.prefix.key_length = source.tenantPrefix->size();
	}

	dest->encryption_ctx = nullptr;
	if (source.cipherKeysCtx.present()) {
		dest->encryption_ctx = new (ar) FDBBGEncryptionCtxV2();
		nativeToApiBGEncryptionKeyCtx(dest->encryption_ctx, source.cipherKeysCtx.get(), ar);
	}
}

ACTOR Future<ApiResult> bulkLoadFilesToWriteImpl(DatabaseContext* db,
                                                 ApiRequest req,
                                                 Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBBulkLoadFilesToWriteRequest>();
	BulkLoadID id((StringRef&)input->id);
	Standalone<VectorRef<BulkLoadFileToWriteRef>> files = wait(db->getBulkLoadFilesToWrite(id, input->count, tenant));

	auto res = TypedApiResult<FDBBulkLoadFilesToWriteResult>::create();

	Arena& arena = res.arena();
	arena.dependsOn(files.arena());

	auto resp = res.getPtr();
	resp->files_count = files.size();
	resp->files_arr = new (arena) FDBBulkLoadFileToWrite*[files.size()];

	for (int fileIdx = 0; fileIdx < files.size(); fileIdx++) {
		const BulkLoadFileToWriteRef& file = files[fileIdx];
		resp->files_arr[fileIdx] = new (arena) FDBBulkLoadFileToWrite();
		nativeToApiFileToWrite(resp->files_arr[fileIdx], file, arena);
	}
	return res;
}

Future<ApiResult> bulkLoadFilesToWrite(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return bulkLoadFilesToWriteImpl(db, req, tenant);
}

void apiToNativeFileWritten(BulkLoadWrittenFileRef& dest, FDBBulkLoadWrittenFile const* source) {
	dest.fileId = StringRef(source->file_id.begin, source->file_id.length);
	dest.keyRange = KeyRangeRef(StringRef(source->key_range.begin_key, source->key_range.begin_key_length),
	                            StringRef(source->key_range.end_key, source->key_range.end_key_length));
	dest.logicalSize = source->logical_size;
	dest.physicalSize = source->physical_size;
}

ACTOR Future<ApiResult> bulkLoadFilesWrittenImpl(DatabaseContext* db,
                                                 ApiRequest req,
                                                 Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBBulkLoadFilesWrittenRequest>();
	BulkLoadID id((StringRef&)input->id);

	// only need shallow copy because request still owns memory, just type conversion for native api
	Standalone<VectorRef<BulkLoadWrittenFileRef>> files;
	files.reserve(files.arena(), input->files_count);
	for (int i = 0; i < input->files_count; i++) {
		BulkLoadWrittenFileRef fileWritten;
		apiToNativeFileWritten(fileWritten, input->files_arr[i]);
		files.push_back(files.arena(), fileWritten);
	}

	wait(db->markBulkLoadFilesWritten(id, files, tenant));

	return TypedApiResult<FDBVoidResult>::create();
}

Future<ApiResult> bulkLoadFilesWritten(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return bulkLoadFilesWrittenImpl(db, req, tenant);
}

ACTOR Future<ApiResult> listBulkLoadsImpl(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBListBulkLoadsRequest>();
	Standalone<VectorRef<BulkLoadIDRef>> ids = wait(db->listBulkLoads(false, (KeyRangeRef&)input->key_range, tenant));

	auto res = TypedApiResult<FDBListBulkLoadsResult>::create();

	Arena& arena = res.arena();
	arena.dependsOn(ids.arena());

	auto resp = res.getPtr();
	resp->ids_count = ids.size();
	resp->ids_arr = new (arena) FDBBulkLoadID*[ids.size()];

	for (int idx = 0; idx < ids.size(); idx++) {
		const BulkLoadIDRef& id_in = ids[idx];
		resp->ids_arr[idx] = new (arena) FDBBulkLoadID();
		auto& id_out = resp->ids_arr[idx];
		id_out->begin = id_in.begin();
		id_out->length = id_in.size();
	}
	return res;
}

Future<ApiResult> listBulkLoads(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return listBulkLoadsImpl(db, req, tenant);
}

uint8_t getUserFacingBulkLoadState(const BulkLoadStatus& inStatus) {
	switch (inStatus.stateCode) {
	case BulkLoadStatus::State::STARTED:
		return FDB_BULK_LOAD_ADDING_FILES;
	case BulkLoadStatus::State::STAGING:
	case BulkLoadStatus::State::STAGING_BLOB_VALIDATED:
	case BulkLoadStatus::State::STAGING_BLOB_DONE:
	case BulkLoadStatus::State::STAGING_DDPREPARED:
		return FDB_BULK_LOAD_COPYING;
	case BulkLoadStatus::State::DONE:
		return FDB_BULK_LOAD_DONE;
	case BulkLoadStatus::State::ABORTED:
	case BulkLoadStatus::State::ABORTED_COMPLETE:
		return FDB_BULK_LOAD_ABORTED;
	default:
		return FDB_BULK_LOAD_UNKNOWN;
	}
}

ACTOR Future<ApiResult> getBulkLoadStatusImpl(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	auto input = req.getTypedRequest<FDBGetBulkLoadStatusRequest>();
	BulkLoadID id((StringRef&)input->id);
	BulkLoadStatus status = wait(db->getBulkLoadStatus(id, tenant));

	auto res = TypedApiResult<FDBGetBulkLoadStatusResult>::create();

	auto resp = res.getPtr();
	resp->status.state = getUserFacingBulkLoadState(status);
	return res;
}

Future<ApiResult> getBulkLoadStatus(DatabaseContext* db, ApiRequest req, Optional<Reference<Tenant>> tenant) {
	return getBulkLoadStatusImpl(db, req, tenant);
}