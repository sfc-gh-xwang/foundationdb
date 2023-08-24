/*
 * RangeLock.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/RangeLock.h"
std::string RangeLockInfo::toString() const {
	return fmt::format("lockId={} status={}", lockId.toString(), RangeLockInfo::statusString(status));
}

json_spirit::mObject RangeLockInfo::toJson() const {
	json_spirit::mObject doc;
	doc["lockId"] = lockId.toString();
	doc["status"] = RangeLockInfo::statusString(status);
	return doc;
}
namespace RangeLockAPI {

const KeyRangeRef rangeLockKeys("\xff/rangeLock/"_sr, "\xff/rangeLock0"_sr);
const KeyRef rangeLockMetadataPrefix = "\xff\x02/rangeLockMetadata/"_sr;

RangeLockMap& rangeLockMap() {
	static RangeLockMap instance(
	    SystemKey(rangeLockKeys.begin), {}, ObjectCodec<RangeLockInfo, _IncludeVersion>(IncludeVersion()));
	return instance;
}

RangeLockMetadata& rangeLockMetadata() {
	static RangeLockMetadata instance;
	return instance;
}

KeyBackedBinaryValue<int> RangeLockMetadata::rangeLockCount() const {
	static KeyBackedBinaryValue<int> count(subspace.pack("rangeLockCount"_sr));
	return count;
}

bool isRangeLockError(const Error& e) {
	static std::unordered_set<int> errors{ error_code_illegal_range_lock_param,
		                                   error_code_illegal_locked_range_access,
		                                   error_code_range_lock_limit_exceeded,
		                                   error_code_illegal_range_lock_operation,
		                                   error_code_unsupported_operation };
	return errors.find(e.code()) != errors.end();
}

} // namespace RangeLockAPI