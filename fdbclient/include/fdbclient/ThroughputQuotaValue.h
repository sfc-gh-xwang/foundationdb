/**
 * ThroughputQuotaValue.h
 */

#pragma once

#include "fdbclient/Tuple.h"

namespace ThrottleApi {

class ThroughputQuotaValue {
public:
	int64_t reservedQuota{ 0 };
	int64_t totalQuota{ 0 };
	bool isValid() const;
	Tuple pack() const;
	static ThroughputQuotaValue unpack(Tuple const& val);
	bool operator==(ThroughputQuotaValue const&) const;
};

} // namespace ThrottleApi
