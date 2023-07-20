/*
 * QuotaThrottler.h
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

#pragma once

#include "fdbclient/PImpl.h"
#include "fdbclient/ThrottlingId.h"
#include "fdbserver/IRKMetricsTracker.h"
#include "fdbserver/IRKThroughputQuotaCache.h"
#include <unordered_set>

class QuotaThrottler {
	PImpl<class QuotaThrottlerImpl> impl;

public:
	QuotaThrottler(UID id, int maxFallingBehind, double limitingThreshold);
	~QuotaThrottler();

	// Periodically forget throttlingIds reporting low throughput
	Future<Void> expireUnusedThrottlingIds();

	// Increment the number of known requests associated with the specified throttling ID
	void addRequests(ThrottlingId const&, int count);

	// Number of throttling IDs for which throttling information is a rate is being
	// sent to GRV proxies
	int64_t throttleCount() const;

	// For each throttling ID and priority combination, return the throughput limit in
	// transactions per second for the cluster (to be shared across all GRV proxies).
	ThrottlingIdMap<double> getTargetRates(int numProxies,
	                                       IRKThroughputQuotaCache const&,
	                                       IRKMetricsTracker const&,
	                                       IRKClusterThroughputTracker const&,
	                                       RKServerThroughputTracker const&);

	// For each throttling ID and priority combination, return the throughput limit in
	// bytes per second for the cluster (to be shared across all GRV proxies).
	ThrottlingIdMap<double> getTargetThroughput(int numProxies,
	                                            IRKThroughputQuotaCache const&,
	                                            IRKMetricsTracker const&,
	                                            IRKClusterThroughputTracker const&,
	                                            RKServerThroughputTracker const&);

	boost::unordered::unordered_flat_set<ThrottlingId> getBusyThrottlingIds() const;

	// Testing only:
public:
	void removeExpiredThrottlingIds();
	uint32_t throttlingIdsTracked() const;
};
