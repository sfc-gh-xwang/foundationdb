/*
 * GrvProxyTagThrottler.h
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

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/ThroughputTracker.h"
#include "fdbserver/GrvTransactionRateInfo.h"
#include "fdbserver/LatencyBandsMap.h"

// GrvProxyTagThrottler is used to throttle GetReadVersionRequests based on throttlingId quotas
// before they're pushed into priority-partitioned queues.
//
// A GrvTransactionRateInfo object and a request queue are maintained for each throttlingId.
// The GrvTransactionRateInfo object is used to determine when a request can be released.
//
// Between each set of waits, releaseTransactions is run, releasing queued transactions
// that have passed the throttlingId throttling stage. Transactions that are not yet ready
// are requeued during releaseTransactions.
class GrvProxyTagThrottler {
	class DelayedRequest {
		static uint64_t lastSequenceNumber;
		double startTime;

	public:
		GetReadVersionRequest req;
		uint64_t sequenceNumber;

		explicit DelayedRequest(GetReadVersionRequest const& req)
		  : startTime(now()),
		    req(req),
		    sequenceNumber(++lastSequenceNumber) {}

		void updateProxyTagThrottledDuration(LatencyBandsMap&);
		bool isMaxThrottled(double maxThrottleDuration) const;
	};

	// Create a GrvTransactionRateInfo with the correct units and limits.
	// If track-average-cost-on-client is enabled, then all the rate into
	// will be in bytes per second, otherwise it will be in transactions per second.
	static GrvTransactionRateInfo createGrvTransactionRateInfo(double rate);

	class TagQueue {
		Deque<DelayedRequest> lowPriorityRequests, highPriorityRequests;
		static uint64_t rejectMaxThrottledRequests(Deque<DelayedRequest>& requests,
		                                           double maxThrottledDuration,
		                                           LatencyBandsMap&);
		uint64_t prioritizedReleased{ 0 }, fifoReleased{ 0 };
		Optional<GrvTransactionRateInfo> rateInfo;

	public:
		enum class Strategy {
			PRIORITIZED,
			FIFO,
		};

	private:
		bool processingHighPriorityRequests(Strategy) const;
		Deque<DelayedRequest> const& requestsWithFront(Strategy) const&;
		Deque<DelayedRequest>& requestsWithFront(Strategy) &;

	public:
		TagQueue() = default;
		explicit TagQueue(double rate) : rateInfo(createGrvTransactionRateInfo(rate)) {}

		DelayedRequest const& front(Strategy) const&;
		DelayedRequest& front(Strategy) &;
		void popFront(Strategy);
		void addRequest(GetReadVersionRequest const&);
		bool highPriorityRequestsRemaining() const;
		bool isEmpty() const;
		double getExcessPrioritizedCost(double starvationToleranceRatio) const;

		// Returns the number of requests rejected
		uint64_t rejectMaxThrottledRequests(double maxThrottleDuration, LatencyBandsMap&);

		Optional<int64_t> costAllowedToStart() const;
		bool isRateInfoPresent() const;
		void resetRateInfo();
		void setRate(double rate);
		void startReleaseWindow();
		void endReleaseWindow(uint64_t numStarted, double elapsed);
	};

	// Track the budgets for each throttlingId
	ThrottlingIdMap<TagQueue> queues;

	double maxThrottleDuration;
	double starvationToleranceRatio;

	// Track latency bands for each throttlingId
	LatencyBandsMap latencyBandsMap;

public:
	explicit GrvProxyTagThrottler(double maxThrottleDuration, double starvationToleranceRatio);

	// Called with rates received from ratekeeper
	void updateRates(ThrottlingIdMap<double> const& newRates);

	struct ReleaseTransactionsResult {
		uint64_t batchPriorityTransactionsReleased{ 0 };
		uint64_t batchPriorityRequestsReleased{ 0 };
		uint64_t lowPriorityTransactionsReleased{ 0 };
		uint64_t lowPriorityRequestsReleased{ 0 };
		uint64_t defaultPriorityTransactionsReleased{ 0 };
		uint64_t defaultPriorityRequestsReleased{ 0 };
		uint64_t rejectedRequests{ 0 };
	};

	// elapsed indicates the amount of time since the last epoch was run.
	// If a request is ready to be executed, it is sent to the deque
	// corresponding to its priority. If not, the request remains queued.
	ReleaseTransactionsResult releaseTransactions(double elapsed,
	                                              Deque<GetReadVersionRequest>& outBatchPriority,
	                                              Deque<GetReadVersionRequest>& outDefaultPriority);

	// Returns true if and only if the request was added
	bool tryAddRequest(GetReadVersionRequest const&);

	void addLatencyBandThreshold(double value);

public: // testing
	// Returns number of throttlingIds tracked
	uint32_t size() const;
};
