/*
 * GrvProxyTagThrottler.cpp
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

#include "fdbclient/Knobs.h"
#include "fdbclient/ThroughputTracker.h"
#include "fdbserver/GrvProxyTagThrottler.h"
#include "fdbserver/Knobs.h"
#include <boost/unordered/unordered_flat_map.hpp>

uint64_t GrvProxyTagThrottler::DelayedRequest::lastSequenceNumber = 0;

void GrvProxyTagThrottler::DelayedRequest::updateProxyTagThrottledDuration(LatencyBandsMap& latencyBandsMap) {
	req.proxyTagThrottledDuration = now() - startTime;
	latencyBandsMap.addMeasurement(req.throttlingId.get(), req.proxyTagThrottledDuration, req.transactionCount);
}

bool GrvProxyTagThrottler::DelayedRequest::isMaxThrottled(double maxThrottleDuration) const {
	return now() - startTime > maxThrottleDuration;
}

GrvTransactionRateInfo GrvProxyTagThrottler::createGrvTransactionRateInfo(double rate) {
	return GrvTransactionRateInfo(
	    SERVER_KNOBS->TAG_THROTTLE_RATE_WINDOW,
	    isClientAverageCostTrackingEnabled() ? SERVER_KNOBS->TAG_THROTTLE_MAX_EMPTY_QUEUE_BYTES_BUDGET
	                                         : SERVER_KNOBS->TAG_THROTTLE_MAX_EMPTY_QUEUE_BUDGET,
	    rate,
	    isClientAverageCostTrackingEnabled() ? SERVER_KNOBS->START_TRANSACTION_MAX_BYTES_TO_START
	                                         : SERVER_KNOBS->START_TRANSACTION_MAX_TRANSACTIONS_TO_START);
}

void GrvProxyTagThrottler::TagQueue::setRate(double rate) {
	if (rateInfo.present()) {
		rateInfo.get().setRate(rate);
	} else {
		rateInfo = createGrvTransactionRateInfo(rate);
	}
}

bool GrvProxyTagThrottler::TagQueue::isMaxThrottled(double maxThrottleDuration) const {
	return !requests.empty() && requests.front().isMaxThrottled(maxThrottleDuration);
}

void GrvProxyTagThrottler::TagQueue::rejectRequests(LatencyBandsMap& latencyBandsMap) {
	CODE_PROBE(true, "GrvProxyTagThrottler rejecting requests");
	while (!requests.empty()) {
		auto& delayedReq = requests.front();
		delayedReq.updateProxyTagThrottledDuration(latencyBandsMap);
		delayedReq.req.reply.sendError(proxy_tag_throttled());
		requests.pop_front();
	}
}

void GrvProxyTagThrottler::TagQueue::endReleaseWindow(uint64_t numStarted, double elapsed) {
	if (rateInfo.present()) {
		CODE_PROBE(requests.empty(), "Tag queue ending release window with empty request queue");
		CODE_PROBE(!requests.empty(), "Tag queue ending release window with requests still queued");
		rateInfo.get().endReleaseWindow(numStarted, requests.empty(), elapsed);
	}
}

GrvProxyTagThrottler::GrvProxyTagThrottler(double maxThrottleDuration)
  : maxThrottleDuration(maxThrottleDuration),
    latencyBandsMap("GrvProxyTagThrottler",
                    deterministicRandom()->randomUniqueID(),
                    SERVER_KNOBS->GLOBAL_TAG_THROTTLING_PROXY_LOGGING_INTERVAL,
                    SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED) {}

void GrvProxyTagThrottler::updateRates(ThrottlingIdMap<double> const& newRates) {
	for (const auto& [throttlingId, rate] : newRates) {
		auto it = queues.find(throttlingId);
		if (it == queues.end()) {
			queues[throttlingId] = TagQueue(rate);
		} else {
			it->second.setRate(rate);
		}
	}

	// Clean up throttling IDs that did not appear in newRates
	for (auto& [throttlingId, queue] : queues) {
		if (newRates.find(throttlingId) == newRates.end()) {
			queue.rateInfo.reset();
		}
	}
	boost::unordered::erase_if(
	    queues, [](const auto& kv) { return kv.second.requests.empty() && !kv.second.rateInfo.present(); });
}

void GrvProxyTagThrottler::addRequest(GetReadVersionRequest const& req) {
	queues[req.throttlingId.get()].requests.emplace_back(req);
}

uint64_t GrvProxyTagThrottler::getCost(GetReadVersionRequest const& req) const {

	if (isClientAverageCostTrackingEnabled()) {
		// track-average-cost-on-client is enabled use the expectedTransactionCost.
		if (req.expectedTotalCost.present()) {
			return static_cast<uint64_t>(req.expectedTotalCost.get());
		} else {
			// expectedTransactionCost not provided, use req.transactionCount * DEFAULT_EXPECTED_TRANSACTION_COST
			return req.transactionCount * CLIENT_KNOBS->DEFAULT_EXPECTED_TRANSACTION_COST;
		}
	} else {
		return req.transactionCount;
	}
}

GrvProxyTagThrottler::ReleaseTransactionsResult GrvProxyTagThrottler::releaseTransactions(
    double elapsed,
    Deque<GetReadVersionRequest>& outBatchPriority,
    Deque<GetReadVersionRequest>& outDefaultPriority) {
	ReleaseTransactionsResult result;

	// Pointer to a TagQueue with some extra metadata stored alongside
	struct TagQueueHandle {
		// Store pointers here to avoid frequent hash map lookups
		TagQueue* queue;
		// Cannot be stored directly because amountReleased outlives TagQueueHandle
		uint64_t* numReleased;
		uint64_t getNextSequenceNumber() const {
			ASSERT(!queue->requests.empty());
			return queue->requests.front().sequenceNumber;
		}
		bool operator>(TagQueueHandle const& rhs) const {
			return getNextSequenceNumber() > rhs.getNextSequenceNumber();
		}
		explicit TagQueueHandle(TagQueue& queue, uint64_t& numReleased) : queue(&queue), numReleased(&numReleased) {
			ASSERT(!this->queue->requests.empty());
		}
	};

	// Priority queue of queues for each tag, ordered by the sequence number of the
	// next request to process in each queue
	std::priority_queue<TagQueueHandle, std::vector<TagQueueHandle>, std::greater<TagQueueHandle>> pqOfQueues;

	// Track transactions released for each throttlingId
	std::vector<std::pair<ThrottlingId, uint64_t>> transactionsReleased;
	transactionsReleased.reserve(queues.size());
	auto const transactionsReleasedInitialCapacity = transactionsReleased.capacity();

	for (auto& [throttlingId, queue] : queues) {
		if (queue.rateInfo.present()) {
			queue.rateInfo.get().startReleaseWindow();
		}
		if (!queue.requests.empty()) {
			// First place the count in the transactionsReleased object,
			// then pass a reference to the count to the TagQueueHandle object
			// emplaced into pqOfQueues.
			//
			// Because we've reserved enough space in transactionsReleased
			// to avoid resizing, this reference should remain valid.
			// This allows each TagQueueHandle to update its number of
			// numReleased counter without incurring the cost of a hash map lookup.
			auto& [_, count] = transactionsReleased.emplace_back(throttlingId, 0);
			pqOfQueues.emplace(queue, count);
		}
	}

	while (!pqOfQueues.empty()) {
		auto tagQueueHandle = pqOfQueues.top();
		pqOfQueues.pop();
		// Used to determine when it is time to start processing another throttlingId
		auto const nextQueueSeqNo =
		    pqOfQueues.empty() ? std::numeric_limits<uint64_t>::max() : pqOfQueues.top().getNextSequenceNumber();

		while (!tagQueueHandle.queue->requests.empty()) {
			auto& delayedReq = tagQueueHandle.queue->requests.front();

			auto cost = getCost(delayedReq.req);

			ASSERT_EQ(tagQueueHandle.getNextSequenceNumber(), delayedReq.sequenceNumber);
			if (tagQueueHandle.queue->rateInfo.present() &&
			    !tagQueueHandle.queue->rateInfo.get().canStart(*(tagQueueHandle.numReleased), cost)) {
				// Cannot release any more transaction from this throttling ID (don't push the tag queue handle back
				// into pqOfQueues)
				CODE_PROBE(true, "GrvProxyTagThrottler throttling transaction");
				if (tagQueueHandle.queue->isMaxThrottled(maxThrottleDuration)) {
					// Requests in this queue have been throttled too long and errors
					// should be sent to clients.
					result.rejectedRequests += tagQueueHandle.queue->requests.size();
					tagQueueHandle.queue->rejectRequests(latencyBandsMap);
				}
				break;
			} else {
				if (tagQueueHandle.getNextSequenceNumber() < nextQueueSeqNo) {
					// Releasing transaction
					*(tagQueueHandle.numReleased) += cost;
					delayedReq.updateProxyTagThrottledDuration(latencyBandsMap);
					if (delayedReq.req.priority == TransactionPriority::BATCH) {
						result.batchPriorityTransactionsReleased += delayedReq.req.transactionCount;
						++result.batchPriorityRequestsReleased;
						outBatchPriority.push_back(delayedReq.req);
					} else if (delayedReq.req.priority == TransactionPriority::DEFAULT) {
						result.defaultPriorityTransactionsReleased += delayedReq.req.transactionCount;
						++result.defaultPriorityRequestsReleased;
						outDefaultPriority.push_back(delayedReq.req);
					} else {
						// Immediate priority transactions should bypass the GrvProxyTagThrottler
						ASSERT(false);
					}
					tagQueueHandle.queue->requests.pop_front();
				} else {
					CODE_PROBE(true, "GrvProxyTagThrottler switching throttlingIds to preserve FIFO");
					pqOfQueues.push(tagQueueHandle);
					break;
				}
			}
		}
	}

	// End release windows for all tag queues
	{
		ThrottlingIdMap<uint64_t> transactionsReleasedMap;
		for (const auto& [throttlingId, count] : transactionsReleased) {
			transactionsReleasedMap[throttlingId] = count;
		}
		for (auto& [throttlingId, queue] : queues) {
			queue.endReleaseWindow(transactionsReleasedMap[throttlingId], elapsed);
		}
	}
	// If the capacity is increased, that means the vector has been illegally resized, potentially
	// corrupting memory
	ASSERT_EQ(transactionsReleased.capacity(), transactionsReleasedInitialCapacity);

	return result;
}

void GrvProxyTagThrottler::addLatencyBandThreshold(double value) {
	CODE_PROBE(size() > 0, "GrvProxyTagThrottler adding latency bands while actively throttling");
	latencyBandsMap.addThreshold(value);
}

uint32_t GrvProxyTagThrottler::size() const {
	return queues.size();
}
