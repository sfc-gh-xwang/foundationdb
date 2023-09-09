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

GrvProxyTagThrottler::DelayedRequest const& GrvProxyTagThrottler::TagQueue::front() const& {
	if (!highPriorityRequests.empty()) {
		return highPriorityRequests.front();
	} else {
		return lowPriorityRequests.front();
	}
}

GrvProxyTagThrottler::DelayedRequest& GrvProxyTagThrottler::TagQueue::front() & {
	if (!highPriorityRequests.empty()) {
		return highPriorityRequests.front();
	} else {
		return lowPriorityRequests.front();
	}
}

void GrvProxyTagThrottler::TagQueue::popFront() {
	if (!highPriorityRequests.empty()) {
		highPriorityRequests.pop_front();
	} else {
		lowPriorityRequests.pop_front();
	}
}

void GrvProxyTagThrottler::TagQueue::addRequest(GetReadVersionRequest const& req) {
	if (req.priority == TransactionPriority::DEFAULT) {
		highPriorityRequests.emplace_back(req);
	} else {
		lowPriorityRequests.emplace_back(req);
	}
}

bool GrvProxyTagThrottler::TagQueue::isEmpty() const {
	return lowPriorityRequests.empty() && highPriorityRequests.empty();
}

uint64_t GrvProxyTagThrottler::TagQueue::rejectMaxThrottledRequests(Deque<DelayedRequest>& requests,
                                                                    double maxThrottleDuration,
                                                                    LatencyBandsMap& latencyBandsMap) {
	uint64_t result = 0;
	while (!requests.empty()) {
		auto& delayedReq = requests.front();
		if (delayedReq.isMaxThrottled(maxThrottleDuration)) {
			CODE_PROBE(true, "GrvProxyTagThrottler rejecting requests");
			delayedReq.updateProxyTagThrottledDuration(latencyBandsMap);
			delayedReq.req.reply.sendError(proxy_tag_throttled());
			requests.pop_front();
		} else {
			break;
		}
	}
	return result;
}

uint64_t GrvProxyTagThrottler::TagQueue::rejectMaxThrottledRequests(double maxThrottleDuration,
                                                                    LatencyBandsMap& latencyBandsMap) {
	auto result = rejectMaxThrottledRequests(highPriorityRequests, maxThrottleDuration, latencyBandsMap);
	result += rejectMaxThrottledRequests(lowPriorityRequests, maxThrottleDuration, latencyBandsMap);
	return result;
}

bool GrvProxyTagThrottler::TagQueue::canStart(int64_t costAlreadyReleased, int64_t cost) const {
	return !rateInfo.present() || rateInfo.get().canStart(costAlreadyReleased, cost);
}

bool GrvProxyTagThrottler::TagQueue::isRateInfoPresent() const {
	return rateInfo.present();
}

void GrvProxyTagThrottler::TagQueue::resetRateInfo() {
	rateInfo.reset();
}

void GrvProxyTagThrottler::TagQueue::setRate(double rate) {
	if (rateInfo.present()) {
		rateInfo.get().setRate(rate);
	} else {
		rateInfo = createGrvTransactionRateInfo(rate);
	}
}

void GrvProxyTagThrottler::TagQueue::startReleaseWindow() {
	if (rateInfo.present()) {
		rateInfo.get().startReleaseWindow();
	}
}

void GrvProxyTagThrottler::TagQueue::endReleaseWindow(uint64_t numStarted, double elapsed) {
	if (rateInfo.present()) {
		CODE_PROBE(isEmpty(), "Tag queue ending release window with empty request queue");
		CODE_PROBE(!isEmpty(), "Tag queue ending release window with requests still queued");
		rateInfo.get().endReleaseWindow(numStarted, isEmpty(), elapsed);
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
			queue.resetRateInfo();
		}
	}
	boost::unordered::erase_if(queues,
	                           [](const auto& kv) { return kv.second.isEmpty() && !kv.second.isRateInfoPresent(); });
}

void GrvProxyTagThrottler::addRequest(GetReadVersionRequest const& req) {
	queues[req.throttlingId.get()].addRequest(req);
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

	class Priority {
		bool highPriorityRequestsRemaining;
		uint64_t nextSequenceNumber;

	public:
		Priority(bool highPriorityRequestsRemaining, uint64_t nextSequenceNumber)
		  : highPriorityRequestsRemaining(highPriorityRequestsRemaining),
		    nextSequenceNumber(nextSequenceNumber) {}

		bool operator<(Priority const& rhs) const {
			if (highPriorityRequestsRemaining != rhs.highPriorityRequestsRemaining) {
				return !highPriorityRequestsRemaining;
			} else {
				return nextSequenceNumber > rhs.nextSequenceNumber;
			}
		}

		bool operator>(Priority const& rhs) const { return rhs < *this; }

		static Priority lowest() { return Priority(false, std::numeric_limits<uint64_t>::max()); }
	};

	// Pointer to a TagQueue with some extra metadata stored alongside
	class TagQueueHandle {
		// Store pointers here to avoid frequent hash map lookups
		TagQueue* queue;
		// Cannot be stored directly because amountReleased outlives TagQueueHandle
		uint64_t* amountReleased;

	public:
		Priority getPriority() const {
			ASSERT(!queue->isEmpty());
			auto& delayedReq = queue->front();
			bool const highPriorityRequestsRemaining = (delayedReq.req.priority == TransactionPriority::DEFAULT);
			return Priority(highPriorityRequestsRemaining, delayedReq.sequenceNumber);
		}
		DelayedRequest const& front() const& { return queue->front(); }
		DelayedRequest& front() & { return queue->front(); }
		void popFront() { queue->popFront(); }
		bool isEmpty() const { return queue->isEmpty(); }
		bool canStart(int64_t cost) const { return queue->canStart(*amountReleased, cost); }
		uint64_t rejectMaxThrottledRequests(double maxThrottleDuration, LatencyBandsMap& latencyBandsMap) {
			return queue->rejectMaxThrottledRequests(maxThrottleDuration, latencyBandsMap);
		}
		void incrementReleased(int cost) { (*amountReleased) += cost; }
		bool operator<(TagQueueHandle const& rhs) const { return getPriority() < rhs.getPriority(); }
		explicit TagQueueHandle(TagQueue& queue, uint64_t& amountReleased)
		  : queue(&queue),
		    amountReleased(&amountReleased) {
			ASSERT(!this->queue->isEmpty());
		}
	};

	// Priority queue of queues for each tag, ordered by the sequence number of the
	// next request to process in each queue
	std::priority_queue<TagQueueHandle> pqOfQueues;

	// Track transactions released for each throttlingId
	std::vector<std::pair<ThrottlingId, uint64_t>> costReleased;
	costReleased.reserve(queues.size());
	auto const costReleasedInitialCapacity = costReleased.capacity();

	for (auto& [throttlingId, queue] : queues) {
		queue.startReleaseWindow();
		if (!queue.isEmpty()) {
			// First place the count in the costReleased object,
			// then pass a reference to the count to the TagQueueHandle object
			// emplaced into pqOfQueues.
			//
			// Because we've reserved enough space in transactionsReleased
			// to avoid resizing, this reference should remain valid.
			// This allows each TagQueueHandle to update its amountReleased
			// counter without incurring the cost of a hash map lookup.
			auto& [_, count] = costReleased.emplace_back(throttlingId, 0);
			pqOfQueues.emplace(queue, count);
		}
	}

	while (!pqOfQueues.empty()) {
		auto tagQueueHandle = pqOfQueues.top();
		pqOfQueues.pop();
		// Used to determine when it is time to start processing another throttlingId
		auto const nextQueuePriority = pqOfQueues.empty() ? Priority::lowest() : pqOfQueues.top().getPriority();

		while (!tagQueueHandle.isEmpty()) {
			auto& delayedReq = tagQueueHandle.front();
			auto const cost = getCost(delayedReq.req);

			if (!tagQueueHandle.canStart(cost)) {
				// Cannot release any more transaction from this throttling ID (don't push the tag queue handle back
				// into pqOfQueues)
				CODE_PROBE(true, "GrvProxyTagThrottler throttling transaction");
				// Requests in this queue have been throttled too long and errors
				// should be sent to clients.
				result.rejectedRequests +=
				    tagQueueHandle.rejectMaxThrottledRequests(maxThrottleDuration, latencyBandsMap);
				break;
			} else {
				if (tagQueueHandle.getPriority() > nextQueuePriority) {
					// Releasing transactions
					tagQueueHandle.incrementReleased(cost);
					delayedReq.updateProxyTagThrottledDuration(latencyBandsMap);
					if (delayedReq.req.priority == TransactionPriority::BATCH) {
						result.batchPriorityTransactionsReleased += delayedReq.req.transactionCount;
						++result.batchPriorityRequestsReleased;
						outBatchPriority.push_back(delayedReq.req);
					} else if (delayedReq.req.priority == TransactionPriority::DEFAULT ||
					           delayedReq.req.priority == TransactionPriority::LOW) {
						result.defaultPriorityTransactionsReleased += delayedReq.req.transactionCount;
						++result.defaultPriorityRequestsReleased;
						outDefaultPriority.push_back(delayedReq.req);
					} else {
						// Immediate priority transactions should bypass the GrvProxyTagThrottler
						ASSERT(false);
					}
					tagQueueHandle.popFront();
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
		ThrottlingIdMap<uint64_t> costReleasedMap;
		for (const auto& [throttlingId, count] : costReleased) {
			costReleasedMap[throttlingId] = count;
		}
		for (auto& [throttlingId, queue] : queues) {
			queue.endReleaseWindow(costReleasedMap[throttlingId], elapsed);
		}
	}
	// If the capacity is increased, that means the vector has been illegally resized, potentially
	// corrupting memory
	ASSERT_EQ(costReleased.capacity(), costReleasedInitialCapacity);

	return result;
}

void GrvProxyTagThrottler::addLatencyBandThreshold(double value) {
	CODE_PROBE(size() > 0, "GrvProxyTagThrottler adding latency bands while actively throttling");
	latencyBandsMap.addThreshold(value);
}

uint32_t GrvProxyTagThrottler::size() const {
	return queues.size();
}
