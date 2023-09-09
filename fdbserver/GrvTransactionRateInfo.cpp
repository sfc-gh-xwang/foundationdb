/*
 * GrvTransactionRateInfo.cpp
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

#include "fdbserver/GrvTransactionRateInfo.h"

GrvTransactionRateInfo::GrvTransactionRateInfo(double rateWindow,
                                               double maxEmptyQueueBudget,
                                               double rate,
                                               double maxToStart)
  : rateWindow(rateWindow),
    maxEmptyQueueBudget(maxEmptyQueueBudget),
    rate(rate),
    maxToStart(maxToStart),
    smoothRate(rateWindow),
    smoothReleased(rateWindow) {
	smoothRate.setTotal(rate);
}

bool GrvTransactionRateInfo::canStart(int64_t costAlreadyReleased, int64_t cost) const {
	return costAlreadyReleased + cost <= std::min(limit + budget, maxToStart);
}

void GrvTransactionRateInfo::endReleaseWindow(int64_t numStarted, bool queueEmpty, double elapsed) {
	// Update the budget to accumulate any extra capacity available or remove any excess that was used.
	// The actual delta is the portion of the limit we didn't use multiplied by the fraction of the rate window that
	// elapsed.
	//
	// We may have exceeded our limit due to the budget or because of higher priority transactions, in which case
	// this delta will be negative. The delta can also be negative in the event that our limit was negative, which
	// can happen if we had already started more transactions in our rate window than our rate would have allowed.
	//
	// This budget has the property that when the budget is required to start transactions (because batches are
	// big), the sum limit+budget will increase linearly from 0 to the batch size over time and decrease by the
	// batch size upon starting a batch. In other words, this works equivalently to a model where we linearly
	// accumulate budget over time in the case that our batches are too big to take advantage of the rate window based
	// limits.
	//
	// Note that "rate window" here indicates a period of rateWindow seconds,
	// whereas "release window" is the period between wait statements, with duration indicated by "elapsed."
	budget = std::max(0.0, budget + elapsed * (limit - numStarted) / rateWindow);

	// If we are emptying out the queue of requests, then we don't need to carry much budget forward
	// If we did keep accumulating budget, then our responsiveness to changes in workflow could be compromised
	if (queueEmpty) {
		budget = std::min(budget, maxEmptyQueueBudget);
	}

	smoothReleased.addDelta(numStarted);
}

void GrvTransactionRateInfo::disable() {
	disabled = true;
	// Use smoothRate.setTotal(0) instead of setting rate to 0 so txns will not be throttled immediately.
	smoothRate.setTotal(0);
}

void GrvTransactionRateInfo::setRate(double rate) {
	ASSERT(rate >= 0 && rate != std::numeric_limits<double>::infinity() && !std::isnan(rate));

	this->rate = rate;
	if (disabled) {
		smoothRate.reset(rate);
		disabled = false;
	} else {
		smoothRate.setTotal(rate);
	}
}

void GrvTransactionRateInfo::startReleaseWindow() {
	// Determine the number of transaction cost that this proxy is allowed to release
	// Roughly speaking, this is done by computing the cost over some historical window that we
	// could have released but didn't, and making that our limit. More precisely, we track a smoothed rate limit and
	// release rate, the difference of which is the rate of additional cost that we could have released
	// based on that window. Then we multiply by the window size to get a cost limit.
	//
	// Limit can be negative in the event that we are releasing more transactions than we are allowed (due to the
	// use of our budget or because of higher priority transactions).
	double releaseRate = smoothRate.smoothTotal() - smoothReleased.smoothRate();
	limit = rateWindow * releaseRate;
}
