/*
 * SSCommitCostCounter.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbserver/SSCommitCostCounter.h"
#include "flow/IRandom.h"

void SSCommitCostCounter::addSSTagCost(const UID& id, const TransactionTag& throttlingTag, uint64_t cost) {
	auto [it, _] = commitCost.try_emplace(id, TransactionTagMap<TransactionCommitCostEstimation>());

	auto& costItem = it->second[throttlingTag];

	costItem.opsSum++;
	costItem.costSum += cost;
}

UIDTransactionTagMap<TransactionCommitCostEstimation> SSCommitCostCounter::getTopBusyTagCost(double elapsed) {
	UIDTransactionTagMap<TransactionCommitCostEstimation> result;

	for (const auto& [ssID, tagCostMap] : commitCost) {
		std::priority_queue<TagCostInfo, std::vector<TagCostInfo>, std::greater<TagCostInfo>> topKTags;

		for (const auto& [tag, cost] : tagCostMap) {
			if ((cost.getCostSum() / elapsed) < minCostTracked) {
				continue;
			}
			if (topKTags.size() < maxTagsTracked) {
				topKTags.emplace(tag, cost);
			} else if (topKTags.top().costEst.getCostSum() < cost.getCostSum()) {
				topKTags.pop();
				topKTags.emplace(tag, cost);
			}
		}

		while (!topKTags.empty()) {
			result[ssID].emplace(topKTags.top().throttlingTag, topKTags.top().costEst);
			topKTags.pop();
		}
	}

	commitCost.clear();

	return result;
}

TEST_CASE("/fdbserver/SSCommitCostCounter/IgnoreBeyondMaxTracked") {

	SSCommitCostCounter counter(/*maxTagsTracked=*/2, /*minCostTracked=*/10);
	ASSERT_EQ(counter.getTopBusyTagCost(1).size(), 0);

	const UID uid;
	counter.addSSTagCost(uid, "throttlingTagA"_sr, 10);
	counter.addSSTagCost(uid, "throttlingTagA"_sr, 10);
	counter.addSSTagCost(uid, "throttlingTagB"_sr, 15);
	counter.addSSTagCost(uid, "throttlingTagC"_sr, 20);

	auto const result = counter.getTopBusyTagCost(1);

	ASSERT(result.contains(uid));
	ASSERT_EQ(result.at(uid).size(), 2);

	ASSERT(result.at(uid).contains("throttlingTagA"_sr));
	ASSERT(!result.at(uid).contains("throttlingTagB"_sr));
	ASSERT(result.at(uid).contains("throttlingTagC"_sr));

	// SSCommitCostCounter::getTopBusyTagCost clears the tag costs, a second call should return empty result.
	auto const result2 = counter.getTopBusyTagCost(1);
	ASSERT(result2.empty());

	return Void();
}

TEST_CASE("/fdbserver/SSCommitCostCounter/IgnoreBelowMinCost") {
	SSCommitCostCounter counter(/*maxTagsTracked=*/2, /*minCostTracked=*/10);
	ASSERT_EQ(counter.getTopBusyTagCost(1).size(), 0);

	const UID uid;
	counter.addSSTagCost(uid, "throttlingTagA"_sr, 5);

	auto const result = counter.getTopBusyTagCost(1);
	ASSERT(result.empty());

	return Void();
}