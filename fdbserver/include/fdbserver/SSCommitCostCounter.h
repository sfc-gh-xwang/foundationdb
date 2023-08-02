/*
 * SSCommitCostCounter.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbserver/RatekeeperInterface.h"

class SSCommitCostCounter {
	int maxTagsTracked;
	uint64_t minCostTracked;

	UIDTransactionTagMap<TransactionCommitCostEstimation> commitCost;

	struct TagCostInfo {
		TransactionTag throttlingTag;
		TransactionCommitCostEstimation costEst;

		TagCostInfo(TransactionTag const& throttlingTag, TransactionCommitCostEstimation const& costEst)
		  : throttlingTag(throttlingTag),
		    costEst(costEst) {}

		bool operator<(TagCostInfo const& rhs) const { return costEst.getCostSum() < rhs.costEst.getCostSum(); }
		bool operator>(TagCostInfo const& rhs) const { return costEst.getCostSum() > rhs.costEst.getCostSum(); }
	};

public:
	SSCommitCostCounter(int maxTagsTracked, uint64_t minCostTracked)
	  : maxTagsTracked(maxTagsTracked),
	    minCostTracked(minCostTracked) {}
	void addSSTagCost(const UID& id, const TransactionTag& throttlingTag, uint64_t cost);

	UIDTransactionTagMap<TransactionCommitCostEstimation> getTopBusyTagCost();
};
