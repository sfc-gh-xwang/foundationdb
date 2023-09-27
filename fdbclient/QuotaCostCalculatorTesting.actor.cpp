/**
 * IQuotaCostCalculatorTesting.actor.cpp
 */

#include "fdbclient/IQuotaCostCalculator.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

void forceLinkQuotaCostCalculatorTests() {}

TEST_CASE("/fdbclient/QuotaCostCalculator/Round") {
	auto const fungibilityRatio = 4.0;
	auto const pageSize = 1024;
	auto const calculator = IQuotaCostCalculator::create("round", fungibilityRatio, pageSize);
	ASSERT_EQ(calculator->getReadCost(0), pageSize);
	ASSERT_EQ(calculator->getWriteCost(0), fungibilityRatio * calculator->getReadCost(0));
	ASSERT_EQ(calculator->getReadCost(pageSize - 1), pageSize);
	ASSERT_EQ(calculator->getWriteCost(pageSize - 1), fungibilityRatio * calculator->getReadCost(pageSize - 1));
	ASSERT_EQ(calculator->getReadCost(pageSize), pageSize);
	ASSERT_EQ(calculator->getWriteCost(pageSize), fungibilityRatio * calculator->getReadCost(pageSize));
	ASSERT_EQ(calculator->getReadCost(pageSize + 1), 2 * pageSize);
	ASSERT_EQ(calculator->getWriteCost(pageSize + 1), fungibilityRatio * calculator->getReadCost(pageSize + 1));
	ASSERT_EQ(calculator->getReadCost(10 * pageSize), 10 * pageSize);
	ASSERT_EQ(calculator->getWriteCost(10 * pageSize), fungibilityRatio * calculator->getReadCost(10 * pageSize));
	return Void();
}

TEST_CASE("/fdbclient/QuotaCostCalculator/Add") {
	auto const fungibilityRatio = 4.0;
	auto const pageSize = 1024;
	auto const calculator = IQuotaCostCalculator::create("add", fungibilityRatio, pageSize);
	ASSERT_EQ(calculator->getReadCost(0), pageSize);
	ASSERT_EQ(calculator->getWriteCost(0), fungibilityRatio * calculator->getReadCost(0));
	ASSERT_EQ(calculator->getReadCost(pageSize - 1), 2 * pageSize - 1);
	ASSERT_EQ(calculator->getWriteCost(pageSize - 1), fungibilityRatio * calculator->getReadCost(pageSize - 1));
	ASSERT_EQ(calculator->getReadCost(pageSize), 2 * pageSize);
	ASSERT_EQ(calculator->getWriteCost(pageSize), fungibilityRatio * calculator->getReadCost(pageSize));
	ASSERT_EQ(calculator->getReadCost(pageSize + 1), 2 * pageSize + 1);
	ASSERT_EQ(calculator->getWriteCost(pageSize + 1), fungibilityRatio * calculator->getReadCost(pageSize + 1));
	ASSERT_EQ(calculator->getReadCost(10 * pageSize), 11 * pageSize);
	ASSERT_EQ(calculator->getWriteCost(10 * pageSize), fungibilityRatio * calculator->getReadCost(10 * pageSize));
	return Void();
}

TEST_CASE("/fdbclient/QuotaCostCalculator/Minimum") {
	auto const fungibilityRatio = 4.0;
	auto const pageSize = 1024;
	auto const calculator = IQuotaCostCalculator::create("minimum", fungibilityRatio, pageSize);
	ASSERT_EQ(calculator->getReadCost(0), pageSize);
	ASSERT_EQ(calculator->getWriteCost(0), fungibilityRatio * calculator->getReadCost(0));
	ASSERT_EQ(calculator->getReadCost(pageSize - 1), pageSize);
	ASSERT_EQ(calculator->getWriteCost(pageSize - 1), fungibilityRatio * calculator->getReadCost(pageSize - 1));
	ASSERT_EQ(calculator->getReadCost(pageSize), pageSize);
	ASSERT_EQ(calculator->getWriteCost(pageSize), fungibilityRatio * calculator->getReadCost(pageSize));
	ASSERT_EQ(calculator->getReadCost(pageSize + 1), pageSize + 1);
	ASSERT_EQ(calculator->getWriteCost(pageSize + 1), fungibilityRatio * calculator->getReadCost(pageSize + 1));
	ASSERT_EQ(calculator->getReadCost(10 * pageSize), 10 * pageSize);
	ASSERT_EQ(calculator->getWriteCost(10 * pageSize), fungibilityRatio * calculator->getReadCost(10 * pageSize));
	return Void();
}
