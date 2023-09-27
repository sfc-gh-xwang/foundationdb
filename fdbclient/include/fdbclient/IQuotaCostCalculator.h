/**
 * IQuotaCostCalculator.h
 */

#pragma once

#include <memory>

class IQuotaCostCalculator {
protected:
	double fungibilityRatio;
	uint64_t pageSize;

	IQuotaCostCalculator(double fungibilityRatio, uint64_t pageSize);

public:
	virtual ~IQuotaCostCalculator() = default;
	virtual uint64_t getReadCost(uint64_t bytes) const = 0;
	virtual uint64_t getWriteCost(uint64_t bytes) const = 0;
	static std::unique_ptr<IQuotaCostCalculator> create(std::string const& type,
	                                                    double fungibilityRatio,
	                                                    uint64_t pageSize);
};
