/**
 * IQuotaCostCalculator.cpp
 */

#include "fdbclient/IQuotaCostCalculator.h"
#include "fdbclient/Knobs.h"
#include "flow/Trace.h"

#include <algorithm>
#include <string>

namespace {

class RoundQuotaCostCalculator : public IQuotaCostCalculator {
public:
	RoundQuotaCostCalculator(double fungibilityRatio, uint64_t pageSize)
	  : IQuotaCostCalculator(fungibilityRatio, pageSize) {}

	uint64_t getReadCost(uint64_t bytes) const override {
		if (bytes == 0) {
			return pageSize;
		} else {
			return ((bytes - 1) / pageSize + 1) * pageSize;
		}
	}

	uint64_t getWriteCost(uint64_t bytes) const override {
		if (bytes == 0) {
			return fungibilityRatio * pageSize;
		} else {
			return ((bytes - 1) / pageSize + 1) * fungibilityRatio * pageSize;
		}
	}
};

class AddQuotaCostCalculator : public IQuotaCostCalculator {
public:
	AddQuotaCostCalculator(double fungibilityRatio, uint64_t pageSize)
	  : IQuotaCostCalculator(fungibilityRatio, pageSize) {}

	uint64_t getReadCost(uint64_t bytes) const override { return pageSize + bytes; }

	uint64_t getWriteCost(uint64_t bytes) const override { return fungibilityRatio * (pageSize + bytes); }
};

class MinimumQuotaCostCalculator : public IQuotaCostCalculator {
public:
	MinimumQuotaCostCalculator(double fungibilityRatio, uint64_t pageSize)
	  : IQuotaCostCalculator(fungibilityRatio, pageSize) {}

	uint64_t getReadCost(uint64_t bytes) const override { return std::max<uint64_t>(pageSize, bytes); }

	uint64_t getWriteCost(uint64_t bytes) const override {
		return fungibilityRatio * std::max<uint64_t>(pageSize, bytes);
	}
};

} // namespace

IQuotaCostCalculator::IQuotaCostCalculator(double fungibilityRatio, uint64_t pageSize)
  : fungibilityRatio(fungibilityRatio),
    pageSize(pageSize) {}

uint64_t IQuotaCostCalculator::getClearCost() const {
	// NOTE: The throttling cost of each clear is assumed to be one page.
	// This makes compuation fast, but can be inaccurate and may
	// underestimate the cost of large clears.
	return fungibilityRatio * pageSize;
}

std::unique_ptr<IQuotaCostCalculator> IQuotaCostCalculator::create(std::string const& type,
                                                                   double fungibilityRatio,
                                                                   uint64_t pageSize) {
	if (type.compare("minimum") == 0) {
		return std::make_unique<MinimumQuotaCostCalculator>(fungibilityRatio, pageSize);
	} else if (type.compare("add") == 0) {
		return std::make_unique<AddQuotaCostCalculator>(fungibilityRatio, pageSize);
	} else if (type.compare("round") == 0) {
		return std::make_unique<RoundQuotaCostCalculator>(fungibilityRatio, pageSize);
	} else {
		TraceEvent(SevWarnAlways, "InvalidQuotaCostTypeSpecified").detail("SpecifiedType", type);
		return std::make_unique<MinimumQuotaCostCalculator>(fungibilityRatio, pageSize);
	}
}

std::unique_ptr<IQuotaCostCalculator> getQuotaCostCalculatorFromKnobs() {
	return IQuotaCostCalculator::create(CLIENT_KNOBS->QUOTA_THROTTLING_COST_FORMULA,
	                                    CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO,
	                                    CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
}
