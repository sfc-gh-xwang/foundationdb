/*
 * BGCorrectnessUtils.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */
#include "fdbserver/workloads/BGCorrectnessUtils.h"

Value genVal(uint32_t val, uint16_t valLen) {
	std::string v(valLen, 'x');
	auto valFormatted = format("%08x", val);
	ASSERT(valFormatted.size() <= v.size());

	for (int i = 0; i < valFormatted.size(); i++) {
		v[i] = valFormatted[i];
	}
	// copy into an arena
	// TODO do this in original arena? a bit more efficient that way
	Arena a;
	return Standalone<StringRef>(StringRef(a, v), a);
}

bool validateValue(const Value& v, uint32_t val, uint16_t valLen) {
	if (v.size() != valLen) {
		return false;
	}
	// check for correct value portion
	auto valFormatted = format("%08x", val);
	ASSERT(valFormatted.size() <= v.size());
	if (v.substr(0, valFormatted.size()) != valFormatted) {
		return false;
	}
	// check for corruption
	for (int i = valFormatted.size(); i < v.size(); i++) {
		if (v[i] != 'x') {
			return false;
		}
	}
	return true;
}