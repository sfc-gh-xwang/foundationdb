/**
 * SafeParse.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "flow/Error.h"
#include "flow/SafeParse.h"

std::string toLower(std::string const& name) {
	std::string lower_name;
	for (auto c = name.begin(); c != name.end(); ++c)
		if (*c >= 'A' && *c <= 'Z')
			lower_name += *c - 'A' + 'a';
		else
			lower_name += *c;
	return lower_name;
}

double safe_stod(std::string const& str) {
	size_t n;
	double value = std::stod(str, &n);
	if (n < str.size()) {
		throw invalid_option_value();
	}
	return value;
}

int safe_stoi(std::string const& str) {
	size_t n;
	int value = std::stoi(str, &n, 0);
	if (n < str.size()) {
		throw invalid_option_value();
	}
	return value;
}

int64_t safe_stoi64(std::string const& str) {
	size_t n;
	int64_t value = static_cast<int64_t>(std::stoll(str, &n, 0));
	if (n < str.size()) {
		throw invalid_option_value();
	}
	return value;
}

bool safe_stob(std::string const& str) {
	if (toLower(str) == "true") {
		return true;
	} else if (toLower(str) == "false") {
		return false;
	} else {
		return safe_stoi(str) != 0;
	}
}
