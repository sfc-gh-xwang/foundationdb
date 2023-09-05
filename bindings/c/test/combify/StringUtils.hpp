/*
 * StringUtils.hpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef COMBIFY_STRING_UTILS_HPP
#define COMBIFY_STRING_UTILS_HPP

#include <rapidjson/document.h>
#include <string>
#include <string_view>

namespace combify {

using std::optional;
using std::string;
using std::string_view;

inline string camelToSnakeLowerCase(string_view s) {
	string ret;
	ret.reserve(s.size() * 2);
	for (auto i = 0u; i < s.size(); i++) {
		if (std::isupper(s[i]) && i > 0) {
			ret.push_back('_');
		}
		ret.push_back(std::tolower(s[i]));
	}
	return ret;
}

template <class StringLike>
StringLike randomAlnumString(int lengthMin, int lengthMax) {
	static_assert(sizeof(typename StringLike::value_type) == 1);
	assert(lengthMin > 0 && lengthMin <= lengthMax);
	constexpr string_view dict = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	auto const ldiff = lengthMax - lengthMin;
	auto const len = lengthMin + (std::rand() % (ldiff + 1));
	StringLike ret;
	ret.reserve(len);
	for (auto i = 0; i < len; i++) {
		ret.push_back(static_cast<typename StringLike::value_type>(dict[std::rand() % static_cast<int>(dict.size())]));
	}
	return ret;
}

template <class StringLike>
StringLike randomHexString(int lengthMin, int lengthMax) {
	static_assert(sizeof(typename StringLike::value_type) == 1);
	assert(lengthMin > 0 && lengthMin <= lengthMax);
	constexpr string_view dict = "0123456789ABCDEFabcdef";
	auto const ldiff = lengthMax - lengthMin;
	auto const len = lengthMin + (std::rand() % (ldiff + 1));
	StringLike ret;
	ret.reserve(len);
	for (auto i = 0; i < len; i++) {
		ret.push_back(static_cast<typename StringLike::value_type>(dict[std::rand() % static_cast<int>(dict.size())]));
	}
	return ret;
}

} // namespace combify

#endif /*COMBIFY_STRING_UTILS_HPP*/
