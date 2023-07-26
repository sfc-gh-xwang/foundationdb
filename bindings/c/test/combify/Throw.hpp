/*
 * Throw.hpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef COMBIFY_THROW_HPP
#define COMBIFY_THROW_HPP

#include <string>
#include <string_view>
#include <stdexcept>

#include <fmt/format.h>
#include <rapidjson/document.h>

namespace combify {

using std::optional;
using std::string;
using std::string_view;

namespace detail {

constexpr inline string_view jsonTypeString(rapidjson::Type type) {
	constexpr const string_view typeStringMap[] = { "null", "false", "true", "object", "array", "string", "number" };
	return typeStringMap[static_cast<int>(type)];
}

[[noreturn]] inline void throwUnmatchedFieldType(string_view fieldName,
                                                 string_view typeExpected,
                                                 rapidjson::Type typeActual) {
	throw std::runtime_error(fmt::format("field type mismatch: name={} type_actual={} type_expected={}",
	                                     fieldName,
	                                     detail::jsonTypeString(typeActual),
	                                     typeExpected));
}

[[noreturn]] inline void throwNoSuchField(string_view fieldName) {
	throw std::runtime_error(fmt::format("no such field: {}", fieldName));
}

[[noreturn]] inline void throwNotAnObject(const rapidjson::Value& v, string_view fieldName) {
	throw std::runtime_error(fmt::format("JSON value from which to parse field '{}' is not an object; actual type={}",
	                                     fieldName,
	                                     detail::jsonTypeString(v.GetType())));
}

} // namespace detail

} // namespace combify

#endif /*COMBIFY_THROW_HPP*/
