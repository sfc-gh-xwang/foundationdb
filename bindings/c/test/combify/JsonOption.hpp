/*
 * JsonOption.hpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef COMBIFY_JSON_OPTION_HPP
#define COMBIFY_JSON_OPTION_HPP

#include <string>
#include <string_view>
#include <optional>

#include <fmt/format.h>
#include <rapidjson/document.h>

#include "StringUtils.hpp"
#include "Throw.hpp"

namespace combify {

using std::optional;
using std::string;
using std::string_view;

// This class template allows you to pass double-quoted string literals as template parameter
template <size_t N>
struct StringLiteral {
	constexpr StringLiteral(const char (&str)[N]) { std::copy_n(str, N, value); }
	char value[N];

	constexpr string_view toView() const noexcept { return string_view(value, N - 1); }

	constexpr optional<string_view> toOptional() const noexcept {
		if constexpr (N <= 1)
			return {};
		return string_view(value, N - 1);
	}
};

struct MustExist {
	static constexpr bool value = true;
};

struct Optional {
	static constexpr bool value = false;
};

template <class T, StringLiteral NAME, class MUST_EXIST>
struct JsonOptionCommonTraits {
	static constexpr const string_view name = NAME.value;
	static constexpr const bool fieldMustExist = MUST_EXIST::value;

	optional<T> value;

	const T& operator*() const { return *value; }

	const T* operator->() const { return &(*value); }

	operator bool() const noexcept { return value.has_value(); }
};

template <class T, StringLiteral NAME, class MUST_EXIST>
struct JsonOption : JsonOptionCommonTraits<T, NAME, MUST_EXIST> {
	using ParentType = JsonOptionCommonTraits<T, NAME, MUST_EXIST>;
	// Base members are not visible to derived class by default if base is from a class template.
	using ParentType::name;
	using ParentType::value;

	// Try to read an attribute [name] from the object [v] and use its content to build T
	void parseFrom(const rapidjson::Value& v) {
		if (!v.IsObject()) {
			detail::throwNotAnObject(v, name);
		}
		auto memberItr = v.FindMember(NAME.value);
		if (memberItr == v.MemberEnd()) {
			if constexpr (MUST_EXIST::value) {
				detail::throwNoSuchField(name);
			} else {
				value.reset();
				return;
			}
		}
		const auto& f = memberItr->value;
		if constexpr (std::is_same_v<T, bool>) {
			if (!f.isBool()) {
				detail::throwUnmatchedFieldType(name, "bool", f.GetType());
			}
			value = f.GetBool();
		} else if constexpr (std::is_arithmetic_v<T>) {
			if (!f.IsNumber()) {
				detail::throwUnmatchedFieldType(name, "number", f.GetType());
			}
			double const number = f.GetDouble();
			if (std::is_unsigned_v<T> && number < 0) {
				throw std::runtime_error(
				    fmt::format("ATTEMPTED TO ASSIGN NEGATIVE NUMBER TO UNSIGNED OPTION {}. VALUE:{}", name, number));
			}
			// no precision loss for integers within [-2^53, 2^53]
			value = static_cast<T>(number);
		} else if constexpr (std::is_same_v<T, string>) {
			if (!f.IsString()) {
				detail::throwUnmatchedFieldType(name, "string", f.GetType());
			}
			value = string(f.GetString(), f.GetStringLength());
		} else {
			throw std::runtime_error(fmt::format(
			    "DEFAULT PARSE LOGIC FOR TYPE {} IS NOT SUPPORTED; IMPLEMENT A SPECIALIZATION", typeid(T).name()));
		}
	}

	// short summary to be prefixed at the beginning of every test
	string shortSummary() const {
		return camelToSnakeLowerCase(name) + ":" + [this]() -> string {
			if (!value) {
				return "X"; // unset
			}
			if constexpr (std::is_same_v<T, bool>) {
				if (*value) {
					return "1";
				} else {
					return "0";
				}
			} else if constexpr (std::is_arithmetic_v<T>) {
				auto s = std::to_string(*value);
				if (s.size() > 3) {
					return "SET";
				} else {
					return s;
				}
			} else if constexpr (std::is_same_v<T, string>) {
				if (value->size() > 3) {
					return "SET";
				} else {
					return *value;
				}
			} else {
				throw std::runtime_error(fmt::format(
				    "default short description logic for type {} is not supported; implement a specialization",
				    typeid(T).name()));
			}
		}();
	}

	string longSummary() const { return fmt::format("{}:{}", camelToSnakeLowerCase(name), *value); }
};

template <StringLiteral... CHOICES>
struct StringChoice : string {
	static constexpr std::array<string_view, sizeof...(CHOICES)> choices{ CHOICES.toView()... };

	// -1 if not found
	static int indexOf(string param) {
		auto itr = std::find(choices.begin(), choices.end(), param);
		if (itr == choices.end()) {
			return -1;
		}
		return itr - choices.begin();
	}
};

// specialization for parsing StringChoice
template <StringLiteral NAME, class MUST_EXIST, StringLiteral... CHOICES>
struct JsonOption<StringChoice<CHOICES...>, NAME, MUST_EXIST> : JsonOptionCommonTraits<string, NAME, MUST_EXIST> {
	using ParentType = JsonOptionCommonTraits<string, NAME, MUST_EXIST>;
	using ParentType::name;
	using ParentType::value;

	int index = -1;

	void parseFrom(const rapidjson::Value& v) {
		if (!v.IsObject()) {
			detail::throwNotAnObject(v, name);
		}
		auto memberItr = v.FindMember(NAME.value);
		if (memberItr == v.MemberEnd()) {
			if constexpr (MUST_EXIST::value) {
				detail::throwNoSuchField(name);
			} else {
				return;
			}
		}
		const auto& f = memberItr->value;
		if (!f.IsString()) {
			detail::throwUnmatchedFieldType(name, "string", f.GetType());
		}
		string maybeValue(f.GetString(), f.GetStringLength());

		using ChoiceType = StringChoice<CHOICES...>;
		index = ChoiceType::indexOf(maybeValue);
		if (index == -1) {
			throw std::runtime_error(
			    fmt::format("INVALID VALUE '{}' FOR STRING CHOICE OPTION {}. VALID VALUES ARE: [{}]",
			                maybeValue,
			                name,
			                fmt::join(ChoiceType::choices, ", ")));
		} else {
			value = maybeValue;
		}
	}

	string shortSummary() const {
		assert(value);
		return camelToSnakeLowerCase(name) + ":" + std::to_string(index);
	}
};

} // namespace combify

#endif /*COMBIFY_JSON_OPTION_HPP*/
