/*
 * NumericUtils.hpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef COMBIFY_NUMERIC_UTILS_HPP
#define COMBIFY_NUMERIC_UTILS_HPP

#include <concepts>
#include <type_traits>

namespace combify {

template <class E>
concept Enum = std::is_enum_v<E>;

template <Enum E>
std::underlying_type_t<E> enumToUnderlying(E e) {
	return static_cast<std::underlying_type_t<E>>(e);
}

} // namespace combify

#endif /*COMBIFY_NUMERIC_UTILS_HPP*/
