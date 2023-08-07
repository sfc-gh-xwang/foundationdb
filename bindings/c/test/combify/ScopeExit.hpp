/*
 * ScopeExit.hpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef COMBIFY_SCOPE_EXIT_HPP
#define COMBIFY_SCOPE_EXIT_HPP

#include <utility>
#include <type_traits>

namespace combify {

// Execute lambda as this object goes out of scope.
template <typename Func>
class ScopeExit {
	std::decay_t<Func> fn;

public:
	ScopeExit(Func&& fn) : fn(std::forward<Func>(fn)) {}

	~ScopeExit() { fn(); }
};

} // namespace combify

#endif /*COMBIFY_SCOPE_EXIT_HPP*/
