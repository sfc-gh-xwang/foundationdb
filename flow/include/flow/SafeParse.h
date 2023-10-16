/**
 * SafeParse.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#pragma once

#include <string>

std::string toLower(std::string const&);

// Converts the given string into a double. If any errors are
// encountered, it throws an invalid_option_value exception.
double safe_stod(std::string const&);

// Converts the given (possibly hexadecimal) string into an
// integer. If any errors are encountered, it throws an
// invalid_option_value exception.
int safe_stoi(std::string const&);

// Converts the given (possibly hexadecimal) string into a 64-bit
// integer. If any errors are encountered, it throws an
// invalid_option_value exception.
int64_t safe_stoi64(std::string const&);

// Converts the given string into a bool. "true" and "false" are case
// insenstively interpreted as true and false. Otherwise, any non-zero
// integer is true. If any errors are encountered, it throws an
// invalid_option_value exception.
bool safe_stob(std::string const&);
