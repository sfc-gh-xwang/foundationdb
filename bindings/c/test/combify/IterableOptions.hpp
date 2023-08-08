/*
 * IterableOptions.hpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef COMBIFY_ITERABLE_OPTIONS_HPP
#define COMBIFY_ITERABLE_OPTIONS_HPP

#include <array>
#include <optional>
#include <string>
#include <string_view>

#include <fmt/format.h>

#include "JsonOption.hpp"
#include "NumericUtils.hpp"
#include "StaticOptions.hpp"
#include "StringUtils.hpp"

namespace combify {

using std::optional;
using std::string;
using std::string_view;

enum class IterableOptionTier : unsigned {
	Network,
	Database,
	Transaction,
};

template <StringLiteral NAME, IterableOptionTier TIER, std::integral T, T... CHOICES>
struct IntegralChoiceOption {
	static_assert(sizeof...(CHOICES) > 0);
	static constexpr const string_view name = NAME.value;
	static constexpr const IterableOptionTier tier = TIER;
	static constexpr const std::array<T, sizeof...(CHOICES)> choices = { CHOICES... };

	const T& value() const {
		assert(index < choices.size());
		return choices[index];
	}

	const T& operator*() const {
		assert(index < choices.size());
		return choices[index];
	}

	void reset() { index = 0; }

	operator bool() const noexcept { return index < choices.size(); }

	bool hasNext() const noexcept { return index + 1 < choices.size(); }

	void operator++() {
		if (index >= choices.size()) {
			throw std::out_of_range(fmt::format("attempted to iterate option {} out of legal range", name));
		}
		index++;
	}

	string shortSummary() const {
		static_assert(tier == IterableOptionTier::Network || tier == IterableOptionTier::Database ||
		              tier == IterableOptionTier::Transaction);
		// Prefix to indicate option tier: e.g. T:GRV_CACHE:1 means GRV_CACHE is a transaction level option and it is
		// set
		constexpr const char prefix[]{ "CDT" };
		fmt::memory_buffer buf;
		fmt::format_to(std::back_inserter(buf), "{}:{}:", prefix[enumToUnderlying(tier)], camelToSnakeLowerCase(name));
		if (index < choices.size()) {
			fmt::format_to(std::back_inserter(buf), "{:d}", choices[index]);
		} else {
			// end-of-range
			fmt::format_to(std::back_inserter(buf), "X");
		}
		return string(buf.data(), buf.size());
	}

	unsigned index = 0;
};

template <StringLiteral NAME, IterableOptionTier TIER>
using BooleanOption = IntegralChoiceOption<NAME, TIER, bool, false, true>;

struct ForceMultiVersionOption : BooleanOption<"forceMultiVersion", IterableOptionTier::Network> {
	bool validForStaticConfig(const StaticOptions&) const { return true; }

	void apply(const StaticOptions&) const { fdb::network::setOption(FDB_NET_OPTION_DISABLE_CLIENT_BYPASS); }
};

struct TrustedClientOption : BooleanOption<"trustedClient", IterableOptionTier::Network> {
	bool validForStaticConfig(const StaticOptions& o) const {
		if (!value()) {
			// Untrusted mode requires valid AuthZ config
			return o.tlsConfig && o.authzConfig;
		}
		// Trusted mode is applicable both with and without TLS
		return true;
	}

	void apply(const StaticOptions& o) const {
		if (o.tlsConfig) {
			auto const& tlsConfig = *o.tlsConfig;
			fdb::network::setOption(FDB_NET_OPTION_TLS_CA_PATH, *tlsConfig.caPath);
			if (value()) {
				// Only enable cert and key if in trusted mode (i.e. mTLS)
				fdb::network::setOption(FDB_NET_OPTION_TLS_CERT_PATH, *tlsConfig.certPath);

				fdb::network::setOption(FDB_NET_OPTION_TLS_KEY_PATH, *tlsConfig.keyPath);
			}
		}
	}

	void applyForAdmin(const StaticOptions& o) const {
		if (o.tlsConfig) {
			// Configure client TLS as trusted mode (i.e. mTLS)
			auto const& tlsConfig = *o.tlsConfig;
			fdb::network::setOption(FDB_NET_OPTION_TLS_CA_PATH, *tlsConfig.caPath);
			fdb::network::setOption(FDB_NET_OPTION_TLS_CERT_PATH, *tlsConfig.certPath);
			fdb::network::setOption(FDB_NET_OPTION_TLS_KEY_PATH, *tlsConfig.keyPath);
		}
	}
};

struct UseGrvCacheOption : BooleanOption<"grvCache", IterableOptionTier::Transaction> {
	bool validForStaticConfig(const StaticOptions&) const { return true; }

	void apply(fdb::Transaction tr) const { tr.setOption(FDB_TR_OPTION_USE_GRV_CACHE); }
};

} // namespace combify

#endif /*COMBIFY_ITERABLE_OPTIONS_HPP*/
