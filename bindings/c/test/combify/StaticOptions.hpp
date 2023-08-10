/*
 * StaticOptions.hpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef COMBIFY_STATIC_OPTIONS_HPP
#define COMBIFY_STATIC_OPTIONS_HPP

#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>

#include "JsonOption.hpp"

namespace combify {

using std::optional;
using std::string;
using std::string_view;

struct TLSConfig {
	JsonOption<string, "caPath", MustExist>
	    caPath; // if TLS is enabled in any form, client trust anchor is always needed
	JsonOption<string, "certPath", MustExist> certPath;
	JsonOption<string, "keyPath", MustExist> keyPath;
};

// specialization for parsing TLSConfig
template <StringLiteral NAME, class MUST_EXIST>
struct JsonOption<TLSConfig, NAME, MUST_EXIST> : JsonOptionCommonTraits<TLSConfig, NAME, MUST_EXIST> {
	using ParentType = JsonOptionCommonTraits<TLSConfig, NAME, MUST_EXIST>;
	using ParentType::name;
	using ParentType::value;

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
		auto& tlsConfigJson = memberItr->value;
		value.emplace(); // default construct TLSConfig
		auto& tlsConfig = *value;
		tlsConfig.caPath.parseFrom(tlsConfigJson);
		tlsConfig.certPath.parseFrom(tlsConfigJson);
		tlsConfig.keyPath.parseFrom(tlsConfigJson);
	}

	string shortSummary() const {
		return fmt::format("{}:{}", camelToSnakeLowerCase(name), [this]() -> string_view {
			if (!value) {
				return "OFF";
			} else if (value->certPath && value->keyPath) {
				return "MUTUAL";
			} else {
				return "UNTRUSTED";
			}
		}());
	}
};

struct AuthZConfig {
	JsonOption<string, "privateKeyPath", MustExist> privateKeyPath;
	JsonOption<string, "keyName", MustExist> keyName;
	// Loaded during parse
	optional<string> privateKeyPem;
	string signToken(const std::vector<int64_t> tenantIds) const;
};

// specialization for parsing AuthZConfig
template <StringLiteral NAME, class MUST_EXIST>
struct JsonOption<AuthZConfig, NAME, MUST_EXIST> : JsonOptionCommonTraits<AuthZConfig, NAME, MUST_EXIST> {
	using ParentType = JsonOptionCommonTraits<AuthZConfig, NAME, MUST_EXIST>;
	using ParentType::name;
	using ParentType::value;

	void parseFrom(const rapidjson::Value& v) {
		if (!v.IsObject()) {
			detail::throwNotAnObject(v, name);
		}
		auto memberItr = v.FindMember(NAME.value);
		if (memberItr == v.MemberEnd()) {
			detail::throwNoSuchField(name);
		}
		auto& authzConfigJson = memberItr->value;
		value.emplace(); // default construct AuthZConfig
		auto& config = *value;
		config.privateKeyPath.parseFrom(authzConfigJson);
		config.keyName.parseFrom(authzConfigJson);
		// This is the only case where any file read is needed. For now just read mid-parse.
		auto ifs = std::ifstream(*value->privateKeyPath);
		std::stringstream fileBuf;
		fileBuf << ifs.rdbuf();
		value->privateKeyPem = std::move(fileBuf).str();
	}

	string shortSummary() const {
		return camelToSnakeLowerCase(name) + ":" + [this]() {
			if (value->privateKeyPath && value->keyName) {
				return "ON";
			} else {
				return "OFF";
			}
		}();
	}
};

// fixed for the duration of this library: mostly cluster configurations
struct StaticOptions {
	JsonOption<string, "clusterFilePath", MustExist> clusterFilePath;
	JsonOption<TLSConfig, "tls", Optional> tlsConfig;
	JsonOption<AuthZConfig, "authz", Optional> authzConfig;
	JsonOption<StringChoice<"disabled", "required_experimental", "optional_experimental">, "tenantMode", MustExist>
	    tenantMode;
	JsonOption<unsigned, "tenantCount", Optional> tenantCount;
	JsonOption<unsigned, "randomSeed", Optional> randomSeed;

	void parseFrom(const rapidjson::Value& v) {
		clusterFilePath.parseFrom(v);
		tlsConfig.parseFrom(v);
		authzConfig.parseFrom(v);
		tenantMode.parseFrom(v);
		tenantCount.parseFrom(v);
		randomSeed.parseFrom(v);
	}

	string shortSummary() const {
		string ret;
		auto addDesc = [&ret](const auto& param) -> void {
			if (!ret.empty()) {
				ret.push_back(' ');
			}
			ret += param.shortSummary();
		};
		addDesc(clusterFilePath);
		addDesc(tlsConfig);
		addDesc(authzConfig);
		addDesc(tenantMode);
		addDesc(tenantCount);
		// Keep randomSeed out for now. Printing at beginning or end of run instead would be more helpful.
		return ret;
	}
};

} // namespace combify

#endif /*COMBIFY_STATIC_OPTIONS_HPP*/
