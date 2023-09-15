#include "fdbrpc/Base64Encode.h"
#include "fdbrpc/Base64Decode.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbrpc/TokenCache.h"
#include "fdbrpc/TokenSign.h"
#include "fdbrpc/TenantInfo.h"
#include "flow/MkCert.h"
#include "flow/ScopeExit.h"
#include "flow/UnitTest.h"
#include "flow/network.h"

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include <fmt/format.h>
#include <list>
#include <deque>
#include <sstream>

#include "flow/actorcompiler.h" // has to be last include

using authz::TenantId;

template <class Key, class Value>
class LRUCache {
public:
	using key_type = Key;
	using list_type = std::list<key_type>;
	using mapped_type = Value;
	using map_type = boost::unordered_map<key_type, std::pair<mapped_type, typename list_type::iterator>>;
	using size_type = unsigned;

	explicit LRUCache(size_type capacity) : _capacity(capacity) { _map.reserve(capacity); }

	size_type size() const { return _map.size(); }
	size_type capacity() const { return _capacity; }
	bool empty() const { return _map.empty(); }

	Optional<mapped_type*> get(key_type const& key) {
		auto i = _map.find(key);
		if (i == _map.end()) {
			return Optional<mapped_type*>();
		}
		auto j = i->second.second;
		if (j != _list.begin()) {
			_list.erase(j);
			_list.push_front(i->first);
			i->second.second = _list.begin();
		}
		return &i->second.first;
	}

	template <class K, class V>
	mapped_type* insert(K&& key, V&& value) {
		auto iter = _map.find(key);
		if (iter != _map.end()) {
			return &iter->second.first;
		}
		if (size() == capacity()) {
			auto i = --_list.end();
			_map.erase(*i);
			_list.erase(i);
		}
		_list.push_front(std::forward<K>(key));
		std::tie(iter, std::ignore) =
		    _map.insert(std::make_pair(*_list.begin(), std::make_pair(std::forward<V>(value), _list.begin())));
		return &iter->second.first;
	}

private:
	const size_type _capacity;
	map_type _map;
	list_type _list;
};

TEST_CASE("/fdbrpc/authz/LRUCache") {
	auto& rng = *deterministicRandom();
	{
		// test very small LRU cache
		LRUCache<int, StringRef> cache(rng.randomInt(2, 10));
		for (int i = 0; i < 200; ++i) {
			cache.insert(i, "val"_sr);
			if (i >= cache.capacity()) {
				for (auto j = 0; j <= i - cache.capacity(); j++)
					ASSERT(!cache.get(j).present());
				// ordering is important so as not to disrupt the LRU order
				for (auto j = i - cache.capacity() + 1; j <= i; j++)
					ASSERT(cache.get(j).present());
			}
		}
	}
	{
		// Test larger cache
		LRUCache<int, StringRef> cache(1000);
		for (auto i = 0; i < 1000; ++i) {
			cache.insert(i, "value"_sr);
		}
		cache.insert(1000, "value"_sr); // should evict 0
		ASSERT(!cache.get(0).present());
	}
	{
		// memory test -- this is what the boost implementation didn't do correctly
		LRUCache<StringRef, Standalone<StringRef>> cache(10);
		std::deque<std::string> cachedStrings;
		std::deque<std::string> evictedStrings;
		for (int i = 0; i < 10; ++i) {
			auto str = rng.randomAlphaNumeric(rng.randomInt(100, 1024));
			Standalone<StringRef> sref(str);
			cache.insert(sref, sref);
			cachedStrings.push_back(str);
		}
		for (int i = 0; i < 10; ++i) {
			Standalone<StringRef> existingStr(cachedStrings.back());
			auto cachedStr = cache.get(existingStr);
			ASSERT(cachedStr.present());
			ASSERT(*cachedStr.get() == existingStr);
			if (!evictedStrings.empty()) {
				Standalone<StringRef> nonexisting(evictedStrings.at(rng.randomInt(0, evictedStrings.size())));
				ASSERT(!cache.get(nonexisting).present());
			}
			auto str = rng.randomAlphaNumeric(rng.randomInt(100, 1024));
			Standalone<StringRef> sref(str);
			evictedStrings.push_back(cachedStrings.front());
			cachedStrings.pop_front();
			cachedStrings.push_back(str);
			cache.insert(sref, sref);
		}
	}
	return Void();
}

struct CacheEntry {
	Arena arena;
	VectorRef<TenantId> tenants;
	Optional<StringRef> tokenId;
	double expirationTime = 0.0;
};

struct AuditEntry {
	IPAddress address;
	TenantId tenantId;
	Optional<Standalone<StringRef>> tokenId;
	bool operator==(const AuditEntry& other) const noexcept = default;
	explicit AuditEntry(IPAddress const& address, TenantId tenantId, CacheEntry const& cacheEntry)
	  : address(address),
	    tenantId(tenantId),
	    tokenId(cacheEntry.tokenId.present() ? Standalone<StringRef>(cacheEntry.tokenId.get(), cacheEntry.arena)
	                                         : Optional<Standalone<StringRef>>()) {}
};

std::size_t hash_value(AuditEntry const& value) {
	std::size_t seed = 0;
	boost::hash_combine(seed, value.address);
	boost::hash_combine(seed, value.tenantId);
	if (value.tokenId.present()) {
		boost::hash_combine(seed, value.tokenId.get());
	}
	return seed;
}

struct TokenCacheImpl {
	TokenCacheImpl();
	LRUCache<StringRef, CacheEntry> cache;
	LRUCache<AuditEntry, double> usedTokens;

	bool validate(TenantId tenantId, StringRef token);
	bool validateAndAdd(double currentTime, StringRef token, NetworkAddress const& peer, UID const& connDebugId);
	void logTokenUsage(double currentTime, UID const& connDebugId, AuditEntry entry);
};

TokenCacheImpl::TokenCacheImpl()
  : cache(FLOW_KNOBS->TOKEN_CACHE_SIZE),
    usedTokens(FLOW_KNOBS->TOKEN_USAGE_DEDUP_CACHE_SIZE) {}

TokenCache::TokenCache() : impl(new TokenCacheImpl()) {}
TokenCache::~TokenCache() {
	delete impl;
}

void TokenCache::createInstance() {
	g_network->setGlobal(INetwork::enTokenCache, new TokenCache());
}

TokenCache& TokenCache::instance() {
	return *reinterpret_cast<TokenCache*>(g_network->global(INetwork::enTokenCache));
}

bool TokenCache::validate(TenantId tenantId, StringRef token) {
	return impl->validate(tenantId, token);
}

#define TRACE_INVALID_PARSED_TOKEN(reason, token)                                                                      \
	TraceEvent(SevWarn, "InvalidToken"_audit, connDebugId)                                                             \
	    .detail("From", peer)                                                                                          \
	    .detail("Reason", reason)                                                                                      \
	    .detail("CurrentTime", fmt::format("{:.6f}", currentTime))                                                     \
	    .detail("Token", toStringRef(arena, token).toStringView())

#define TRACE_INVALID_CACHED_TOKEN(reason, entry)                                                                      \
	TraceEvent(SevWarn, "InvalidToken"_audit, connDebugId)                                                             \
	    .detail("From", peer)                                                                                          \
	    .detail("Reason", reason)                                                                                      \
	    .detail("CurrentTime", fmt::format("{:.6f}", currentTime))                                                     \
	    .detail("TokenId", entry->tokenId)                                                                             \
	    .detail("ExpirationTime", fmt::format("{:.6f}", entry->expirationTime))                                        \
	    .detail("AuthorizedTenants", fmt::format("{:#x}", fmt::join(entry->tenants, " ")))

bool TokenCacheImpl::validateAndAdd(double currentTime,
                                    StringRef token,
                                    NetworkAddress const& peer,
                                    UID const& connDebugId) {
	Arena arena;
	authz::jwt::TokenRef t;
	StringRef signInput;
	Optional<StringRef> err;
	bool verifyOutcome;
	if ((err = authz::jwt::parseToken(arena, token, t, signInput)).present()) {
		CODE_PROBE(true, "Token can't be parsed");
		TraceEvent te(SevWarn, "InvalidToken"_audit, connDebugId);
		te.detail("From", peer);
		te.detail("Reason", "ParseError");
		te.detail("ErrorDetail", err.get());
		if (signInput.empty()) { // unrecognizable token structure
			if (FLOW_KNOBS->LOG_MALFORMED_TOKEN_STRINGS) {
				te.detail("Token", token.toString());
			} else {
				te.detail("Token", "[not_a_valid_jwt_structure]");
			}
		} else { // trace with signature part taken out
			te.detail("SignInput", signInput.toString());
		}
		return false;
	}
	auto key = FlowTransport::transport().getPublicKeyByName(t.keyId);
	if (!key.present()) {
		CODE_PROBE(true, "Token referencing non-existing key");
		TRACE_INVALID_PARSED_TOKEN("UnknownKey", t);
		return false;
	} else if (!t.issuedAtUnixTime.present()) {
		CODE_PROBE(true, "Token has no issued-at field");
		TRACE_INVALID_PARSED_TOKEN("NoIssuedAt", t);
		return false;
	} else if (!t.expiresAtUnixTime.present()) {
		CODE_PROBE(true, "Token has no expiration time");
		TRACE_INVALID_PARSED_TOKEN("NoExpirationTime", t);
		return false;
	} else if (double(t.expiresAtUnixTime.get()) <= currentTime) {
		CODE_PROBE(true, "Expired token");
		TRACE_INVALID_PARSED_TOKEN("Expired", t);
		return false;
	} else if (!t.notBeforeUnixTime.present()) {
		CODE_PROBE(true, "Token has no not-before field");
		TRACE_INVALID_PARSED_TOKEN("NoNotBefore", t);
		return false;
	} else if (double(t.notBeforeUnixTime.get()) > currentTime) {
		CODE_PROBE(true, "Token's not-before is in the future");
		TRACE_INVALID_PARSED_TOKEN("TokenNotYetValid", t);
		return false;
	} else if (!t.tenants.present()) {
		CODE_PROBE(true, "Token with no tenants");
		TRACE_INVALID_PARSED_TOKEN("NoTenants", t);
		return false;
	}
	std::tie(verifyOutcome, err) = authz::jwt::verifyToken(signInput, t, key.get());
	if (err.present()) {
		CODE_PROBE(true, "Error while verifying token");
		TRACE_INVALID_PARSED_TOKEN("ErrorWhileVerifyingToken", t).detail("ErrorDetail", err.get());
		return false;
	} else if (!verifyOutcome) {
		CODE_PROBE(true, "Token with invalid signature", probe::decoration::rare);
		TRACE_INVALID_PARSED_TOKEN("InvalidSignature", t);
		return false;
	} else {
		CacheEntry c;
		c.expirationTime = t.expiresAtUnixTime.get();
		c.tenants.reserve(c.arena, t.tenants.get().size());
		for (auto tenantId : t.tenants.get()) {
			c.tenants.push_back(c.arena, tenantId);
		}
		if (t.tokenId.present()) {
			c.tokenId = StringRef(c.arena, t.tokenId.get());
		}
		cache.insert(StringRef(c.arena, token), c);
		return true;
	}
}

bool TokenCacheImpl::validate(TenantId tenantId, StringRef token) {
	NetworkAddress peer = FlowTransport::transport().currentDeliveryPeerAddress();
	UID connDebugId = FlowTransport::transport().currentDeliveryPeerDebugID();
	auto cachedEntry = cache.get(token);
	double currentTime = g_network->timer();

	if (!cachedEntry.present()) {
		if (validateAndAdd(currentTime, token, peer, connDebugId)) {
			cachedEntry = cache.get(token);
		} else {
			return false;
		}
	}

	ASSERT(cachedEntry.present());

	auto& entry = cachedEntry.get();
	if (entry->expirationTime < currentTime) {
		CODE_PROBE(true, "Found expired token in cache");
		TRACE_INVALID_CACHED_TOKEN("ExpiredInCache", entry);
		return false;
	}
	bool tenantFound = false;
	for (auto const& t : entry->tenants) {
		if (t == tenantId) {
			tenantFound = true;
			break;
		}
	}
	if (!tenantFound) {
		CODE_PROBE(true, "Valid token doesn't reference tenant");
		TRACE_INVALID_CACHED_TOKEN("TenantTokenMismatch", entry)
		    .detail("RequestedTenant", fmt::format("{:#x}", tenantId));
		return false;
	}
	// audit logging
	if (FLOW_KNOBS->AUDIT_LOGGING_ENABLED)
		logTokenUsage(currentTime, connDebugId, AuditEntry(peer.ip, tenantId, *cachedEntry.get()));
	return true;
}

void TokenCacheImpl::logTokenUsage(double currentTime, UID const& connDebugId, AuditEntry entry) {
	Optional<double*> optionalFirstLoggedTime = usedTokens.get(entry);
	if (optionalFirstLoggedTime.present()) {
		double const firstLoggedTime = *optionalFirstLoggedTime.get();
		if (firstLoggedTime + FLOW_KNOBS->AUDIT_TIME_WINDOW < currentTime) {
			// reset timer for this entry and log again
			*optionalFirstLoggedTime.get() = currentTime;
		} else {
			// within the dedup window; nothing to do.
			return;
		}
	} else {
		// found this for the first time. insert to cache and log.
		usedTokens.insert(entry, currentTime);
	}
	CODE_PROBE(true, "Audit Logging Running");
	TraceEvent("AuditTokenUsed"_audit, connDebugId)
	    .detail("Client", entry.address)
	    .detail("TenantId", fmt::format("{:#x}", entry.tenantId))
	    .detail("TokenId", entry.tokenId)
	    .log();
}

namespace authz::jwt {
extern TokenRef makeRandomTokenSpec(Arena&, IRandom&, authz::Algorithm);
}

TEST_CASE("/fdbrpc/authz/TokenCache/BadTokens") {
	auto const pubKeyName = "someEcPublicKey"_sr;
	auto const rsaPubKeyName = "someRsaPublicKey"_sr;
	auto privateKey = mkcert::makeEcP256();
	auto publicKey = privateKey.toPublic();
	auto rsaPrivateKey = mkcert::makeRsa4096Bit(); // to trigger unmatched sign algorithm
	auto rsaPublicKey = rsaPrivateKey.toPublic();
	std::pair<std::function<void(Arena&, IRandom&, authz::jwt::TokenRef&)>, char const*> badMutations[]{
		{
		    [](Arena&, IRandom&, authz::jwt::TokenRef&) { FlowTransport::transport().removeAllPublicKeys(); },
		    "NoKeyWithSuchName",
		},
		{
		    [](Arena&, IRandom&, authz::jwt::TokenRef& token) { token.expiresAtUnixTime.reset(); },
		    "NoExpirationTime",
		},
		{
		    [](Arena&, IRandom& rng, authz::jwt::TokenRef& token) {
		        token.expiresAtUnixTime = std::max<double>(g_network->timer() - 10 - rng.random01() * 50, 0);
		    },
		    "ExpiredToken",
		},
		{
		    [](Arena&, IRandom&, authz::jwt::TokenRef& token) { token.notBeforeUnixTime.reset(); },
		    "NoNotBefore",
		},
		{
		    [](Arena&, IRandom& rng, authz::jwt::TokenRef& token) {
		        token.notBeforeUnixTime = g_network->timer() + 10 + rng.random01() * 50;
		    },
		    "TokenNotYetValid",
		},
		{
		    [](Arena&, IRandom&, authz::jwt::TokenRef& token) { token.issuedAtUnixTime.reset(); },
		    "NoIssuedAt",
		},
		{
		    [](Arena& arena, IRandom&, authz::jwt::TokenRef& token) { token.tenants.reset(); },
		    "NoTenants",
		},
		{
		    [](Arena& arena, IRandom&, authz::jwt::TokenRef& token) {
		        TenantId* newTenants = new (arena) TenantId[1];
		        *newTenants = token.tenants.get()[0] + 1;
		        token.tenants = VectorRef<TenantId>(newTenants, 1);
		    },
		    "UnmatchedTenant",
		},
		{
		    [rsaPubKeyName](Arena& arena, IRandom&, authz::jwt::TokenRef& token) { token.keyId = rsaPubKeyName; },
		    "UnmatchedSignAlgorithm",
		},
	};
	auto const numBadMutations = sizeof(badMutations) / sizeof(badMutations[0]);
	for (auto repeat = 0; repeat < 50; repeat++) {
		auto arena = Arena();
		auto& rng = *deterministicRandom();
		auto validTokenSpec = authz::jwt::makeRandomTokenSpec(arena, rng, authz::Algorithm::ES256);
		validTokenSpec.keyId = pubKeyName;
		for (auto i = 0; i <= numBadMutations + 1; i++) {
			FlowTransport::transport().addPublicKey(pubKeyName, publicKey);
			FlowTransport::transport().addPublicKey(rsaPubKeyName, rsaPublicKey);
			auto publicKeyClearGuard = ScopeExit([]() { FlowTransport::transport().removeAllPublicKeys(); });
			auto signedToken = StringRef();
			auto tmpArena = Arena();
			if (i < numBadMutations) {
				auto [mutationFn, mutationDesc] = badMutations[i];
				auto mutatedTokenSpec = validTokenSpec;
				mutationFn(tmpArena, rng, mutatedTokenSpec);
				signedToken = authz::jwt::signToken(tmpArena, mutatedTokenSpec, privateKey);
				if (TokenCache::instance().validate(validTokenSpec.tenants.get()[0], signedToken)) {
					fmt::print("Unexpected successful validation at mutation {}, token spec: {}\n",
					           mutationDesc,
					           toStringRef(tmpArena, mutatedTokenSpec).toStringView());
					ASSERT(false);
				}
			} else if (i == numBadMutations) {
				// squeeze in a bad signature case that does not fit into mutation interface
				signedToken = authz::jwt::signToken(tmpArena, validTokenSpec, privateKey);
				signedToken.popBack();
				if (TokenCache::instance().validate(validTokenSpec.tenants.get()[0], signedToken)) {
					fmt::print("Unexpected successful validation with a token with truncated signature part\n");
					ASSERT(false);
				}
			} else {
				// test if badly base64-encoded tenant name causes validation to fail as expected
				auto signInput = authz::jwt::makeSignInput(tmpArena, validTokenSpec);
				auto b64Header = signInput.eat("."_sr);
				auto payload = base64::url::decode(tmpArena, signInput).get();
				rapidjson::Document d;
				d.Parse(reinterpret_cast<const char*>(payload.begin()), payload.size());
				ASSERT(!d.HasParseError());
				rapidjson::StringBuffer wrBuf;
				rapidjson::Writer<rapidjson::StringBuffer> wr(wrBuf);
				auto tenantsField = d.FindMember("tenants");
				ASSERT(tenantsField != d.MemberEnd());
				tenantsField->value.PushBack("ABC#", d.GetAllocator()); // inject base64-illegal character
				d.Accept(wr);
				auto b64ModifiedPayload = base64::url::encode(
				    tmpArena, StringRef(reinterpret_cast<const uint8_t*>(wrBuf.GetString()), wrBuf.GetSize()));
				signInput = b64Header.withSuffix("."_sr, tmpArena).withSuffix(b64ModifiedPayload, tmpArena);
				signedToken = authz::jwt::signToken(tmpArena, signInput, validTokenSpec.algorithm, privateKey);
				if (TokenCache::instance().validate(validTokenSpec.tenants.get()[0], signedToken)) {
					fmt::print(
					    "Unexpected successful validation of a token with tenant name containing non-base64 chars)\n");
					ASSERT(false);
				}
			}
		}
	}
	if (TokenCache::instance().validate(TenantInfo::INVALID_TENANT, StringRef())) {
		fmt::print("Unexpected successful validation of ill-formed token (no signature part)\n");
		ASSERT(false);
	}
	if (TokenCache::instance().validate(TenantInfo::INVALID_TENANT, "1111.22"_sr)) {
		fmt::print("Unexpected successful validation of ill-formed token (no signature part)\n");
		ASSERT(false);
	}
	if (TokenCache::instance().validate(TenantInfo::INVALID_TENANT, "////.////.////"_sr)) {
		fmt::print("Unexpected successful validation of unparseable token\n");
		ASSERT(false);
	}
	fmt::print("TEST OK\n");
	return Void();
}

TEST_CASE("/fdbrpc/authz/TokenCache/GoodTokens") {
	// Don't repeat because token expiry is at seconds granularity and sleeps are costly in unit tests
	state Arena arena;
	state PrivateKey privateKey = mkcert::makeEcP256();
	state StringRef pubKeyName = "somePublicKey"_sr;
	state ScopeExit<std::function<void()>> publicKeyClearGuard(
	    [pubKeyName = pubKeyName]() { FlowTransport::transport().removePublicKey(pubKeyName); });
	state authz::jwt::TokenRef tokenSpec =
	    authz::jwt::makeRandomTokenSpec(arena, *deterministicRandom(), authz::Algorithm::ES256);
	state StringRef signedToken;
	FlowTransport::transport().addPublicKey(pubKeyName, privateKey.toPublic());
	tokenSpec.expiresAtUnixTime = g_network->timer() + 2.0;
	tokenSpec.keyId = pubKeyName;
	signedToken = authz::jwt::signToken(arena, tokenSpec, privateKey);
	if (!TokenCache::instance().validate(tokenSpec.tenants.get()[0], signedToken)) {
		fmt::print("Unexpected failed token validation, token spec: {}, now: {}\n",
		           toStringRef(arena, tokenSpec).toStringView(),
		           g_network->timer());
		ASSERT(false);
	}
	wait(delay(3.5));
	if (TokenCache::instance().validate(tokenSpec.tenants.get()[0], signedToken)) {
		fmt::print(
		    "Unexpected successful token validation after supposedly expiring in cache, token spec: {}, now: {}\n",
		    toStringRef(arena, tokenSpec).toStringView(),
		    g_network->timer());
		ASSERT(false);
	}
	fmt::print("TEST OK\n");
	return Void();
}

namespace {

template <class Modifier>
concept DocumentModifier = requires(Modifier&& modifier) {
	                           { modifier(std::declval<rapidjson::Document&>()) } -> std::same_as<void>;
                           };

// Utility functions for modifying sign input
template <DocumentModifier Modifier>
StringRef modifyHeader(Arena& arena, StringRef signInput, Modifier&& modifier) {
	auto headerPart = signInput.eat("."_sr);
	auto headerStr = base64::url::decode(arena, headerPart).get();
	auto payloadPart = signInput;
	rapidjson::Document d;
	d.Parse(reinterpret_cast<const char*>(headerStr.begin()), headerStr.size());
	ASSERT(!d.HasParseError());
	rapidjson::StringBuffer wrBuf;
	rapidjson::Writer<rapidjson::StringBuffer> wr(wrBuf);
	modifier(d);
	d.Accept(wr);
	auto newHeaderStr = StringRef(reinterpret_cast<const uint8_t*>(wrBuf.GetString()), wrBuf.GetSize());
	fmt::print("New header after modifier: {}\n", newHeaderStr.toStringView());
	auto newHeaderPart = base64::url::encode(arena, newHeaderStr);
	return concatenateStrings(arena, newHeaderPart, "."_sr, payloadPart);
}

template <DocumentModifier Modifier>
StringRef modifyPayload(Arena& arena, StringRef signInput, Modifier&& modifier) {
	auto headerPart = signInput.eat("."_sr);
	auto payloadPart = signInput;
	auto payloadStr = base64::url::decode(arena, payloadPart).get();
	rapidjson::Document d;
	d.Parse(reinterpret_cast<const char*>(payloadStr.begin()), payloadStr.size());
	ASSERT(!d.HasParseError());
	rapidjson::StringBuffer wrBuf;
	rapidjson::Writer<rapidjson::StringBuffer> wr(wrBuf);
	modifier(d);
	d.Accept(wr);
	auto newPayloadStr = StringRef(reinterpret_cast<const uint8_t*>(wrBuf.GetString()), wrBuf.GetSize());
	fmt::print("New payload after modifier: {}\n", newPayloadStr.toStringView());
	auto newPayloadPart = base64::url::encode(arena, newPayloadStr);
	return concatenateStrings(arena, headerPart, "."_sr, newPayloadPart);
}

template <class Inspector>
concept SignInputInspector =
    requires(Inspector&& inspector) {
	    {
		    inspector(std::declval<rapidjson::Document const&>(), std::declval<rapidjson::Document const&>())
		    } -> std::same_as<void>;
    };

template <SignInputInspector Inspector>
void inspectSignInput(StringRef signInput, Inspector&& inspector) {
	auto arena = Arena();
	auto headerPart = signInput.eat("."_sr);
	auto payloadPart = signInput;
	auto headerDoc = rapidjson::Document();
	{
		auto headerStr = base64::url::decode(arena, headerPart).get();
		headerDoc.Parse(reinterpret_cast<const char*>(headerStr.begin()), headerStr.size());
		ASSERT(!headerDoc.HasParseError());
	}
	auto payloadDoc = rapidjson::Document();
	{
		auto payloadStr = base64::url::decode(arena, payloadPart).get();
		payloadDoc.Parse(reinterpret_cast<const char*>(payloadStr.begin()), payloadStr.size());
		ASSERT(!payloadDoc.HasParseError());
	}
	inspector(headerDoc, payloadDoc);
}

} // anonymous namespace

TEST_CASE("/fdbrpc/authz/TokenCache/CommonAttacks") {
	auto privateKey = mkcert::makeEcP256();
	auto publicKey = privateKey.toPublic();
	auto pubKeyName = "somePublicKey"_sr;
	auto publicKeyClearGuard = ScopeExit([pubKeyName]() { FlowTransport::transport().removePublicKey(pubKeyName); });
	FlowTransport::transport().addPublicKey(pubKeyName, publicKey);
	auto fakeSignature = Standalone<StringRef>();
	{
		// alg=none
		fmt::print("Testing alg=none case\n");
		auto arena = Arena();
		authz::jwt::TokenRef tokenSpec =
		    authz::jwt::makeRandomTokenSpec(arena, *deterministicRandom(), authz::Algorithm::ES256);
		tokenSpec.keyId = pubKeyName;
		auto signInput = authz::jwt::makeSignInput(arena, tokenSpec);
		auto modifiedSignInput = modifyHeader(arena, signInput, [](rapidjson::Document& d) {
			auto algItr = d.FindMember("alg");
			ASSERT(algItr != d.MemberEnd());
			algItr->value.SetString("none");
		});
		// Test that we don't accept it unsigned, even if it's alg=none
		ASSERT(!TokenCache::instance().validate(tokenSpec.tenants.get()[0], modifiedSignInput));
		// Test that we don't accept it, even if it's signed by the proper key, because it's alg=none
		auto algNoneToken = authz::jwt::signToken(arena, modifiedSignInput, authz::Algorithm::ES256, privateKey);
		auto sigPart = algNoneToken;
		sigPart.eat("."_sr).eat("."_sr);
		// use this part for different, "spoofed signature" test case
		fakeSignature = Standalone<StringRef>(sigPart);
		ASSERT(!TokenCache::instance().validate(
		    tokenSpec.tenants.get()[0],
		    authz::jwt::signToken(arena, modifiedSignInput, authz::Algorithm::ES256, privateKey)));
	}
	{
		// missing signature
		fmt::print("Testing missing signature case\n");
		auto arena = Arena();
		auto tokenSpec = authz::jwt::makeRandomTokenSpec(arena, *deterministicRandom(), authz::Algorithm::ES256);
		tokenSpec.keyId = pubKeyName;
		auto signInput = authz::jwt::makeSignInput(arena, tokenSpec);
		ASSERT(!TokenCache::instance().validate(tokenSpec.tenants.get()[0], signInput));
	}
	{
		// spoofed signature
		fmt::print("Testing spoofed signature case\n");
		auto arena = Arena();
		ASSERT(!fakeSignature.empty());
		auto tokenSpec = authz::jwt::makeRandomTokenSpec(arena, *deterministicRandom(), authz::Algorithm::ES256);
		tokenSpec.keyId = pubKeyName;
		auto signInput = authz::jwt::makeSignInput(arena, tokenSpec);
		auto tokenWithFakeSig = signInput.withSuffix("."_sr, arena).withSuffix(fakeSignature, arena);
		ASSERT(!TokenCache::instance().validate(tokenSpec.tenants.get()[0], tokenWithFakeSig));
	}
	{
		// algorithm confusion: RS256
		auto rsaPrivateKey = mkcert::makeRsa4096Bit();
		fmt::print("Testing algorithm confusion case: RS256\n");
		auto arena = Arena();
		auto tokenSpec = authz::jwt::makeRandomTokenSpec(arena, *deterministicRandom(), authz::Algorithm::ES256);
		tokenSpec.keyId = pubKeyName;
		auto signInput = authz::jwt::makeSignInput(arena, tokenSpec);
		auto rsaSignInput = modifyHeader(arena, signInput, [](rapidjson::Document& d) {
			auto algItr = d.FindMember("alg");
			ASSERT(algItr != d.MemberEnd());
			algItr->value.SetString("RS256");
		});
		ASSERT(!TokenCache::instance().validate(
		    tokenSpec.tenants.get()[0],
		    authz::jwt::signToken(arena, rsaSignInput, authz::Algorithm::RS256, rsaPrivateKey)));
	}
	{
		// algorithm confusion: HS256
		fmt::print("Testing algorithm confusion case: HS256\n");
		// As we don't implement HS256, we simply test that it fails to parse-validate
		auto arena = Arena();
		ASSERT(!fakeSignature.empty());
		auto tokenSpec = authz::jwt::makeRandomTokenSpec(arena, *deterministicRandom(), authz::Algorithm::ES256);
		tokenSpec.keyId = pubKeyName;
		auto signInput = authz::jwt::makeSignInput(arena, tokenSpec);
		auto hs256SignInput = modifyHeader(arena, signInput, [](rapidjson::Document& d) {
			auto algItr = d.FindMember("alg");
			ASSERT(algItr != d.MemberEnd());
			algItr->value.SetString("HS256");
		});
		ASSERT(!TokenCache::instance().validate(
		    tokenSpec.tenants.get()[0], hs256SignInput.withSuffix("."_sr, arena).withSuffix(fakeSignature, arena)));
	}
	return Void();
}

namespace {

void appendRandomJsonString(IRandom& rng, std::ostringstream& o) {
	auto const len = rng.randomInt(1, 30);
	if (rng.random01() < 0.1) {
		// random byte string (illegal)
		auto str = std::string(len, '\0');
		for (auto& ch : str) {
			ch = rng.randomUInt32() % 256;
		}
		o << '"' << str << '"';
	} else {
		// alphanum string (legal)
		o << '"' << rng.randomAlphaNumeric(len) << '"';
	}
}

FDB_BOOLEAN_PARAM(TerminalTypesOnly); // no need to nest objects/arrays

void appendRandomJsonValue(IRandom& rng, std::ostringstream& o, TerminalTypesOnly terminalTypesOnly) {
	auto valueAppenders = std::vector<std::function<void(IRandom&, std::ostringstream&)>>{
		[](IRandom& rng, std::ostringstream& o) {
		    // String case
		    appendRandomJsonString(rng, o);
		},
		[](IRandom& rng, std::ostringstream& o) {
		    // Numeric case
		    switch (rng.randomInt(0, 2 + 1)) {
		    case 0:
			    // boolean
			    o << (rng.coinflip() ? "true" : "false");
			    break;
		    case 1:
			    // int
			    o << rng.randomInt64(0, 0xfffffff);
			    break;
		    case 2:
		    default:
			    // float
			    o << rng.random01() * 1e10;
			    break;
		    }
		},
		[](IRandom& rng, std::ostringstream& o) {
		    // array
		    o << "[";
		    auto const arrayLen = rng.randomInt(0, 4);
		    for (auto i = 0; i < arrayLen; i++) {
			    if (i > 0) {
				    o << ",";
			    }
			    appendRandomJsonValue(rng, o, TerminalTypesOnly::True);
		    }
		    o << "]";
		},
		[](IRandom& rng, std::ostringstream& o) {
		    // object
		    o << "{";
		    auto const numAttrs = rng.randomInt(0, 8);
		    for (auto i = 0; i < numAttrs; i++) {
			    if (i > 0) {
				    o << ",";
			    }
			    appendRandomJsonString(rng, o);
			    o << ":";
			    appendRandomJsonValue(rng, o, TerminalTypesOnly::True);
		    }
		    o << "}";
		},
	};
	auto const choiceBound = terminalTypesOnly ? valueAppenders.size() - 2 : valueAppenders.size();
	valueAppenders[rng.randomInt(0, choiceBound)](rng, o);
}

// Utility function to fill out JSON object with random fields and values.
// Assumes that '{' has already been written to the output stream.
void appendRandomJsonObjectAttribute(IRandom& rng, std::ostringstream& o) {
	appendRandomJsonString(rng, o);
	o << ":";
	appendRandomJsonValue(rng, o, TerminalTypesOnly::False);
}

} // anonymous namespace

TEST_CASE("/fdbrpc/authz/TokenCache/FuzzedStringDoesNotBreakCache") {
	// Generate a bunch of random token byte strings without consideration for encoding or structure.
	// The goal is to test that it does not crash the server or break the correctness of the cache.
	auto privateKey = mkcert::makeEcP256();
	auto publicKey = privateKey.toPublic();
	auto pubKeyName = "somePublicKey"_sr;
	auto publicKeyClearGuard = ScopeExit([pubKeyName]() { FlowTransport::transport().removePublicKey(pubKeyName); });
	FlowTransport::transport().addPublicKey(pubKeyName, publicKey);
	auto& rng = *deterministicRandom();
	for (auto i = 0; i < 1000; i++) {
		auto arena = Arena();
		auto length = rng.randomInt(1, 1000);
		auto buf = new (arena) uint8_t[length];
		for (auto i = 0; i < length; i++) {
			buf[i] = rng.randomUInt32() % 256;
		}
		ASSERT(!TokenCache::instance().validate(TenantId(rng.randomInt64(0, 0xfffffff)), StringRef(buf, length)));
	}
	// Now test with random strings that follow the token string structure:
	// i.e. 3 base64url-encoded string segments joined by '.' where the first two segments are JSON,
	// but the JSON parts do not adhere to the JWT schema.
	for (auto i = 0; i < 1000; i++) {
		auto arena = Arena();
		auto generateBase64UrlEncodedJsonObject = [](IRandom& rng, Arena& arena, int numAttrs) -> StringRef {
			auto stream =
			    std::ostringstream(); // cannot be using rapidjson Writer, as we may use JSON-illegal char sequences.
			stream << "{";
			for (auto i = 0; i < numAttrs; i++) {
				if (i > 0) {
					stream << ", ";
				}
				appendRandomJsonObjectAttribute(rng, stream);
			}
			stream << "}";
			auto objectStr = std::move(stream).str();
			if (rng.random01() < 0.1) {
				fmt::print("Sample object: {}\n", StringRef(objectStr).printable());
			}
			return base64::url::encode(arena, StringRef(objectStr));
		};
		auto headerPart = generateBase64UrlEncodedJsonObject(rng, arena, rng.randomInt(0, 8));
		auto payloadPart = generateBase64UrlEncodedJsonObject(rng, arena, rng.randomInt(0, 15));
		auto token = StringRef();
		if (rng.random01() < 0.1) {
			// do a proper JWT signing (expensive)
			auto signInput = concatenateStrings(arena, headerPart, "."_sr, payloadPart);
			token = authz::jwt::signToken(arena, signInput, authz::Algorithm::ES256, privateKey);
		} else {
			// just fit the structure with randomly generated bytes encoded in base64url
			auto plainSig = std::string(rng.randomUInt32() % 40 + 50, '\0');
			for (auto& byte : plainSig) {
				byte = rng.randomUInt32() % 256;
			}
			auto b64urlSig = base64::url::encode(arena, plainSig);
			token = concatenateStrings(arena, headerPart, "."_sr, payloadPart, "."_sr, b64urlSig);
		}
		ASSERT(!TokenCache::instance().validate(TenantId(rng.randomInt64(0, 0xfffffff)), token));
	}
	return Void();
}

namespace {

void modifyValue(IRandom& rng, rapidjson::Document& d, rapidjson::Value::MemberIterator itr) {
	using JsonValue = rapidjson::Value;
	ASSERT(itr != d.MemberEnd());
	auto setRandomTypeAndValue = [&rng, &d](JsonValue& value) {
		auto const choice = rng.randomInt(0, 5 + 1);
		switch (choice) {
		case 0:
			value.SetInt64(rng.randomInt64(0, 0xffffffffffLL));
			break;
		case 1:
			value.SetString(rng.randomAlphaNumeric(rng.randomInt(0, 50)).c_str(), d.GetAllocator());
			break;
		case 2:
			value.SetDouble(rng.random01() * 1e10 * (rng.coinflip() ? -1.0 : 1.0));
			break;
		case 3:
			value.SetBool(rng.coinflip());
			break;
		case 4: {
			auto& array = value.SetArray();
			auto len = rng.randomInt(0, 3);
			for (auto i = 0; i < len; i++) {
				if (rng.coinflip()) {
					array.PushBack(JsonValue(rng.randomInt(0, 100)), d.GetAllocator());
				} else {
					array.PushBack(JsonValue(rng.randomAlphaNumeric(rng.randomInt(0, 10)).c_str(), d.GetAllocator()),
					               d.GetAllocator());
				}
			}
		} break;
		case 5:
		default: {
			auto& object = value.SetObject();
			auto len = rng.randomInt(0, 4);
			for (auto i = 0; i < len; i++) {
				auto key = rng.randomAlphaNumeric(rng.randomInt(1, 15));
				if (rng.coinflip()) {
					object.AddMember(JsonValue(key.c_str(), d.GetAllocator()),
					                 JsonValue(rng.randomInt(-1000, 1000)),
					                 d.GetAllocator());
				} else {
					object.AddMember(JsonValue(key.c_str(), d.GetAllocator()),
					                 JsonValue(rng.randomAlphaNumeric(rng.randomInt(0, 60)).c_str(), d.GetAllocator()),
					                 d.GetAllocator());
				}
			}
		} break;
		}
	};
	auto& value = itr->value;
	if (value.IsUint()) {
		if (rng.random01() < 0.1) {
			d.RemoveMember(itr);
		} else if (rng.coinflip()) {
			// maintain the type, but use different value
			value.SetUint64(rng.randomUInt64() % static_cast<uint64_t>((g_network->timer() + 1e4) * 2));
		} else {
			setRandomTypeAndValue(value);
		}
	} else if (value.IsString()) {
		if (rng.random01() < 0.1) {
			d.RemoveMember(itr);
		} else if (rng.coinflip()) {
			value.SetString(rng.randomAlphaNumeric(rng.randomInt(0, 20)).c_str(), d.GetAllocator());
		} else {
			setRandomTypeAndValue(value);
		}
	} else {
		if (rng.random01() < 0.3) {
			setRandomTypeAndValue(value);
		}
	}
}

} // anonymous namespace

TEST_CASE("/fdbrpc/authz/TokenCache/FuzzLegalTokenFields") {
	// Generate a bunch of random token byte strings without consideration for encoding or structure.
	// The goal is to test that it does not crash the server or break the correctness of the cache.
	auto privateKey = mkcert::makeEcP256();
	auto publicKey = privateKey.toPublic();
	auto pubKeyName = "somePublicKey"_sr;
	auto publicKeyClearGuard = ScopeExit([pubKeyName]() { FlowTransport::transport().removePublicKey(pubKeyName); });
	FlowTransport::transport().addPublicKey(pubKeyName, publicKey);
	auto& rng = *deterministicRandom();

	auto headerModifiers = std::vector<std::function<void(rapidjson::Document&)>>{
		[&rng](rapidjson::Document& d) {
		    auto itr = d.FindMember("alg");
		    ASSERT(itr != d.MemberEnd());
		    modifyValue(rng, d, itr);
		},
		[&rng](rapidjson::Document& d) {
		    auto itr = d.FindMember("kid");
		    ASSERT(itr != d.MemberEnd());
		    modifyValue(rng, d, itr);
		},
		[&rng](rapidjson::Document& d) {
		    auto itr = d.FindMember("typ");
		    ASSERT(itr != d.MemberEnd());
		    modifyValue(rng, d, itr);
		},
	};
	auto payloadModifiers = std::vector<std::function<void(rapidjson::Document&)>>{
		[&rng](rapidjson::Document& d) {
		    auto itr = d.FindMember("iss");
		    ASSERT(itr != d.MemberEnd());
		    modifyValue(rng, d, itr);
		},
		[&rng](rapidjson::Document& d) {
		    auto itr = d.FindMember("sub");
		    ASSERT(itr != d.MemberEnd());
		    modifyValue(rng, d, itr);
		},
		[&rng](rapidjson::Document& d) {
		    auto itr = d.FindMember("aud");
		    ASSERT(itr != d.MemberEnd());
		    modifyValue(rng, d, itr);
		},
		[&rng](rapidjson::Document& d) {
		    auto itr = d.FindMember("jti");
		    ASSERT(itr != d.MemberEnd());
		    modifyValue(rng, d, itr);
		},
		[&rng](rapidjson::Document& d) {
		    auto itr = d.FindMember("iat");
		    ASSERT(itr != d.MemberEnd());
		    modifyValue(rng, d, itr);
		},
		[&rng](rapidjson::Document& d) {
		    auto itr = d.FindMember("exp");
		    ASSERT(itr != d.MemberEnd());
		    modifyValue(rng, d, itr);
		},
		[&rng](rapidjson::Document& d) {
		    auto itr = d.FindMember("nbf");
		    ASSERT(itr != d.MemberEnd());
		    modifyValue(rng, d, itr);
		},
		[&rng](rapidjson::Document& d) {
		    auto itr = d.FindMember("tenants");
		    ASSERT(itr != d.MemberEnd());
		    modifyValue(rng, d, itr);
		},
	};

	for (auto i = 0; i < 1000; i++) {
		auto arena = Arena();
		auto spec = authz::jwt::makeRandomTokenSpec(arena, rng, authz::Algorithm::ES256);
		spec.keyId = pubKeyName;
		auto signInput = authz::jwt::makeSignInput(arena, spec);
		auto headerModifiedInput = modifyHeader(arena, signInput, [&rng, &headerModifiers](rapidjson::Document& d) {
			for (const auto& modifier : headerModifiers) {
				if (rng.coinflip()) {
					modifier(d);
				}
			}
		});
		auto fuzzedInput = modifyPayload(arena, headerModifiedInput, [&rng, &payloadModifiers](rapidjson::Document& d) {
			for (const auto& modifier : payloadModifiers) {
				if (rng.coinflip()) {
					modifier(d);
				}
			}
		});
		auto realSignature = false;
		auto token = StringRef();
		if (rng.random01() < 0.3) {
			token = authz::jwt::signToken(arena, fuzzedInput, authz::Algorithm::ES256, privateKey);
			realSignature = true;
		} else {
			auto randStr = rng.randomAlphaNumeric(rng.randomInt(30, 80));
			if (rng.coinflip()) {
				auto encodedStr = base64::url::encode(arena, StringRef(randStr));
				token = concatenateStrings(arena, fuzzedInput, "."_sr, encodedStr);
			} else {
				token = concatenateStrings(arena, fuzzedInput, "."_sr, StringRef(randStr));
			}
		}
		auto expectVerifyOk = false;
		// If we have a valid signature, this should be set to valid tenant id from token during inspectSignInput()
		// lambda below. Otherwise, it does not matter if it's any random ID.
		TenantId tenantIdToValidate = rng.randomInt64(1, 0xfffffff);
		if (realSignature) {
			// The likelihood of a sign input going through above fuzzing modifications and passing verification
			// is extremely rare, but we need to guard against flaky tests.
			inspectSignInput(
			    fuzzedInput,
			    [&expectVerifyOk, &pubKeyName, &tenantIdToValidate](rapidjson::Document const& header,
			                                                        rapidjson::Document const& payload) -> void {
				    using namespace std::literals::string_view_literals;
				    auto memberIsArrayOfStrings = [](rapidjson::Document const& d, const char* memberName) -> bool {
					    auto itr = d.FindMember(memberName);
					    if (itr == d.MemberEnd()) {
						    return false;
					    }
					    if (!itr->value.IsArray()) {
						    return false;
					    }
					    for (auto i = 0; i < itr->value.Size(); i++) {
						    if (!itr->value[i].IsString()) {
							    return false;
						    }
					    }
					    return true;
				    };
				    auto verifyTenantsFieldAndGetTenantId = [](rapidjson::Document const& d) -> Optional<TenantId> {
					    auto itr = d.FindMember("tenants");
					    if (itr == d.MemberEnd() || !itr->value.IsArray()) {
						    return {};
					    }
					    auto& array = itr->value;
					    Optional<TenantId> ret;
					    auto arena = Arena();
					    for (auto i = 0; i < array.Size(); i++) {
						    if (!array[i].IsString()) {
							    return {};
						    }
						    Optional<StringRef> decodedString =
						        base64::decode(arena,
						                       StringRef(reinterpret_cast<const uint8_t*>(array[i].GetString()),
						                                 array[i].GetStringLength()));
						    if (!decodedString.present() || decodedString.get().size() != sizeof(TenantId)) {
							    return {};
						    }
						    TenantId tenantId = *reinterpret_cast<const TenantId*>(decodedString.get().begin());
						    tenantId = fromBigEndian64(tenantId);
						    ret = tenantId;
					    }
					    return ret;
				    };

				    auto ok = true;
				    ok = ok && header.HasMember("alg") && header["alg"].IsString() &&
				         header["alg"].GetString() == "ES256"sv;
				    ok = ok && header.HasMember("typ") && header["typ"].IsString() &&
				         header["typ"].GetString() == "JWT"sv;
				    ok = ok && header.HasMember("kid") && header["kid"].IsString() &&
				         header["kid"].GetString() == pubKeyName.toStringView();
				    ok = ok && (!payload.HasMember("iss") || payload["iss"].IsString());
				    ok = ok && (!payload.HasMember("sub") || payload["sub"].IsString());
				    ok = ok && (!payload.HasMember("aud") || payload["aud"].IsString() ||
				                memberIsArrayOfStrings(payload, "aud"));
				    ok = ok && (!payload.HasMember("jti") || payload["jti"].IsString());
				    ok =
				        ok && (payload.HasMember("iat") && payload["iat"].IsNumber() && payload["iat"].GetDouble() > 0);
				    auto optTenantId = verifyTenantsFieldAndGetTenantId(payload);
				    ok = ok && optTenantId.present();
				    if (ok) {
					    tenantIdToValidate = optTenantId.get();
				    }
				    // This part has a very low likelihood for flakyness if we call timer() because each call to timer()
				    // may increase time by up to 0.05s upon next call by TokenCache in simulation mode.
				    // Still, defer as much as possible.
				    auto currentTime = g_network->timer();
				    ok = ok && payload.HasMember("exp") && payload["exp"].IsNumber() &&
				         payload["exp"].GetDouble() > 0 && payload["exp"].GetDouble() > currentTime;
				    ok = ok && payload.HasMember("nbf") && payload["nbf"].IsNumber() &&
				         payload["nbf"].GetDouble() > 0 && payload["nbf"].GetDouble() <= currentTime;
				    expectVerifyOk = ok;
			    });
		}
		ASSERT_EQ(TokenCache::instance().validate(tenantIdToValidate, token), expectVerifyOk);
	}
	return Void();
}
