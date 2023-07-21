#include <boost/spirit/home/x3/support/ast/position_tagged.hpp>
#include <boost/spirit/home/x3/support/ast/variant.hpp>
#include <boost/spirit/home/x3/support/utility/error_reporting.hpp>
#include <boost/spirit/home/x3/support/utility/annotate_on_success.hpp>
#include <boost/spirit/home/x3/numeric/real.hpp>
#include <boost/fusion/adapted/std_pair.hpp>
#include <boost/spirit/home/x3.hpp>
#include <boost/variant.hpp>
#include <boost/optional.hpp>
#include <cassert>
#include <cmath>
#include <iostream>
#include <iomanip>
#include <map>
#include <string>
#include <variant>
#include <vector>
#include <type_traits>
#include <fmt/format.h>
#include <cxxabi.h>

namespace boost::spirit::x3 {

// utility for AST comparisons in tests
template <typename ... Types>
bool operator==(const variant<Types...>& lhs, const variant<Types...>& rhs) {
	return lhs.get() == rhs.get();
}

}

namespace x3 = boost::spirit::x3;

namespace ast {

struct scalar : x3::variant<bool, double, uint64_t, int64_t>, x3::position_tagged {
	using base_type::base_type;
	using base_type::operator=;

	static constexpr double EPS = 1e-10;

	bool operator==(const scalar& other) const {
		auto const* lhs = boost::get<double>(this);
		auto const* rhs = boost::get<double>(&other);
		if (lhs && rhs) {
			if (*lhs == *rhs)
				return true;
			// as parser should be producing something close to the expected value, we use "essentially equal" semantics
			if (std::fabs(*lhs - *rhs) < std::min(std::fabs(*lhs), std::fabs(*rhs)) * EPS) {
				return true;
			} else {
				std::cout << "scalar double mismatch: left-hand (" << *lhs << ") is not equal to right-hand (" << *rhs << ") within margin " << EPS << "\n";
				return false;
			}
		} else {
			if (get() != other.get()) {
				std::cout << "scalar mismatch: which = " << get().which() << " vs " << other.get().which() << "\n";
				return false;
			} else {
				return true;
			}
		}
	}
};

struct single_value : x3::variant<scalar, std::string>, x3::position_tagged {
	using base_type::base_type;
	using base_type::operator=;

	bool operator==(const single_value& other) const {
		if (get() != other.get()) {
			std::cout << "unmatched single_value: which = " << get().which() << " vs " << other.get().which() << "\n";
			return false;
		} else {
			return true;
		}
	}
};

using metadatum = std::pair<std::string, boost::optional<single_value>>;
using metadata = std::map<std::string, boost::optional<single_value>>;

struct include {
	std::string filename;
	bool operator==(const include& other) const noexcept = default;
};

struct file_identifier_decl {
	std::string id;
	bool operator==(const file_identifier_decl& other) const noexcept = default;
};

struct namespace_decl {
	std::vector<std::string> ns;
	bool operator==(const namespace_decl& other) const noexcept = default;
};

struct attribute_decl {
	std::string name;
	bool operator==(const attribute_decl& other) const noexcept = default;
};

// when a table member is another table/struct/union or a struct member is another struct
struct typeref {
	std::string name;
	// support for when *name* refers to an enum type and the field has a default value
	boost::optional<std::string> default_;
	bool operator==(const typeref& other) const noexcept = default;
};

template <class T>
struct scalar_type {
	boost::optional<T> default_;
	bool operator==(const scalar_type& other) const noexcept = default;
};

using scalar_types_variant = x3::variant<
		scalar_type<bool>, 
		scalar_type<int8_t>, 
		scalar_type<uint8_t>,
		scalar_type<int16_t>,
		scalar_type<uint16_t>,
		scalar_type<int64_t>,
		scalar_type<uint64_t>,
		scalar_type<int32_t>,
		scalar_type<uint32_t>,
		scalar_type<float>,
		scalar_type<double>>;

struct scalar_typeinfo : scalar_types_variant {
	using base_type::base_type;
	using base_type::operator=;

	bool operator==(const scalar_typeinfo& other) const noexcept = default;
};

template <typename T>
struct type_tag {
	// If T is scalar, it's always a vector (non-vector case is covered by scalar_type)
	using type = T;
	bool operator==(const type_tag& other) const noexcept {
		return true;
	}
};

using object_types_variant = x3::variant<
		type_tag<std::string>,
		typeref>;

struct object_typeinfo : object_types_variant {
	using base_type::base_type;
	using base_type::operator=;

	bool operator==(const object_typeinfo& other) const noexcept {
		return get() == other.get();
	}
};

using singular_types_variant = x3::variant<
		type_tag<bool>, 
		type_tag<int8_t>, 
		type_tag<uint8_t>,
		type_tag<int16_t>,
		type_tag<uint16_t>,
		type_tag<int64_t>,
		type_tag<uint64_t>,
		type_tag<int32_t>,
		type_tag<uint32_t>,
		type_tag<float>,
		type_tag<double>,
		type_tag<std::string>,
		typeref>;

struct vector_typeinfo : singular_types_variant {
	using base_type::base_type;
	using base_type::operator=;

	bool operator==(const vector_typeinfo& other) const noexcept {
		return get() == other.get();
	}
};

using table_fieldtype_variant = x3::variant<scalar_typeinfo, object_typeinfo, vector_typeinfo>;

struct table_field_decl {
	std::string name;
	table_fieldtype_variant typeinfo;
	boost::optional<metadata> meta;

	bool operator==(const table_field_decl& other) const noexcept {
		return name == other.name && typeinfo.get() == other.typeinfo.get() && meta == other.meta;
	}
};

struct table_decl {
	std::string name;
	boost::optional<metadata> meta;
	std::vector<table_field_decl> fields;
	bool operator==(const table_decl& other) const noexcept = default;
};

using struct_fieldtype_variant = x3::variant<
		type_tag<bool>, 
		type_tag<int8_t>, 
		type_tag<uint8_t>,
		type_tag<int16_t>,
		type_tag<uint16_t>,
		type_tag<int64_t>,
		type_tag<uint64_t>,
		type_tag<int32_t>,
		type_tag<uint32_t>,
		type_tag<float>,
		type_tag<double>,
		typeref>;

struct struct_field_decl {
	std::string name;
	struct_fieldtype_variant typeinfo;
	boost::optional<metadata> meta;
	bool operator==(const struct_field_decl& other) const noexcept {
		return name == other.name && typeinfo.get() == other.typeinfo.get() && meta == other.meta;
	}
};

struct struct_decl {
	std::string name;
	boost::optional<metadata> meta;
	std::vector<struct_field_decl> fields;
	bool operator==(const struct_decl& other) const noexcept = default;
};

using enumval_variant = x3::variant<uint64_t, int64_t>;

struct enumval_decl {
	std::string name;
	// whether the assigned value fits in the type that backs enum is deferred to code generator
	boost::optional<enumval_variant> value;
	bool operator==(const enumval_decl& other) const noexcept = default;
};

using enum_type_variant = x3::variant<
						type_tag<int8_t>,
						type_tag<uint8_t>,
						type_tag<int16_t>,
						type_tag<uint16_t>,
						type_tag<int64_t>,
						type_tag<uint64_t>,
						type_tag<int32_t>,
						type_tag<uint32_t>>;

struct enum_decl {
	std::string name;
	enum_type_variant typeinfo;
	boost::optional<metadata> meta;
	std::vector<enumval_decl> values;
	bool operator==(const enum_decl& other) const noexcept = default;
};

struct union_subtype_decl {
	std::string type;
	boost::optional<std::string> alias;
	bool operator==(const union_subtype_decl& other) const noexcept = default;
};

struct union_decl {
	std::string name;
	boost::optional<metadata> meta;
	std::vector<union_subtype_decl> subtypes;
	bool operator==(const union_decl& other) const noexcept = default;
};

} // namespace ast

BOOST_FUSION_ADAPT_STRUCT(ast::file_identifier_decl, id)
BOOST_FUSION_ADAPT_STRUCT(ast::include, filename)
BOOST_FUSION_ADAPT_STRUCT(ast::namespace_decl, ns)
BOOST_FUSION_ADAPT_STRUCT(ast::attribute_decl, name)
BOOST_FUSION_ADAPT_STRUCT(ast::typeref, name, default_)
BOOST_FUSION_ADAPT_STRUCT(ast::table_field_decl, name, typeinfo, meta)
BOOST_FUSION_ADAPT_STRUCT(ast::table_decl, name, meta, fields)
BOOST_FUSION_ADAPT_STRUCT(ast::struct_field_decl, name, typeinfo, meta)
BOOST_FUSION_ADAPT_STRUCT(ast::struct_decl, name, meta, fields)
BOOST_FUSION_ADAPT_STRUCT(ast::enumval_decl, name, value)
BOOST_FUSION_ADAPT_STRUCT(ast::enum_decl, name, typeinfo, meta, values)
BOOST_FUSION_ADAPT_STRUCT(ast::union_subtype_decl, alias, type) // member order reversed to accommodate the parse order
BOOST_FUSION_ADAPT_STRUCT(ast::union_decl, name, meta, subtypes)

BOOST_FUSION_ADAPT_TPL_STRUCT(
               (T), (ast::scalar_type) (T),
               (boost::optional<T>, default_))

namespace detail {

std::string demangle_symbol(const std::string& mangled) {
	int status = 0;
	size_t length = 0;
	auto demangled = abi::__cxa_demangle(mangled.c_str(), nullptr, &length, &status);
	if (demangled) {
		std::string ret(demangled, length);
		free(demangled);
		// each compounding of parsers (by operator >, +, |, etc) adds to the parser type, whose C++ type id could grow to
		// hundreds of characters. Try to simplify this by trying to find a rule type that the parser type maps to.
		auto const rule_pos = ret.rfind("boost::spirit::x3::rule");
		if (rule_pos != std::string::npos) {
			// dump from the rule part.
			ret = ret.substr(rule_pos + 15);
		}
		return ret;
	} else {
		// exception.which() may not be a mangled C++ type id, in which case we just return as-is.
		return mangled;
	}
}

} // namespace detail

namespace rule {

struct error_handler
{
    template <typename Iterator, typename Exception, typename Context>
    x3::error_handler_result on_error(
        Iterator& first, Iterator const& last
      , Exception const& x, Context const& context)
    {
        auto& error_handler = x3::get<x3::error_handler_tag>(context).get();
        std::string message = "Error! Expecting: " + detail::demangle_symbol(x.which()) + " here:";
        error_handler(x.where(), message);
        return x3::error_handler_result::fail;
    }
};

// require doubles to have a decimal point to prefer ints over doubles
x3::real_parser<double, x3::strict_real_policies<double>> const strict_double;
x3::real_parser<float, x3::strict_real_policies<float>> const strict_float;
//x3::int_parser<int64_t, 16> const hex_int64;
x3::uint_parser<uint64_t, 16> const hex_uint64;

#ifdef DEFINE_RULE
#error "DEFINE_RULE macro clashes with existing definition"
#endif
#define DEFINE_RULE(NAME, RULE, RESOLVED_TYPE) \
	[[maybe_unused]] x3::rule<struct NAME##_class, RESOLVED_TYPE> const NAME = #NAME; \
	[[maybe_unused]] auto const NAME##_def = RULE; \
	struct NAME##_class : error_handler, x3::annotate_on_success {}; \
	BOOST_SPIRIT_DEFINE(NAME)


// identifiers: names of namespaces, attributes, types, and fields
DEFINE_RULE(ident,
			(x3::ascii::alpha | x3::ascii::char_('_')) >> *(x3::ascii::alnum | x3::ascii::char_('_')),
			std::string);

// parses references to other types in field declarations (parsed as identifiers) into ast::typeref
DEFINE_RULE(typeref, ident, ast::typeref);

DEFINE_RULE(char_in_quoted_string,
			(
				 "\\n" > x3::attr('\n')
			   | "\\t" > x3::attr('\t')
			   | "\\r" > x3::attr('\r')
			   | "\\b" > x3::attr('\b')
			   | "\\f" > x3::attr('\f')
			   | "\\x" > x3::uint_parser<uint8_t, 16, 2, 2>() // bytes in hex: e.g. "\xff"; always two digits
			   | "\\"  > x3::char_("\"\\/") // escaped double-quote, backslash, and slash
			   | ~x3::char_('"')
		   ), char);

// quoted string constants with escapes. missing feature: "\uXXXX" which should parses into utf8
DEFINE_RULE(string_constant,
			(x3::lexeme['"' > *(char_in_quoted_string) > '"']),
			std::string);

DEFINE_RULE(four_quoted_string_chars,
			x3::repeat(4)[char_in_quoted_string],
			std::string);

DEFINE_RULE(file_identifier_constant,
			(x3::lexeme['"' > four_quoted_string_chars > '"']),
			std::string);

// constants and metadata
DEFINE_RULE(boolean_constant, x3::bool_, bool);
DEFINE_RULE(unsigned_constant,
			((x3::lit("0x") > hex_uint64) | x3::uint64),
			uint64_t);
DEFINE_RULE(scalar,
			boolean_constant | strict_double | unsigned_constant | x3::int64,
			// as the parser evaluates rules from left-to-right, strict double must come before ints
			// to prevent parsing e.g. "1.0" as an unsigned_constant 1 and leaving ".0" behind.
			// unsigned_constant must come before int64 to parse "3" as unsigned int and not int
			ast::scalar);
DEFINE_RULE(single_value, scalar | string_constant, ast::single_value);
DEFINE_RULE(metadatum,
			ident > -(x3::lit(':') > single_value),
			ast::metadatum);
DEFINE_RULE(metadata,
			x3::lit('(') > (metadatum % ',') > ')',
			ast::metadata);

// includes, file identifiers, namespaces, attributes, root
DEFINE_RULE(include,
			x3::lit("include") > string_constant > ';',
			ast::include);
DEFINE_RULE(file_identifier_decl, x3::lit("file_identifier") > file_identifier_constant > ';', ast::file_identifier_decl);
DEFINE_RULE(namespace_decl,
			x3::lit("namespace") > ident > *(x3::lit('.') > ident) >> ';',
			ast::namespace_decl);
DEFINE_RULE(attribute_decl,
			x3::lit("attribute") >> (
				(x3::lit('"') > ident > x3::lit('"')) |
				ident
			),
			ast::attribute_decl);

DEFINE_RULE(
		bool_field_part,
		x3::lit("bool") > -(x3::lit('=') > x3::bool_),
		ast::scalar_type<bool>);

DEFINE_RULE(
		int8_field_part,
		(x3::lit("byte") | "int8") > -(x3::lit('=') > x3::int8),
		ast::scalar_type<int8_t>);

DEFINE_RULE(
		uint8_field_part,
		(x3::lit("ubyte") | "uint8") > -(x3::lit('=') > x3::uint8),
		ast::scalar_type<uint8_t>);

DEFINE_RULE(
		int16_field_part,
		(x3::lit("short") | "int16") > -(x3::lit('=') > x3::int16),
		ast::scalar_type<int16_t>);

DEFINE_RULE(
		uint16_field_part,
		(x3::lit("ushort") | "uint16") > -(x3::lit('=') > x3::uint16),
		ast::scalar_type<uint16_t>);

DEFINE_RULE(
		int32_field_part,
		(x3::lit("int") | "int32") > -(x3::lit('=') > x3::int32),
		ast::scalar_type<int32_t>);

DEFINE_RULE(
		uint32_field_part,
		(x3::lit("uint") | "uint32") > -(x3::lit('=') > x3::uint32),
		ast::scalar_type<uint32_t>);

DEFINE_RULE(
		int64_field_part,
		(x3::lit("long") | "int64") > -(x3::lit('=') > x3::int64),
		ast::scalar_type<int64_t>);

DEFINE_RULE(
		uint64_field_part,
		(x3::lit("ulong") | "uint64") > -(x3::lit('=') > x3::uint64),
		ast::scalar_type<uint64_t>);

DEFINE_RULE(
		float32_field_part,
		(x3::lit("float") | "float32") > -(x3::lit('=') > strict_float),
		ast::scalar_type<float>);

DEFINE_RULE(
		float64_field_part,
		(x3::lit("double") | "float64") > -(x3::lit('=') > strict_double),
		ast::scalar_type<double>);

// [u]int32 must be placed at the last of the integer rules because of the alias "[u]int":
// e.g. parsing "uint64" may be parsed as "uint" and left partially parsed.
DEFINE_RULE(
		scalar_typeinfo,
		bool_field_part |
		int8_field_part |
		uint8_field_part |
		int16_field_part |
		uint16_field_part |
		int64_field_part |
		uint64_field_part |
		int32_field_part |
		uint32_field_part |
		float32_field_part |
		float64_field_part,
		ast::scalar_typeinfo);

auto const bool_type_part = x3::lit("bool") > x3::attr(ast::type_tag<bool>{});

auto const int8_type_part = (x3::lit("byte") | "int8") > x3::attr(ast::type_tag<int8_t>{});

auto const uint8_type_part = (x3::lit("ubyte") | "uint8") > x3::attr(ast::type_tag<uint8_t>{});

auto const int16_type_part = (x3::lit("short") | "int16") > x3::attr(ast::type_tag<int16_t>{});

auto const uint16_type_part = (x3::lit("ushort") | "uint16") > x3::attr(ast::type_tag<uint16_t>{});

auto const int32_type_part = (x3::lit("int32") | "int") > x3::attr(ast::type_tag<int32_t>{});

auto const uint32_type_part = (x3::lit("uint32") | "uint") > x3::attr(ast::type_tag<uint32_t>{});

auto const int64_type_part = (x3::lit("long") | "int64") > x3::attr(ast::type_tag<int64_t>{});

auto const uint64_type_part = (x3::lit("ulong") | "uint64") > x3::attr(ast::type_tag<uint64_t>{});

auto const float32_type_part = (x3::lit("float") | "float32") > x3::attr(ast::type_tag<float>{});

auto const float64_type_part = (x3::lit("double") | "float64") > x3::attr(ast::type_tag<double>{});

auto const string_type_part = x3::lit("string") > x3::attr(ast::type_tag<std::string>{});

DEFINE_RULE(typeref_type_part, ident > x3::attr(boost::none), ast::typeref);
DEFINE_RULE(typeref_field_part, ident > -(x3::lit('=') > ident), ast::typeref);
DEFINE_RULE(no_string_field_in_struct, !(x3::lit("string") >> ';'), x3::unused_type);

// [u]int32 must be placed at the last of the integer rules because of the alias "[u]int":
// e.g. parsing "[uint64]" may be parsed as "[uint" and throw an error because there's '6' instead of ']'
DEFINE_RULE(
		vector_typeinfo,
		(x3::lit('[') >
			(bool_type_part |
			 int8_type_part |
			 uint8_type_part |
			 int16_type_part |
			 uint16_type_part |
			 int64_type_part |
			 uint64_type_part |
			 int32_type_part |
			 uint32_type_part |
			 float32_type_part |
			 float64_type_part |
			 string_type_part |
			 typeref_type_part) > ']'),
		ast::vector_typeinfo);

DEFINE_RULE(
		object_typeinfo,
		(string_type_part | typeref_field_part),
		ast::object_typeinfo);

DEFINE_RULE(
		table_field_decl,
		(ident > ':' > (scalar_typeinfo | object_typeinfo | vector_typeinfo) > -metadata > ';'),
		ast::table_field_decl);

DEFINE_RULE(
		table_decl,
		(x3::lit("table") > ident > -metadata > '{' > +table_field_decl > '}'),
		ast::table_decl);

// Only scalars or other structs allowed, no defaults
DEFINE_RULE(
		struct_field_decl,
		(ident > ':' >
			no_string_field_in_struct >
			(
				bool_type_part |
				int8_type_part |
				uint8_type_part |
				int16_type_part |
				uint16_type_part |
				int64_type_part |
				uint64_type_part |
				int32_type_part |
				uint32_type_part |
				float32_type_part |
				float64_type_part |
				typeref_type_part
			) > -metadata > ';'),
		ast::struct_field_decl);

DEFINE_RULE(
		struct_decl,
		(x3::lit("struct") > ident > -metadata > '{' > +struct_field_decl > '}'),
		ast::struct_decl);

DEFINE_RULE(
		enumval_decl,
		(ident > -(x3::lit('=') > (unsigned_constant | x3::int64))),
		ast::enumval_decl);

DEFINE_RULE(
		enum_decl,
		(
			x3::lit("enum") > ident > ':' > (
				int8_type_part |
				uint8_type_part |
				int16_type_part |
				uint16_type_part |
				int64_type_part |
				uint64_type_part |
				int32_type_part |
				uint32_type_part) >
			-metadata >
			'{' > (enumval_decl % ',') > '}'
		),
		ast::enum_decl);

DEFINE_RULE(
		union_subtype_decl,
		-(ident >> ':') > ident,
		ast::union_subtype_decl);

DEFINE_RULE(
		union_decl,
		(x3::lit("union") > ident > -metadata > '{' > (union_subtype_decl % ',') > '}'),
		ast::union_decl);

#undef DEFINE_RULE

} // namespace rule

namespace test {

// default optional indicates that we expect parse to fail (i.e. a parse error or a partial match)
template <class AST>
using case_type = std::pair<std::string, boost::optional<AST>>;

template <class AST>
using input_set_type = std::vector<case_type<AST>>;

template <class Rule, class AST, class EQUALS_FN>
bool run_ast_test(
		const Rule& r,
		const input_set_type<AST>& cases,
		EQUALS_FN&& equals_fn) {
	std::cout << "Testing " << r.name << " parser...\n";
	unsigned failures = 0;
	for (const auto& [input, expected] : cases) {
		auto bg = input.begin();
		auto ed = input.end();
		auto output = AST();
		using error_handler_type = x3::error_handler<decltype(bg)>;
		error_handler_type error_handler(bg, ed, std::cerr);
		auto const parser = x3::with<x3::error_handler_tag>(std::ref(error_handler))[r];
		auto const parse_ok = x3::phrase_parse(bg, ed, parser, x3::space, output);
		auto const parse_complete = (bg == ed);
		auto test_ok = true;
		auto value_match = true;
		if (!expected) {
			// parse should fail
			test_ok = !parse_ok || !parse_complete;
		} else {
			if (parse_ok && parse_complete)
				value_match = equals_fn(*expected, output);
			test_ok = parse_ok && parse_complete && value_match;
		}
		std::cout << "  "  << (test_ok ? "O " : "X ");
		if (!test_ok) {
			if (!expected) {
				std::cout << "[parse_unexpectedly_succeeded] ";
			} else {
				if (!parse_ok)
					std::cout << "[parse_error] ";
				if (!parse_complete)
					std::cout << "[parse_incomplete_at_pos_" << (bg - input.begin()) << "_of_length_" << input.size() << "] ";
				if (!value_match)
					std::cout << "[value_mismatch] ";
			}
		} else if (!expected) {
			std::cout << "[parse_failed_as_expected] ";
		}
		std::cout << std::quoted(input) << '\n';
		failures += !test_ok;
	}
	if (failures)
		std::cout << failures << " cases failed\n";
	return failures == 0;
}

template <class Rule, class AST>
bool run_ast_test(const Rule& r, const input_set_type<AST>& cases) {
	return run_ast_test(r, cases, [](const AST& lhs, const AST& rhs) { return lhs == rhs; });
}

input_set_type<std::string> const string_constant_input_set =
	[]() -> input_set_type<std::string> {
		using namespace std::string_literals;
		return input_set_type<std::string> {
			{ "\"\\n\"", "\n"s },
			{ "\"\\t\"", "\t"s },
			{ "\"\\r\"", "\r"s },
			{ "\"\\b\"", "\b"s },
			{ "\"\\f\"", "\f"s },
			{ "\"\\\\\"", "\\"s },
			{ "\"\\/\"", "/"s },
			{ "\"\\\"\"", "\""s },
			{ "\"\\x13\\xdc\"", "\x13\xdc"s },
			{ "\"\\x1\"", {} }, // hex characters must contain two digits
		};
	}();

bool string_constant() {
	return run_ast_test(rule::string_constant, string_constant_input_set);
}

input_set_type<ast::scalar> const scalar_input_set = {
	{ "1.0", ast::scalar(1.0) }, // double
	{ "10", ast::scalar(uint64_t(10)) }, // uint64
	{ "-10.0", ast::scalar(-10.0) }, // double
	{ "-10", ast::scalar(int64_t(-10)) }, // int64
	{ "-3", ast::scalar(int64_t(-3)) }, // int64
	{ "0xfe", ast::scalar(uint64_t(0xfe)) }, // uint64
	{ "0xDeadBeefDeadBeef", ast::scalar(uint64_t(0xdeadbeefdeadbeefUL)) }, // uint64
	{ "0xdeadbeefdeadbeef", ast::scalar(uint64_t(0xdeadbeefdeadbeefUL)) }, // uint64
	{ "-0xdeadbeefdeadbeef", boost::none }, // negative hex int64 not supported yet TODO: make this throw a parse error with message
	{ "true", ast::scalar(true) }, // bool
	{ "false", ast::scalar(false) }, // bool
	{ "1", ast::scalar(uint64_t(1)) }, // uint64
	{ "0", ast::scalar(uint64_t(0)) }, // uint64
};

bool scalar() {
	return run_ast_test(rule::scalar, scalar_input_set);
}

// reuse scalar/string_constant test input for single value test
input_set_type<ast::single_value> const single_value_input_set =
	[]() -> input_set_type<ast::single_value> {
		auto ret = input_set_type<ast::single_value>();
		for (auto const& [input, expected] : string_constant_input_set) {
			if (expected) {
				ret.emplace_back(input, *expected);
			} else {
				ret.emplace_back(input, boost::none);
			}
		}
		for (auto const& [input, expected] : scalar_input_set) {
			if (expected) {
				ret.emplace_back(input, *expected);
			} else {
				ret.emplace_back(input, boost::none);
			}
		}
		return ret;
	}();

bool single_value() {
	return run_ast_test(rule::single_value, single_value_input_set);
}

input_set_type<ast::metadatum> const metadatum_input_set = []() {
	using namespace std::string_literals;
	auto ret = input_set_type<ast::metadatum>{
		{ "attr", ast::metadatum{ "attr", boost::none }},
		{ "abc: \"def\"", ast::metadatum{ "abc", ast::single_value("def"s) }},
		{ "id: 42", ast::metadatum{ "id", ast::single_value(ast::scalar(uint64_t(42))) }},
		{ "deprecated", ast::metadatum{ "deprecated", boost::none }},
		{ "required", ast::metadatum{ "required", boost::none }},
		{ "force_align: 16", ast::metadatum{ "force_align", ast::single_value(ast::scalar(uint64_t(16))) }},
		{ "nested_flatbuffer: \"someTable\"", ast::metadatum{ "nested_flatbuffer", ast::single_value("someTable"s) }},
	};
	return ret;
}();

bool metadatum() {
	return run_ast_test(
			rule::metadatum,
			metadatum_input_set,
			[](const ast::metadatum& lhs, const ast::metadatum& rhs) {
				auto const& [l_name, l_value] = lhs;
				auto const& [r_name, r_value] = rhs;
				if (l_name != r_name) {
					std::cout << "attribute names mismatch: " << std::quoted(l_name) << " vs " << std::quoted(r_name) << "\n";
					return false;
				}
				// not C++ parlance "lvalue", just left-hand value.
				auto const l_value_set = bool(l_value);
				auto const r_value_set = bool(r_value);
				if (l_value_set != r_value_set) {
					std::cout << "value availability mismatch: " << l_value_set << " vs " << r_value_set << "\n";
					return false;
				}
				if (!l_value_set && !r_value_set) {
					return true;
				}
				auto const& l_value_actual = *l_value;
				auto const& r_value_actual = *r_value;
				if (l_value_actual.get().which() != r_value_actual.get().which()) {
					std::cout << "value variant type mismatch: " << l_value_actual.get().which() << " vs " << r_value_actual.get().which() << "\n";
					return false;
				}
				if (l_value_actual.get() != r_value_actual.get()) {
					std::cout << "unmatched variant values\n";
					return false;
				}
				return true;
		});
}

input_set_type<ast::metadata> const metadata_input_set = []() {
	using namespace std::string_literals;
	input_set_type<ast::metadata> ret = {
		{
			"(a: 10, b: \"c\", d, e: true)"s,
			ast::metadata{
				{ "a", ast::single_value(ast::scalar(uint64_t(10))) },
				{ "b", ast::single_value("c"s) },
				{ "d", boost::none },
				{ "e", ast::single_value(ast::scalar(true)) }
			},
		},
		{
			// because x3 parser's parse-into-container class/function template uses map.insert() which does not
			// update entries in-place, any insertions to the existing key are silently ignored, which isn't great.
			"(a, b: 4, a: 3)",
			ast::metadata{
				{ "a", boost::none },
				{ "b", ast::single_value(ast::scalar(uint64_t(4))) },
			},
		},
		{
			"(a: true, a: \"def\")",
			ast::metadata{
				{ "a", ast::single_value(ast::scalar(true)) },
			},
		},
		{ "()", boost::none }, // empty metadata fails x3::list parser rule
	};
	return ret;
}();

bool metadata() {
	return run_ast_test(rule::metadata, metadata_input_set);
}

input_set_type<ast::scalar_typeinfo> scalar_typeinfo_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::scalar_typeinfo>{
		{
			"int = 10",
			ast::scalar_typeinfo{ast::scalar_type<int32_t>{10}},
		},
		{
			"bool",
			ast::scalar_typeinfo{ast::scalar_type<bool>{boost::none}},
		},
		{
			"bool = true",
			ast::scalar_typeinfo{ast::scalar_type<bool>{true}},
		},
		{
			"ulong",
			ast::scalar_typeinfo{ast::scalar_type<uint64_t>{boost::none}},
		},
		{
			"int64 = -5",
			ast::scalar_typeinfo{ast::scalar_type<int64_t>{-5}},
		},
		{
			"double = -3.14",
			ast::scalar_typeinfo{ast::scalar_type<double>{-3.14}},
		},
		{
			"double",
			ast::scalar_typeinfo{ast::scalar_type<double>{boost::none}},
		},
	};
}();

bool scalar_typeinfo() {
	return run_ast_test(rule::scalar_typeinfo, scalar_typeinfo_input_set);
}

input_set_type<ast::object_typeinfo> object_typeinfo_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::object_typeinfo>{
		{
			"string",
			ast::object_typeinfo{ast::type_tag<std::string>{}},
		},
		{
			"def",
			ast::object_typeinfo{ast::typeref{"def", boost::none}},
		},
		{
			"someType",
			ast::object_typeinfo{ast::typeref{"someType", boost::none}},
		},
		{
			"1someType", // identifiers cannot start with a number
			boost::none,
		},
	};
}();

bool object_typeinfo() {
	return run_ast_test(rule::object_typeinfo, object_typeinfo_input_set);
}

input_set_type<ast::vector_typeinfo> vector_typeinfo_input_set = []() {
	return input_set_type<ast::vector_typeinfo>{
		{
			"[string]",
			ast::vector_typeinfo{ast::type_tag<std::string>{}},
		},
		{
			"[def]",
			ast::vector_typeinfo{ast::typeref{"def", boost::none}},
		},
		{
			"[someType]",
			ast::vector_typeinfo{ast::typeref{"someType", boost::none}},
		},
		{
			"[1someType]", // identifiers cannot start with a number
			boost::none,
		},
		{
			"[uint]",
			ast::vector_typeinfo{ast::type_tag<uint32_t>{}},
		},
		{
			"[uint32]",
			ast::vector_typeinfo{ast::type_tag<uint32_t>{}},
		},
		{
			"[int]",
			ast::vector_typeinfo{ast::type_tag<int32_t>{}},
		},
		{
			"[int32]",
			ast::vector_typeinfo{ast::type_tag<int32_t>{}},
		},
		{
			"[uint64]",
			ast::vector_typeinfo{ast::type_tag<uint64_t>{}},
		},
		{
			"[ulong]",
			ast::vector_typeinfo{ast::type_tag<uint64_t>{}},
		},
		{
			"[int64]",
			ast::vector_typeinfo{ast::type_tag<int64_t>{}},
		},
		{
			"[long]",
			ast::vector_typeinfo{ast::type_tag<int64_t>{}},
		},
		{
			"[uint16]",
			ast::vector_typeinfo{ast::type_tag<uint16_t>{}},
		},
		{
			"[ushort]",
			ast::vector_typeinfo{ast::type_tag<uint16_t>{}},
		},
		{
			"[int16]",
			ast::vector_typeinfo{ast::type_tag<int16_t>{}},
		},
		{
			"[short]",
			ast::vector_typeinfo{ast::type_tag<int16_t>{}},
		},
		{
			"[uint8]",
			ast::vector_typeinfo{ast::type_tag<uint8_t>{}},
		},
		{
			"[ubyte]",
			ast::vector_typeinfo{ast::type_tag<uint8_t>{}},
		},
		{
			"[int8]",
			ast::vector_typeinfo{ast::type_tag<int8_t>{}},
		},
		{
			"[byte]",
			ast::vector_typeinfo{ast::type_tag<int8_t>{}},
		},
		{
			"[bool]",
			ast::vector_typeinfo{ast::type_tag<bool>{}},
		},
	};
}();

bool vector_typeinfo() {
	return run_ast_test(rule::vector_typeinfo, vector_typeinfo_input_set);
}

input_set_type<ast::table_field_decl> table_field_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::table_field_decl>{
		{
			"a : int = 10;",
			ast::table_field_decl{
				"a",
				ast::table_fieldtype_variant{
					ast::scalar_typeinfo{
						ast::scalar_type<int32_t>{10}}},
				boost::none,
			},
		},
		{
			"Names: [string] (required);",
			ast::table_field_decl{
				"Names",
				ast::table_fieldtype_variant{
					ast::vector_typeinfo{
						ast::type_tag<std::string>{}}},
				ast::metadata{
					{ "required", boost::none }
				},
			},
		},
	};
}();

bool table_field_decl() {
	return run_ast_test(rule::table_field_decl, table_field_decl_input_set);
}

input_set_type<ast::table_decl> table_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::table_decl>{
		{
			"table Object (id: 10) {"                    "\n"
			"  name : string (required, priority: 1);"   "\n"
			"  points : [Point];"                        "\n"
			"  shape : Shape = Rectangle (required);"    "\n"
			"}",
			ast::table_decl{
				"Object",
				ast::metadata{
					{ "id", ast::single_value(ast::scalar(uint64_t(10))) },
				},
				{
					ast::table_field_decl{
						"name",
						ast::table_fieldtype_variant{
							ast::object_typeinfo{
								ast::type_tag<std::string>{}},
						},
						ast::metadata{
							{ "required", boost::none },
							{ "priority", ast::single_value(ast::scalar(uint64_t(1))) },
						},
					},
					ast::table_field_decl{
						"points",
						ast::table_fieldtype_variant{
							ast::vector_typeinfo{
							ast::typeref{ "Point", boost::none }},
						},
						boost::none,
					},
					ast::table_field_decl{
						"shape",
						ast::table_fieldtype_variant{
							ast::object_typeinfo{
								ast::typeref{ "Shape", "Rectangle"s }},
						},
						ast::metadata{ { "required", boost::none } },
					},
				},
			},
		},
	};
}();

bool table_decl() {
	return run_ast_test(rule::table_decl, table_decl_input_set);
}

input_set_type<ast::struct_field_decl> struct_field_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::struct_field_decl>{
		{
			"a : int = 10;",
			boost::none, // struct fields cannot have defaults
		},
		{
			"a : int;",
			ast::struct_field_decl{
				"a",
				ast::struct_fieldtype_variant{
					ast::type_tag<int>{}},
				boost::none,
			},
		},
		{
			"s : string;",
			boost::none, // string should not be allowed as type
		},
		{
			"s : string_;",
			ast::struct_field_decl{
				"s",
				ast::struct_fieldtype_variant{
					ast::typeref{"string_", boost::none}},
				boost::none,
			}, // string_ could technically be another struct or an enum
		},
	};
}();

bool struct_field_decl() {
	return run_ast_test(rule::struct_field_decl, struct_field_decl_input_set);
}

input_set_type<ast::struct_decl> struct_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::struct_decl>{
		{
			"struct SomeStruct (id: 10) {"         "\n"
			"  x : int (priority: 1);"             "\n"
			"  Y : float;"                         "\n"
			"  shape : Shape;"                     "\n"
			"}",
			ast::struct_decl{
				"SomeStruct",
				ast::metadata{
					{ "id", ast::single_value(ast::scalar(uint64_t(10))) },
				},
				{
					ast::struct_field_decl{
						"x",
						ast::struct_fieldtype_variant{
							ast::type_tag<int>{}},
						ast::metadata{
							{ "priority", ast::single_value(ast::scalar(uint64_t(1))) },
						},
					},
					ast::struct_field_decl{
						"Y",
						ast::struct_fieldtype_variant{
							ast::type_tag<float>{}},
						boost::none,
					},
					ast::struct_field_decl{
						"shape",
						ast::struct_fieldtype_variant{
							ast::typeref{ "Shape", boost::none }
						},
						boost::none,
					},
				},
			},
		},
	};
}();

bool struct_decl() {
	return run_ast_test(rule::struct_decl, struct_decl_input_set);
}

input_set_type<ast::enumval_decl> enumval_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::enumval_decl>{
		{ "Red = 1", ast::enumval_decl{ "Red", ast::enumval_variant(uint64_t(1)) }},
		{ "Blue", ast::enumval_decl{ "Blue", boost::none }},
		{ "Yellow = -3", ast::enumval_decl{ "Yellow", ast::enumval_variant(int64_t(-3)) }},
		{ "Yellow = -3 (deprecated)", boost::none }, // no metadata allowed for enum values
	};
}();

bool enumval_decl() {
	return run_ast_test(rule::enumval_decl, enumval_decl_input_set);
}

input_set_type<ast::enum_decl> enum_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::enum_decl>{
		{
			"enum Colors : int (deprecated) {"    "\n"
			"  Red = 1,"                    "\n"
			"  Blue = 0xab,"                "\n"
			"  Yellow = -5,"                "\n"
			"  Green"                       "\n"
			"}",
			ast::enum_decl{
				"Colors",
				ast::enum_type_variant{ast::type_tag<int>{}},
				ast::metadata{{"deprecated", boost::none}},
				{
					ast::enumval_decl{"Red", ast::enumval_variant(uint64_t(1))},
					ast::enumval_decl{"Blue", ast::enumval_variant(uint64_t(0xab))},
					ast::enumval_decl{"Yellow", ast::enumval_variant(int64_t(-5))},
					ast::enumval_decl{"Green", boost::none},
				},
			},
		},
		{
			"enum Colors (deprecated) {"    "\n"
			"  Red = 1,"                    "\n"
			"  Blue = 0xab,"                "\n"
			"  Yellow = -5,"                "\n"
			"  Green"                       "\n"
			"}",
			boost::none // enum type information (e.g. (: int) is required
		},
	};
}();

bool enum_decl() {
	return run_ast_test(rule::enum_decl, enum_decl_input_set);
}

input_set_type<ast::union_subtype_decl> union_subtype_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::union_subtype_decl>{
		{ "SomeAlias: SomeType", ast::union_subtype_decl{"SomeType", "SomeAlias"s}},
		{ "SomeType", ast::union_subtype_decl{"SomeType", boost::none}},
		{ "SomeAlias: SomeType = SomeValue", boost::none }, // union subtypes cannot default to a value
		{ "SomeAlias: SomeType (deprecated)", boost::none }, // union subtypes are not allowed metadata
	};
}();

bool union_subtype_decl() {
	return run_ast_test(rule::union_subtype_decl, union_subtype_decl_input_set);
}

input_set_type<ast::union_decl> union_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::union_decl>{
		{
			"union U (priority: 1, deprecated) {"    "\n"
			"  Start: MarkerPosition,"               "\n"
			"  Point: PointPosition,"                "\n"
			"  Direction,"                           "\n"
			"  Finish: MarkerPosition"               "\n"
			"}",
			ast::union_decl{
				"U",
				ast::metadata{
					{ "priority", ast::single_value(ast::scalar(uint64_t(1))) },
					{ "deprecated", boost::none },
				},
				{
					ast::union_subtype_decl{"MarkerPosition", "Start"s},
					ast::union_subtype_decl{"PointPosition", "Point"s},
					ast::union_subtype_decl{"Direction", boost::none},
					ast::union_subtype_decl{"MarkerPosition", "Finish"s},
				},
			},
		},
	};
}();

bool union_decl() {
	return run_ast_test(rule::union_decl, union_decl_input_set);
}

input_set_type<ast::include> include_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::include>{
		{ "include \"schema.fbs\";", ast::include{"schema.fbs"}},
		{ "include schema.fbs;", boost::none },
		{ "include \"\";", ast::include{""}}, // empty include file is not filtered out at parser level
	};
}();

bool include() {
	return run_ast_test(rule::include, include_input_set);
}

input_set_type<ast::file_identifier_decl> file_identifier_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::file_identifier_decl>{
		{ "file_identifier \"Zxvf\";", ast::file_identifier_decl{"Zxvf"}},
		{ "file_identifier \"Zxf\";", boost::none }, // file id must be exactly 4 characters
		{ "file_identifier \"Zxvfr\";", boost::none }, // file id must be exactly 4 characters
		{ "file_identifier \"Zx\\xacv\";", ast::file_identifier_decl{"Zx\xacv"} }, // hex bytes allowed
		{ "file_identifier \"\";", boost::none },
		{ "file_identifier \"T\";", boost::none },
		{ "file_identifier \"xU\";", boost::none },
	};
}();

bool file_identifier_decl() {
	return run_ast_test(rule::file_identifier_decl, file_identifier_decl_input_set);
}

} // namespace test

int main() {
	auto ok = true;
	ok = ok && test::string_constant();
	ok = ok && test::scalar();
	ok = ok && test::single_value();
	ok = ok && test::metadatum();
	ok = ok && test::metadata();
	ok = ok && test::scalar_typeinfo();
	ok = ok && test::object_typeinfo();
	ok = ok && test::vector_typeinfo();
	ok = ok && test::table_field_decl();
	ok = ok && test::table_decl();
	ok = ok && test::struct_field_decl();
	ok = ok && test::struct_decl();
	ok = ok && test::enumval_decl();
	ok = ok && test::enum_decl();
	ok = ok && test::union_subtype_decl();
	ok = ok && test::union_decl();
	ok = ok && test::include();
	ok = ok && test::file_identifier_decl();
	if (!ok) {
		std::cout << "TEST FAILED\n";
		return 1;
	} else {
		std::cout << "TEST OK\n";
		return 0;
	}
}
