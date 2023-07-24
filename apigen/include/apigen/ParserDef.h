#ifndef FLATBUFFERS_PARSER_DEFINITIONS
#define FLATBUFFERS_PARSER_DEFINITIONS

#include <boost/spirit/home/x3.hpp>
#include <boost/spirit/home/x3/numeric/real.hpp>
#include <boost/spirit/home/x3/support/utility/annotate_on_success.hpp>
#include <boost/spirit/home/x3/support/utility/error_reporting.hpp>
#include <boost/fusion/adapted/std_pair.hpp>
#include "apigen/AST.h"
#include "apigen/AdaptedAST.h"
#include <string>

namespace rule {

namespace x3 = boost::spirit::x3;

namespace detail {

std::string demangle_symbol(const std::string& mangled) {
	int status = 0;
	size_t length = 0;
	auto demangled = abi::__cxa_demangle(mangled.c_str(), nullptr, &length, &status);
	if (demangled) {
		std::string ret(demangled, length);
		free(demangled);
		// each compounding of parsers (by operator >, +, |, etc) adds to the parser type, whose C++ type id could grow
		// to hundreds of characters. Try to simplify this by trying to find a rule type that the parser type maps to.
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

struct error_handler {
	template <typename Iterator, typename Exception, typename Context>
	x3::error_handler_result on_error(Iterator& first,
	                                  Iterator const& last,
	                                  Exception const& x,
	                                  Context const& context) {
		auto& error_handler = x3::get<x3::error_handler_tag>(context).get();
		std::string message = "Error! Expecting: " + detail::demangle_symbol(x.which()) + " here:";
		error_handler(x.where(), message);
		return x3::error_handler_result::fail;
	}
};

// require doubles to have a decimal point to prefer ints over doubles
x3::real_parser<double, x3::strict_real_policies<double>> const strict_double;
x3::real_parser<float, x3::strict_real_policies<float>> const strict_float;
x3::uint_parser<uint64_t, 16> const hex_uint64;

#ifdef DEFINE_RULE
#error "DEFINE_RULE macro clashes with existing definition"
#endif
#define DEFINE_RULE(NAME, RULE, RESOLVED_TYPE)                                                                         \
	struct NAME##_class;                                                                                               \
	[[maybe_unused]] x3::rule<NAME##_class, RESOLVED_TYPE> const NAME = #NAME;                                         \
	[[maybe_unused]] auto const NAME##_def = RULE;                                                                     \
	BOOST_SPIRIT_DEFINE(NAME);                                                                                         \
	struct NAME##_class : error_handler, x3::annotate_on_success {}

// identifiers: names of namespaces, attributes, types, and fields
DEFINE_RULE(ident,
            (x3::ascii::alpha | x3::ascii::char_('_')) >> *(x3::ascii::alnum | x3::ascii::char_('_')),
            std::string);

// parses references to other types in field declarations (parsed as identifiers) into ast::typeref
DEFINE_RULE(typeref, ident, ast::typeref);

DEFINE_RULE(char_in_quoted_string,
            ("\\n" > x3::attr('\n') | "\\t" > x3::attr('\t') | "\\r" > x3::attr('\r') | "\\b" > x3::attr('\b') |
             "\\f" > x3::attr('\f') |
             "\\x" > x3::uint_parser<uint8_t, 16, 2, 2>() // bytes in hex: e.g. "\xff"; always two digits
             | "\\" > x3::char_("\"\\/") // escaped double-quote, backslash, and slash
             | ~x3::char_('"')),
            char);

// quoted string constants with escapes. missing feature: "\uXXXX" which should parses into utf8
DEFINE_RULE(string_constant, (x3::lexeme['"' > *(char_in_quoted_string) > '"']), std::string);

DEFINE_RULE(four_quoted_string_chars, x3::repeat(4)[char_in_quoted_string], std::string);

DEFINE_RULE(file_identifier_constant, (x3::lexeme['"' > four_quoted_string_chars > '"']), std::string);

// constants and metadata
DEFINE_RULE(boolean_constant, x3::bool_, bool);
DEFINE_RULE(unsigned_constant, ((x3::lit("0x") > hex_uint64) | x3::uint64), uint64_t);
DEFINE_RULE(scalar,
            boolean_constant | strict_double | unsigned_constant | x3::int64,
            // as the parser evaluates rules from left-to-right, strict double must come before ints
            // to prevent parsing e.g. "1.0" as an unsigned_constant 1 and leaving ".0" behind.
            // unsigned_constant must come before int64 to parse "3" as unsigned int and not int
            ast::scalar);
DEFINE_RULE(single_value, scalar | string_constant, ast::single_value);
DEFINE_RULE(metadatum, ident > -(x3::lit(':') > single_value), ast::metadatum);
DEFINE_RULE(metadata, x3::lit('(') > (metadatum % ',') > ')', ast::metadata);

// includes, file identifiers, namespaces, attributes, root
DEFINE_RULE(include, x3::lit("include") > string_constant > ';', ast::include);
DEFINE_RULE(file_identifier_decl,
            x3::lit("file_identifier") > file_identifier_constant > ';',
            ast::file_identifier_decl);
DEFINE_RULE(file_extension_decl, x3::lit("file_extension") > string_constant > ';', ast::file_extension_decl);
DEFINE_RULE(root_type_decl, x3::lit("root_type") > ident > ';', ast::root_type_decl);
DEFINE_RULE(rpc_method_decl, (ident > '(' > ident > ')' > ':' > ident > -metadata > ';'), ast::rpc_method_decl);
DEFINE_RULE(rpc_decl, (x3::lit("rpc_service") > ident > '{' > +rpc_method_decl > '}'), ast::rpc_decl);
DEFINE_RULE(namespace_path, (ident % '.'), std::vector<std::string>);
DEFINE_RULE(namespace_decl, x3::lit("namespace") > namespace_path > ';', ast::namespace_decl);
DEFINE_RULE(attribute_decl,
            (x3::lit("attribute") > ((x3::lit('"') > ident > x3::lit('"')) | ident) > ';'),
            ast::attribute_decl);

DEFINE_RULE(bool_field_part, x3::lit("bool") > -(x3::lit('=') > x3::bool_), ast::scalar_type<bool>);

DEFINE_RULE(int8_field_part, (x3::lit("byte") | "int8") > -(x3::lit('=') > x3::int8), ast::scalar_type<int8_t>);

DEFINE_RULE(uint8_field_part, (x3::lit("ubyte") | "uint8") > -(x3::lit('=') > x3::uint8), ast::scalar_type<uint8_t>);

DEFINE_RULE(int16_field_part, (x3::lit("short") | "int16") > -(x3::lit('=') > x3::int16), ast::scalar_type<int16_t>);

DEFINE_RULE(uint16_field_part,
            (x3::lit("ushort") | "uint16") > -(x3::lit('=') > x3::uint16),
            ast::scalar_type<uint16_t>);

DEFINE_RULE(int32_field_part, (x3::lit("int") | "int32") > -(x3::lit('=') > x3::int32), ast::scalar_type<int32_t>);

DEFINE_RULE(uint32_field_part, (x3::lit("uint") | "uint32") > -(x3::lit('=') > x3::uint32), ast::scalar_type<uint32_t>);

DEFINE_RULE(int64_field_part, (x3::lit("long") | "int64") > -(x3::lit('=') > x3::int64), ast::scalar_type<int64_t>);

DEFINE_RULE(uint64_field_part,
            (x3::lit("ulong") | "uint64") > -(x3::lit('=') > x3::uint64),
            ast::scalar_type<uint64_t>);

DEFINE_RULE(float32_field_part,
            (x3::lit("float") | "float32") > -(x3::lit('=') > strict_float),
            ast::scalar_type<float>);

DEFINE_RULE(float64_field_part,
            (x3::lit("double") | "float64") > -(x3::lit('=') > strict_double),
            ast::scalar_type<double>);

// [u]int32 must be placed at the last of the integer rules because of the alias "[u]int":
// e.g. parsing "uint64" may be parsed as "uint" and left partially parsed.
DEFINE_RULE(scalar_typeinfo,
            bool_field_part | int8_field_part | uint8_field_part | int16_field_part | uint16_field_part |
                int64_field_part | uint64_field_part | int32_field_part | uint32_field_part | float32_field_part |
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
DEFINE_RULE(vector_typeinfo,
            (x3::lit('[') > (bool_type_part | int8_type_part | uint8_type_part | int16_type_part | uint16_type_part |
                             int64_type_part | uint64_type_part | int32_type_part | uint32_type_part |
                             float32_type_part | float64_type_part | string_type_part | typeref_type_part) > ']'),
            ast::vector_typeinfo);

DEFINE_RULE(object_typeinfo, (string_type_part | typeref_field_part), ast::object_typeinfo);

DEFINE_RULE(table_field_decl,
            (ident > ':' > (scalar_typeinfo | object_typeinfo | vector_typeinfo) > -metadata > ';'),
            ast::table_field_decl);

DEFINE_RULE(table_decl, (x3::lit("table") > ident > -metadata > '{' > +table_field_decl > '}'), ast::table_decl);

// Only scalars or other structs allowed, no defaults
DEFINE_RULE(struct_field_decl,
            (ident > ':' > no_string_field_in_struct >
             (bool_type_part | int8_type_part | uint8_type_part | int16_type_part | uint16_type_part | int64_type_part |
              uint64_type_part | int32_type_part | uint32_type_part | float32_type_part | float64_type_part |
              typeref_type_part) > -metadata > ';'),
            ast::struct_field_decl);

DEFINE_RULE(struct_decl, (x3::lit("struct") > ident > -metadata > '{' > +struct_field_decl > '}'), ast::struct_decl);

DEFINE_RULE(enumval_decl, (ident > -(x3::lit('=') > (unsigned_constant | x3::int64))), ast::enumval_decl);

DEFINE_RULE(enum_decl,
            (x3::lit("enum") > ident > ':' > (int8_type_part | uint8_type_part | int16_type_part | uint16_type_part |
                                              int64_type_part | uint64_type_part | int32_type_part | uint32_type_part) >
             -metadata > '{' > (enumval_decl % ',') > '}'),
            ast::enum_decl);

DEFINE_RULE(union_subtype_decl, -(ident >> ':') > ident, ast::union_subtype_decl);

DEFINE_RULE(union_decl,
            (x3::lit("union") > ident > -metadata > '{' > (union_subtype_decl % ',') > '}'),
            ast::union_decl);

DEFINE_RULE(schema,
            *include > *(namespace_decl | table_decl | struct_decl | enum_decl | union_decl | root_type_decl |
                         file_extension_decl | file_identifier_decl | attribute_decl | rpc_decl),
            ast::schema);

#undef DEFINE_RULE

} // namespace rule

#endif /*FLATBUFFERS_PARSER_DEFINITIONS*/
