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

std::string demangleSymbol(const std::string& mangled) {
	int status = 0;
	size_t length = 0;
	auto demangled = abi::__cxa_demangle(mangled.c_str(), nullptr, &length, &status);
	if (demangled) {
		std::string ret(demangled, length);
		free(demangled);
		// each compounding of parsers (by operator >, +, |, etc) adds to the parser type, whose C++ type id could grow
		// to hundreds of characters. Try to simplify this by trying to find a rule type that the parser type maps to.
		auto const rulePos = ret.rfind("boost::spirit::x3::rule");
		if (rulePos != std::string::npos) {
			// dump from the rule part.
			ret = ret.substr(rulePos + 15);
		}
		return ret;
	} else {
		// exception.which() may not be a mangled C++ type id, in which case we just return as-is.
		return mangled;
	}
}

} // namespace detail

struct ErrorHandler {
	template <typename Iterator, typename Exception, typename Context>
	x3::error_handler_result on_error(Iterator& first,
	                                  Iterator const& last,
	                                  Exception const& x,
	                                  Context const& context) {
		auto& errorHandler = x3::get<x3::error_handler_tag>(context).get();
		std::string message = "Error! Expecting: " + detail::demangleSymbol(x.which()) + " here:";
		errorHandler(x.where(), message);
		return x3::error_handler_result::fail;
	}
};

// require doubles to have a decimal point to prefer ints over doubles
x3::real_parser<double, x3::strict_real_policies<double>> const StrictDouble;
x3::real_parser<float, x3::strict_real_policies<float>> const StrictFloat;
x3::uint_parser<uint64_t, 16> const hex_uint64;

#ifdef DEFINE_RULE
#error "DEFINE_RULE macro clashes with existing definition"
#endif
#define DEFINE_RULE(NAME, RULE, RESOLVED_TYPE)                                                                         \
	struct NAME##Class;                                                                                                \
	[[maybe_unused]] x3::rule<NAME##Class, RESOLVED_TYPE> const NAME = #NAME;                                          \
	[[maybe_unused]] auto const NAME##_def = RULE;                                                                     \
	BOOST_SPIRIT_DEFINE(NAME);                                                                                         \
	struct NAME##Class : ErrorHandler, x3::annotate_on_success {}

// Identifiers: names of namespaces, attributes, types, and fields
DEFINE_RULE(Ident,
            (x3::ascii::alpha | x3::ascii::char_('_')) >> *(x3::ascii::alnum | x3::ascii::char_('_')),
            std::string);

// parses references to other types in field declarations (parsed as Identifiers) into ast::TypeRef
DEFINE_RULE(TypeRef, Ident, ast::TypeRef);

DEFINE_RULE(CharInQuotedString,
            ("\\n" > x3::attr('\n') | "\\t" > x3::attr('\t') | "\\r" > x3::attr('\r') | "\\b" > x3::attr('\b') |
             "\\f" > x3::attr('\f') |
             "\\x" > x3::uint_parser<uint8_t, 16, 2, 2>() // bytes in hex: e.g. "\xff"; always two digits
             | "\\" > x3::char_("\"\\/") // escaped double-quote, backslash, and slash
             | ~x3::char_('"')),
            char);

// quoted string constants with escapes. missing feature: "\uXXXX" which should parses into utf8
DEFINE_RULE(StringConstant, (x3::lexeme['"' > *(CharInQuotedString) > '"']), std::string);

DEFINE_RULE(FourQuotedStringChars, x3::repeat(4)[CharInQuotedString], std::string);

DEFINE_RULE(FileIdentifierConstant, (x3::lexeme['"' > FourQuotedStringChars > '"']), std::string);

// constants and Metadata
DEFINE_RULE(BooleanConstant, x3::bool_, bool);
DEFINE_RULE(UnsignedConstant, ((x3::lit("0x") > hex_uint64) | x3::uint64), uint64_t);
DEFINE_RULE(Scalar,
            BooleanConstant | StrictDouble | UnsignedConstant | x3::int64,
            // as the parser evaluates rules from left-to-right, strict double must come before ints
            // to prevent parsing e.g. "1.0" as an UnsignedConstant 1 and leaving ".0" behind.
            // UnsignedConstant must come before int64 to parse "3" as unsigned int and not int
            ast::Scalar);
DEFINE_RULE(SingleValue, Scalar | StringConstant, ast::SingleValue);
DEFINE_RULE(Metadatum, Ident > -(x3::lit(':') > SingleValue), ast::Metadatum);
DEFINE_RULE(Metadata, x3::lit('(') > (Metadatum % ',') > ')', ast::Metadata);

// includes, file Identifiers, namespaces, attributes, root
DEFINE_RULE(Include, x3::lit("include") > StringConstant > ';', ast::Include);
DEFINE_RULE(FileIdentifierDecl, x3::lit("file_identifier") > FileIdentifierConstant > ';', ast::FileIdentifierDecl);
DEFINE_RULE(FileExtensionDecl, x3::lit("file_extension") > StringConstant > ';', ast::FileExtensionDecl);
DEFINE_RULE(RootTypeDecl, x3::lit("root_type") > Ident > ';', ast::RootTypeDecl);
DEFINE_RULE(RpcMethodDecl, (Ident > '(' > Ident > ')' > ':' > Ident > -Metadata > ';'), ast::RpcMethodDecl);
DEFINE_RULE(RpcDecl, (x3::lit("rpc_service") > Ident > '{' > +RpcMethodDecl > '}'), ast::RpcDecl);
DEFINE_RULE(NamespacePath, (Ident % '.'), std::vector<std::string>);
DEFINE_RULE(NamespaceDecl, x3::lit("namespace") > NamespacePath > ';', ast::NamespaceDecl);
DEFINE_RULE(AttributeDecl,
            (x3::lit("attribute") > ((x3::lit('"') > Ident > x3::lit('"')) | Ident) > ';'),
            ast::AttributeDecl);

DEFINE_RULE(BoolFieldPart, x3::lit("bool") > -(x3::lit('=') > x3::bool_), ast::ScalarType<bool>);

DEFINE_RULE(Int8FieldPart, (x3::lit("byte") | "int8") > -(x3::lit('=') > x3::int8), ast::ScalarType<int8_t>);

DEFINE_RULE(Uint8FieldPart, (x3::lit("ubyte") | "uint8") > -(x3::lit('=') > x3::uint8), ast::ScalarType<uint8_t>);

DEFINE_RULE(Int16FieldPart, (x3::lit("short") | "int16") > -(x3::lit('=') > x3::int16), ast::ScalarType<int16_t>);

DEFINE_RULE(Uint16FieldPart, (x3::lit("ushort") | "uint16") > -(x3::lit('=') > x3::uint16), ast::ScalarType<uint16_t>);

DEFINE_RULE(Int32FieldPart, (x3::lit("int") | "int32") > -(x3::lit('=') > x3::int32), ast::ScalarType<int32_t>);

DEFINE_RULE(Uint32FieldPart, (x3::lit("uint") | "uint32") > -(x3::lit('=') > x3::uint32), ast::ScalarType<uint32_t>);

DEFINE_RULE(Int64FieldPart, (x3::lit("long") | "int64") > -(x3::lit('=') > x3::int64), ast::ScalarType<int64_t>);

DEFINE_RULE(Uint64FieldPart, (x3::lit("ulong") | "uint64") > -(x3::lit('=') > x3::uint64), ast::ScalarType<uint64_t>);

DEFINE_RULE(Float32FieldPart, (x3::lit("float") | "float32") > -(x3::lit('=') > StrictFloat), ast::ScalarType<float>);

DEFINE_RULE(Float64FieldPart,
            (x3::lit("double") | "float64") > -(x3::lit('=') > StrictDouble),
            ast::ScalarType<double>);

// [u]int32 must be placed at the last of the integer rules because of the alias "[u]int":
// e.g. parsing "uint64" may be parsed as "uint" and left partially parsed.
DEFINE_RULE(ScalarTypeInfo,
            BoolFieldPart | Int8FieldPart | Uint8FieldPart | Int16FieldPart | Uint16FieldPart | Int64FieldPart |
                Uint64FieldPart | Int32FieldPart | Uint32FieldPart | Float32FieldPart | Float64FieldPart,
            ast::ScalarTypeInfo);

auto const BoolTypePart = x3::lit("bool") > x3::attr(ast::TypeTag<bool>{});

auto const Int8TypePart = (x3::lit("byte") | "int8") > x3::attr(ast::TypeTag<int8_t>{});

auto const Uint8TypePart = (x3::lit("ubyte") | "uint8") > x3::attr(ast::TypeTag<uint8_t>{});

auto const Int16TypePart = (x3::lit("short") | "int16") > x3::attr(ast::TypeTag<int16_t>{});

auto const Uint16TypePart = (x3::lit("ushort") | "uint16") > x3::attr(ast::TypeTag<uint16_t>{});

auto const Int32TypePart = (x3::lit("int32") | "int") > x3::attr(ast::TypeTag<int32_t>{});

auto const Uint32TypePart = (x3::lit("uint32") | "uint") > x3::attr(ast::TypeTag<uint32_t>{});

auto const Int64TypePart = (x3::lit("long") | "int64") > x3::attr(ast::TypeTag<int64_t>{});

auto const Uint64TypePart = (x3::lit("ulong") | "uint64") > x3::attr(ast::TypeTag<uint64_t>{});

auto const Float32TypePart = (x3::lit("float") | "float32") > x3::attr(ast::TypeTag<float>{});

auto const Float64TypePart = (x3::lit("double") | "float64") > x3::attr(ast::TypeTag<double>{});

auto const StringTypePart = x3::lit("string") > x3::attr(ast::TypeTag<std::string>{});

DEFINE_RULE(TypeRefTypePart, Ident > x3::attr(boost::none), ast::TypeRef);
DEFINE_RULE(TypeRefFieldPart, Ident > -(x3::lit('=') > Ident), ast::TypeRef);
DEFINE_RULE(NoStringFieldInStruct, !(x3::lit("string") >> ';'), x3::unused_type);

// [u]int32 must be placed at the last of the integer rules because of the alias "[u]int":
// e.g. parsing "[uint64]" may be parsed as "[uint" and throw an error because there's '6' instead of ']'
DEFINE_RULE(VectorTypeInfo,
            (x3::lit('[') > (BoolTypePart | Int8TypePart | Uint8TypePart | Int16TypePart | Uint16TypePart |
                             Int64TypePart | Uint64TypePart | Int32TypePart | Uint32TypePart | Float32TypePart |
                             Float64TypePart | StringTypePart | TypeRefTypePart) > ']'),
            ast::VectorTypeInfo);

DEFINE_RULE(ObjectTypeInfo, (StringTypePart | TypeRefFieldPart), ast::ObjectTypeInfo);

DEFINE_RULE(TableFieldDecl,
            (Ident > ':' > (ScalarTypeInfo | ObjectTypeInfo | VectorTypeInfo) > -Metadata > ';'),
            ast::TableFieldDecl);

DEFINE_RULE(TableDecl, (x3::lit("table") > Ident > -Metadata > '{' > +TableFieldDecl > '}'), ast::TableDecl);

// Only Scalars or other structs allowed, no defaults
DEFINE_RULE(StructFieldDecl,
            (Ident > ':' > NoStringFieldInStruct >
             (BoolTypePart | Int8TypePart | Uint8TypePart | Int16TypePart | Uint16TypePart | Int64TypePart |
              Uint64TypePart | Int32TypePart | Uint32TypePart | Float32TypePart | Float64TypePart | TypeRefTypePart) >
             -Metadata > ';'),
            ast::StructFieldDecl);

DEFINE_RULE(StructDecl, (x3::lit("struct") > Ident > -Metadata > '{' > +StructFieldDecl > '}'), ast::StructDecl);

DEFINE_RULE(EnumValDecl, (Ident > -(x3::lit('=') > (UnsignedConstant | x3::int64))), ast::EnumValDecl);

DEFINE_RULE(EnumDecl,
            (x3::lit("enum") > Ident > ':' > (Int8TypePart | Uint8TypePart | Int16TypePart | Uint16TypePart |
                                              Int64TypePart | Uint64TypePart | Int32TypePart | Uint32TypePart) >
             -Metadata > '{' > (EnumValDecl % ',') > '}'),
            ast::EnumDecl);

DEFINE_RULE(UnionSubTypeDecl, -(Ident >> ':') > Ident, ast::UnionSubTypeDecl);

DEFINE_RULE(UnionDecl, (x3::lit("union") > Ident > -Metadata > '{' > (UnionSubTypeDecl % ',') > '}'), ast::UnionDecl);

DEFINE_RULE(Schema,
            *Include > *(NamespaceDecl | TableDecl | StructDecl | EnumDecl | UnionDecl | RootTypeDecl |
                         FileExtensionDecl | FileIdentifierDecl | AttributeDecl | RpcDecl),
            ast::Schema);

#undef DEFINE_RULE

} // namespace rule

#endif /*FLATBUFFERS_PARSER_DEFINITIONS*/
