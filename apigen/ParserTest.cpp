#include <boost/spirit/home/x3.hpp>
#include <boost/spirit/home/x3/support/utility/error_reporting.hpp>
#include "apigen/AST.h"
#include "apigen/AdaptedAST.h"
#include "apigen/ParserDef.h"
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
#include <cxxabi.h>

namespace x3 = boost::spirit::x3;

namespace test {

// default optional indicates that we expect parse to fail (i.e. a parse error or a partial match)
template <class AST>
using Case = std::pair<std::string, boost::optional<AST>>;

template <class AST>
using InputSet = std::vector<Case<AST>>;

template <class Rule, class AST, class EqualsFn>
bool runAstTest(const Rule& r, const InputSet<AST>& cases, EqualsFn&& equalsFn) {
	std::cout << "Testing " << r.name << " parser...\n";
	unsigned failures = 0;
	for (const auto& [input, expected] : cases) {
		auto bg = input.begin();
		auto ed = input.end();
		auto output = AST();
		using ErrorHandler = x3::error_handler<decltype(bg)>;
		ErrorHandler errorHandler(bg, ed, std::cerr);
		auto const parser = x3::with<x3::error_handler_tag>(std::ref(errorHandler))[r];
		auto const parseOk = x3::phrase_parse(bg, ed, parser, x3::space, output);
		auto const parseComplete = (bg == ed);
		auto testOk = true;
		auto valueMatch = true;
		if (!expected) {
			// parse should fail
			testOk = !parseOk || !parseComplete;
		} else {
			if (parseOk && parseComplete)
				valueMatch = equalsFn(*expected, output);
			testOk = parseOk && parseComplete && valueMatch;
		}
		std::cout << "  " << (testOk ? "O " : "X ");
		if (!testOk) {
			if (!expected) {
				std::cout << "[parse_unexpectedly_succeeded] ";
			} else {
				if (!parseOk)
					std::cout << "[parse_error] ";
				if (!parseComplete)
					std::cout << "[parse_incomplete_at_pos_" << (bg - input.begin()) << "_of_length_" << input.size()
					          << "] ";
				if (!valueMatch)
					std::cout << "[value_mismatch] ";
			}
		} else if (!expected) {
			std::cout << "[parse_failed_as_expected] ";
		}
		std::cout << std::quoted(input) << '\n';
		failures += !testOk;
	}
	if (failures)
		std::cout << failures << " cases failed\n";
	return failures == 0;
}

template <class Rule, class AST>
bool runAstTest(const Rule& r, const InputSet<AST>& cases) {
	return runAstTest(r, cases, [](const AST& lhs, const AST& rhs) { return lhs == rhs; });
}

InputSet<std::string> const StringConstantInputSet = []() -> InputSet<std::string> {
	using namespace std::string_literals;
	return InputSet<std::string>{
		{ "\"\\n\"", "\n"s }, { "\"\\t\"", "\t"s },  { "\"\\r\"", "\r"s },
		{ "\"\\b\"", "\b"s }, { "\"\\f\"", "\f"s },  { "\"\\\\\"", "\\"s },
		{ "\"\\/\"", "/"s },  { "\"\\\"\"", "\""s }, { "\"\\x13\\xdc\"", "\x13\xdc"s },
		{ "\"\\x1\"", {} }, // hex characters must contain two digits
	};
}();

bool stringConstant() {
	return runAstTest(rule::string_constant, StringConstantInputSet);
}

InputSet<ast::scalar> const scalarInputSet = {
	{ "1.0", ast::scalar(1.0) }, // double
	{ "10", ast::scalar(uint64_t(10)) }, // uint64
	{ "-10.0", ast::scalar(-10.0) }, // double
	{ "-10", ast::scalar(int64_t(-10)) }, // int64
	{ "-3", ast::scalar(int64_t(-3)) }, // int64
	{ "0xfe", ast::scalar(uint64_t(0xfe)) }, // uint64
	{ "0xDeadBeefDeadBeef", ast::scalar(uint64_t(0xdeadbeefdeadbeefUL)) }, // uint64
	{ "0xdeadbeefdeadbeef", ast::scalar(uint64_t(0xdeadbeefdeadbeefUL)) }, // uint64
	{ "-0xdeadbeefdeadbeef",
	  boost::none }, // negative hex int64 not supported yet TODO: make this throw a parse error with message
	{ "true", ast::scalar(true) }, // bool
	{ "false", ast::scalar(false) }, // bool
	{ "1", ast::scalar(uint64_t(1)) }, // uint64
	{ "0", ast::scalar(uint64_t(0)) }, // uint64
};

bool scalar() {
	return runAstTest(rule::scalar, scalarInputSet);
}

// reuse scalar/string_constant test input for single value test
InputSet<ast::single_value> const singleValueInputSet = []() -> InputSet<ast::single_value> {
	auto ret = InputSet<ast::single_value>();
	for (auto const& [input, expected] : StringConstantInputSet) {
		if (expected) {
			ret.emplace_back(input, *expected);
		} else {
			ret.emplace_back(input, boost::none);
		}
	}
	for (auto const& [input, expected] : scalarInputSet) {
		if (expected) {
			ret.emplace_back(input, *expected);
		} else {
			ret.emplace_back(input, boost::none);
		}
	}
	return ret;
}();

bool singleValue() {
	return runAstTest(rule::single_value, singleValueInputSet);
}

InputSet<ast::metadatum> const metadatumInputSet = []() {
	using namespace std::string_literals;
	auto ret = InputSet<ast::metadatum>{
		{ "attr", ast::metadatum{ "attr", boost::none } },
		{ "abc: \"def\"", ast::metadatum{ "abc", ast::single_value("def"s) } },
		{ "id: 42", ast::metadatum{ "id", ast::single_value(ast::scalar(uint64_t(42))) } },
		{ "deprecated", ast::metadatum{ "deprecated", boost::none } },
		{ "required", ast::metadatum{ "required", boost::none } },
		{ "force_align: 16", ast::metadatum{ "force_align", ast::single_value(ast::scalar(uint64_t(16))) } },
		{ "nested_flatbuffer: \"someTable\"", ast::metadatum{ "nested_flatbuffer", ast::single_value("someTable"s) } },
	};
	return ret;
}();

bool metadatum() {
	return runAstTest(rule::metadatum, metadatumInputSet);
}

InputSet<ast::metadata> const metadataInputSet = []() {
	using namespace std::string_literals;
	InputSet<ast::metadata> ret = {
		{
		    "(a: 10, b: \"c\", d, e: true)"s,
		    ast::metadata{ { "a", ast::single_value(ast::scalar(uint64_t(10))) },
		                   { "b", ast::single_value("c"s) },
		                   { "d", boost::none },
		                   { "e", ast::single_value(ast::scalar(true)) } },
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
	return runAstTest(rule::metadata, metadataInputSet);
}

InputSet<ast::scalar_typeinfo> scalarTypeInfoInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::scalar_typeinfo>{
		{
		    "int = 10",
		    ast::scalar_typeinfo{ ast::scalar_type<int32_t>{ 10 } },
		},
		{
		    "bool",
		    ast::scalar_typeinfo{ ast::scalar_type<bool>{ boost::none } },
		},
		{
		    "bool = true",
		    ast::scalar_typeinfo{ ast::scalar_type<bool>{ true } },
		},
		{
		    "ulong",
		    ast::scalar_typeinfo{ ast::scalar_type<uint64_t>{ boost::none } },
		},
		{
		    "int64 = -5",
		    ast::scalar_typeinfo{ ast::scalar_type<int64_t>{ -5 } },
		},
		{
		    "double = -3.14",
		    ast::scalar_typeinfo{ ast::scalar_type<double>{ -3.14 } },
		},
		{
		    "double",
		    ast::scalar_typeinfo{ ast::scalar_type<double>{ boost::none } },
		},
	};
}();

bool scalarTypeInfo() {
	return runAstTest(rule::scalar_typeinfo, scalarTypeInfoInputSet);
}

InputSet<ast::object_typeinfo> objectTypeInfoInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::object_typeinfo>{
		{
		    "string",
		    ast::object_typeinfo{ ast::type_tag<std::string>{} },
		},
		{
		    "def",
		    ast::object_typeinfo{ ast::typeref{ "def", boost::none } },
		},
		{
		    "someType",
		    ast::object_typeinfo{ ast::typeref{ "someType", boost::none } },
		},
		{
		    "1someType", // identifiers cannot start with a number
		    boost::none,
		},
	};
}();

bool objectTypeInfo() {
	return runAstTest(rule::object_typeinfo, objectTypeInfoInputSet);
}

InputSet<ast::vector_typeinfo> vectorTypeInfoInputSet = []() {
	return InputSet<ast::vector_typeinfo>{
		{
		    "[string]",
		    ast::vector_typeinfo{ ast::type_tag<std::string>{} },
		},
		{
		    "[def]",
		    ast::vector_typeinfo{ ast::typeref{ "def", boost::none } },
		},
		{
		    "[someType]",
		    ast::vector_typeinfo{ ast::typeref{ "someType", boost::none } },
		},
		{
		    "[1someType]", // identifiers cannot start with a number
		    boost::none,
		},
		{
		    "[uint]",
		    ast::vector_typeinfo{ ast::type_tag<uint32_t>{} },
		},
		{
		    "[uint32]",
		    ast::vector_typeinfo{ ast::type_tag<uint32_t>{} },
		},
		{
		    "[int]",
		    ast::vector_typeinfo{ ast::type_tag<int32_t>{} },
		},
		{
		    "[int32]",
		    ast::vector_typeinfo{ ast::type_tag<int32_t>{} },
		},
		{
		    "[uint64]",
		    ast::vector_typeinfo{ ast::type_tag<uint64_t>{} },
		},
		{
		    "[ulong]",
		    ast::vector_typeinfo{ ast::type_tag<uint64_t>{} },
		},
		{
		    "[int64]",
		    ast::vector_typeinfo{ ast::type_tag<int64_t>{} },
		},
		{
		    "[long]",
		    ast::vector_typeinfo{ ast::type_tag<int64_t>{} },
		},
		{
		    "[uint16]",
		    ast::vector_typeinfo{ ast::type_tag<uint16_t>{} },
		},
		{
		    "[ushort]",
		    ast::vector_typeinfo{ ast::type_tag<uint16_t>{} },
		},
		{
		    "[int16]",
		    ast::vector_typeinfo{ ast::type_tag<int16_t>{} },
		},
		{
		    "[short]",
		    ast::vector_typeinfo{ ast::type_tag<int16_t>{} },
		},
		{
		    "[uint8]",
		    ast::vector_typeinfo{ ast::type_tag<uint8_t>{} },
		},
		{
		    "[ubyte]",
		    ast::vector_typeinfo{ ast::type_tag<uint8_t>{} },
		},
		{
		    "[int8]",
		    ast::vector_typeinfo{ ast::type_tag<int8_t>{} },
		},
		{
		    "[byte]",
		    ast::vector_typeinfo{ ast::type_tag<int8_t>{} },
		},
		{
		    "[bool]",
		    ast::vector_typeinfo{ ast::type_tag<bool>{} },
		},
	};
}();

bool vectorTypeInfo() {
	return runAstTest(rule::vector_typeinfo, vectorTypeInfoInputSet);
}

InputSet<ast::table_field_decl> tableFieldDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::table_field_decl>{
		{
		    "a : int = 10;",
		    ast::table_field_decl{
		        "a",
		        ast::table_fieldtype_variant{ ast::scalar_typeinfo{ ast::scalar_type<int32_t>{ 10 } } },
		        boost::none,
		    },
		},
		{
		    "Names: [string] (required);",
		    ast::table_field_decl{
		        "Names",
		        ast::table_fieldtype_variant{ ast::vector_typeinfo{ ast::type_tag<std::string>{} } },
		        ast::metadata{ { "required", boost::none } },
		    },
		},
	};
}();

bool tableFieldDecl() {
	return runAstTest(rule::table_field_decl, tableFieldDeclInputSet);
}

InputSet<ast::table_decl> tableDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::table_decl>{
		{
		    "table Object (id: 10) {"
		    "\n"
		    "  name : string (required, priority: 1);"
		    "\n"
		    "  points : [Point];"
		    "\n"
		    "  shape : Shape = Rectangle (required);"
		    "\n"
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
		                    ast::object_typeinfo{ ast::type_tag<std::string>{} },
		                },
		                ast::metadata{
		                    { "required", boost::none },
		                    { "priority", ast::single_value(ast::scalar(uint64_t(1))) },
		                },
		            },
		            ast::table_field_decl{
		                "points",
		                ast::table_fieldtype_variant{
		                    ast::vector_typeinfo{ ast::typeref{ "Point", boost::none } },
		                },
		                boost::none,
		            },
		            ast::table_field_decl{
		                "shape",
		                ast::table_fieldtype_variant{
		                    ast::object_typeinfo{ ast::typeref{ "Shape", "Rectangle"s } },
		                },
		                ast::metadata{ { "required", boost::none } },
		            },
		        },
		    },
		},
	};
}();

bool tableDecl() {
	return runAstTest(rule::table_decl, tableDeclInputSet);
}

InputSet<ast::struct_field_decl> structFieldDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::struct_field_decl>{
		{
		    "a : int = 10;",
		    boost::none, // struct fields cannot have defaults
		},
		{
		    "a : int;",
		    ast::struct_field_decl{
		        "a",
		        ast::struct_fieldtype_variant{ ast::type_tag<int>{} },
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
		        ast::struct_fieldtype_variant{ ast::typeref{ "string_", boost::none } },
		        boost::none,
		    }, // string_ could technically be another struct or an enum
		},
	};
}();

bool structFieldDecl() {
	return runAstTest(rule::struct_field_decl, structFieldDeclInputSet);
}

InputSet<ast::struct_decl> structDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::struct_decl>{
		{
		    "struct SomeStruct (id: 10) {"
		    "\n"
		    "  x : int (priority: 1);"
		    "\n"
		    "  Y : float;"
		    "\n"
		    "  shape : Shape;"
		    "\n"
		    "}",
		    ast::struct_decl{
		        "SomeStruct",
		        ast::metadata{
		            { "id", ast::single_value(ast::scalar(uint64_t(10))) },
		        },
		        {
		            ast::struct_field_decl{
		                "x",
		                ast::struct_fieldtype_variant{ ast::type_tag<int>{} },
		                ast::metadata{
		                    { "priority", ast::single_value(ast::scalar(uint64_t(1))) },
		                },
		            },
		            ast::struct_field_decl{
		                "Y",
		                ast::struct_fieldtype_variant{ ast::type_tag<float>{} },
		                boost::none,
		            },
		            ast::struct_field_decl{
		                "shape",
		                ast::struct_fieldtype_variant{ ast::typeref{ "Shape", boost::none } },
		                boost::none,
		            },
		        },
		    },
		},
	};
}();

bool structDecl() {
	return runAstTest(rule::struct_decl, structDeclInputSet);
}

InputSet<ast::enumval_decl> enumValDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::enumval_decl>{
		{ "Red = 1", ast::enumval_decl{ "Red", ast::enumval_variant(uint64_t(1)) } },
		{ "Blue", ast::enumval_decl{ "Blue", boost::none } },
		{ "Yellow = -3", ast::enumval_decl{ "Yellow", ast::enumval_variant(int64_t(-3)) } },
		{ "Yellow = -3 (deprecated)", boost::none }, // no metadata allowed for enum values
	};
}();

bool enumValDecl() {
	return runAstTest(rule::enumval_decl, enumValDeclInputSet);
}

InputSet<ast::enum_decl> enumDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::enum_decl>{
		{
		    "enum Colors : int (deprecated) {"
		    "\n"
		    "  Red = 1,"
		    "\n"
		    "  Blue = 0xab,"
		    "\n"
		    "  Yellow = -5,"
		    "\n"
		    "  Green"
		    "\n"
		    "}",
		    ast::enum_decl{
		        "Colors",
		        ast::enum_type_variant{ ast::type_tag<int>{} },
		        ast::metadata{ { "deprecated", boost::none } },
		        {
		            ast::enumval_decl{ "Red", ast::enumval_variant(uint64_t(1)) },
		            ast::enumval_decl{ "Blue", ast::enumval_variant(uint64_t(0xab)) },
		            ast::enumval_decl{ "Yellow", ast::enumval_variant(int64_t(-5)) },
		            ast::enumval_decl{ "Green", boost::none },
		        },
		    },
		},
		{
		    "enum Colors (deprecated) {"
		    "\n"
		    "  Red = 1,"
		    "\n"
		    "  Blue = 0xab,"
		    "\n"
		    "  Yellow = -5,"
		    "\n"
		    "  Green"
		    "\n"
		    "}",
		    boost::none // enum type information (e.g. (: int) is required
		},
	};
}();

bool enumDecl() {
	return runAstTest(rule::enum_decl, enumDeclInputSet);
}

InputSet<ast::union_subtype_decl> unionSubTypeDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::union_subtype_decl>{
		{ "SomeAlias: SomeType", ast::union_subtype_decl{ "SomeType", "SomeAlias"s } },
		{ "SomeType", ast::union_subtype_decl{ "SomeType", boost::none } },
		{ "SomeAlias: SomeType = SomeValue", boost::none }, // union subtypes cannot default to a value
		{ "SomeAlias: SomeType (deprecated)", boost::none }, // union subtypes are not allowed metadata
	};
}();

bool unionSubTypeDecl() {
	return runAstTest(rule::union_subtype_decl, unionSubTypeDeclInputSet);
}

InputSet<ast::union_decl> unionDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::union_decl>{
		{
		    "union U (priority: 1, deprecated) {"
		    "\n"
		    "  Start: MarkerPosition,"
		    "\n"
		    "  Point: PointPosition,"
		    "\n"
		    "  Direction,"
		    "\n"
		    "  Finish: MarkerPosition"
		    "\n"
		    "}",
		    ast::union_decl{
		        "U",
		        ast::metadata{
		            { "priority", ast::single_value(ast::scalar(uint64_t(1))) },
		            { "deprecated", boost::none },
		        },
		        {
		            ast::union_subtype_decl{ "MarkerPosition", "Start"s },
		            ast::union_subtype_decl{ "PointPosition", "Point"s },
		            ast::union_subtype_decl{ "Direction", boost::none },
		            ast::union_subtype_decl{ "MarkerPosition", "Finish"s },
		        },
		    },
		},
	};
}();

bool unionDecl() {
	return runAstTest(rule::union_decl, unionDeclInputSet);
}

InputSet<ast::include> includeInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::include>{
		{ "include \"schema.fbs\";", ast::include{ "schema.fbs" } },
		{ "include schema.fbs;", boost::none },
		{ "include \"\";", ast::include{ "" } }, // empty include file is not filtered out at parser level
	};
}();

bool include() {
	return runAstTest(rule::include, includeInputSet);
}

InputSet<ast::file_identifier_decl> fileIdentifierDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::file_identifier_decl>{
		{ "file_identifier \"Zxvf\";", ast::file_identifier_decl{ "Zxvf" } },
		{ "file_identifier \"Zxf\";", boost::none }, // file id must be exactly 4 characters
		{ "file_identifier \"Zxvfr\";", boost::none }, // file id must be exactly 4 characters
		{ "file_identifier \"Zx\\xacv\";", ast::file_identifier_decl{ "Zx\xacv" } }, // hex bytes allowed
		{ "file_identifier \"\";", boost::none },
		{ "file_identifier \"T\";", boost::none },
		{ "file_identifier \"xU\";", boost::none },
	};
}();

bool fileIdentifierDecl() {
	return runAstTest(rule::file_identifier_decl, fileIdentifierDeclInputSet);
}

InputSet<ast::file_extension_decl> fileExtensionDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::file_extension_decl>{
		{ "file_extension \"fbs\";", ast::file_extension_decl{ "fbs" } },
		{ "file_extension \"bin\";", ast::file_extension_decl{ "bin" } },
		{ "file_extension fbs;", boost::none }, // needs double quote
	};
}();

bool fileExtensionDecl() {
	return runAstTest(rule::file_extension_decl, fileExtensionDeclInputSet);
}

InputSet<ast::namespace_decl> namespaceDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::namespace_decl>{
		{ "namespace FlatBuffers;", ast::namespace_decl{ { "FlatBuffers" } } },
		{ "namespace FlatBuffers.Schema;", ast::namespace_decl{ { "FlatBuffers", "Schema" } } },
		{ "namespace A.B.C;", ast::namespace_decl{ { "A", "B", "C" } } },
		{ "namespace 1A.B.C;",
		  boost::none }, // namespace elements are identifiers, which can only start with alphabet or _
		{ "namespace ;", boost::none }, // namespace elements cannot be empty
		{ "namespace a", boost::none }, // namespace declaration must end with a semicolon
		{ "namespace a.b.;", boost::none }, // namespace string cannot end with a period
		{ "namespace a.",
		  boost::none }, // namespace declaration must end with a semicolon and namespace name cannot end with a period
	};
}();

bool namespaceDecl() {
	return runAstTest(rule::namespace_decl, namespaceDeclInputSet);
}

InputSet<ast::attribute_decl> attributeDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::attribute_decl>{
		{ "attribute \"attr\";", ast::attribute_decl{ "attr" } },
		{ "attribute attr;", ast::attribute_decl{ "attr" } },
		{ "attribute \"_Attr\";", ast::attribute_decl{ "_Attr" } },
		{ "attribute _Attr;", ast::attribute_decl{ "_Attr" } },
		{ "attribute \"1_Attr\";", boost::none },
		{ "attribute 1_Attr;", boost::none },
		{ "attribute A#ttr;", boost::none },
		{ "attribute \"A#ttr\";", boost::none },
	};
}();

bool attributeDecl() {
	return runAstTest(rule::attribute_decl, attributeDeclInputSet);
}

InputSet<ast::root_type_decl> rootTypeDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::root_type_decl>{
		{ "root_type MyRoot;", ast::root_type_decl{ "MyRoot" } },
		{ "root_type _MyRoot;", ast::root_type_decl{ "_MyRoot" } },
		{ "root_type 1Root;", boost::none },
		{ "root_type R#oot;", boost::none },
	};
}();

bool rootTypeDecl() {
	return runAstTest(rule::root_type_decl, rootTypeDeclInputSet);
}

InputSet<ast::rpc_method_decl> rpcMethodDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::rpc_method_decl>{
		{
		    "Service(Request): Response;",
		    ast::rpc_method_decl{ "Service", "Request", "Response", boost::none },
		},
		{
		    "Service(Request): Response (id: 7);",
		    ast::rpc_method_decl{
		        "Service",
		        "Request",
		        "Response",
		        ast::metadata{ { "id", ast::single_value(ast::scalar(uint64_t(7))) } },
		    },
		},
	};
}();

bool rpcMethodDecl() {
	return runAstTest(rule::rpc_method_decl, rpcMethodDeclInputSet);
}

InputSet<ast::rpc_decl> rpcDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::rpc_decl>{
		{
		    "rpc_service Service {"
		    "\n"
		    "  Store(Monster): StoreResponse;"
		    "\n"
		    "  Retrieve(MonsterId):Monster (deprecated);"
		    "\n"
		    "}",
		    ast::rpc_decl{
		        "Service",
		        {
		            ast::rpc_method_decl{ "Store", "Monster", "StoreResponse", boost::none },
		            ast::rpc_method_decl{
		                "Retrieve",
		                "MonsterId",
		                "Monster",
		                ast::metadata{ { "deprecated", boost::none } },
		            },
		        },
		    },
		},
	};
}();

bool rpcDecl() {
	return runAstTest(rule::rpc_decl, rpcDeclInputSet);
}

InputSet<ast::schema> schemaInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::schema>{
		{
		    R"(
include "common.fbs";

namespace MyGame;

attribute "priority";

enum Color : byte { Red = 1, Green, Blue }

union Any { Monster, Weapon, Pickup }

struct Vec3 {
  x:float;
  y:float;
  z:float;
}

table Monster {
  pos:Vec3;
  mana:short = 150;
  hp:short = 100;
  name:string;
  friendly:bool = false (deprecated, priority: 1);
  inventory:[ubyte];
  color:Color = Blue;
  test:Any;
}

root_type Monster;
)",
		    ast::schema{
		        // includes
		        { ast::include{ "common.fbs" } },
		        // elements
		        {
		            ast::schema_elements_variant{
		                ast::namespace_decl{ { "MyGame" } },
		            },
		            ast::schema_elements_variant{
		                ast::attribute_decl{ "priority" },
		            },
		            ast::schema_elements_variant{
		                ast::enum_decl{
		                    "Color",
		                    ast::enum_type_variant{ ast::type_tag<int8_t>{} },
		                    boost::none, // metadata
		                    {
		                        ast::enumval_decl{ "Red", ast::enumval_variant{ uint64_t(1) } },
		                        ast::enumval_decl{ "Green", boost::none },
		                        ast::enumval_decl{ "Blue", boost::none },
		                    },
		                },
		            },
		            ast::schema_elements_variant{
		                ast::union_decl{
		                    "Any",
		                    boost::none, // metadata
		                    {
		                        ast::union_subtype_decl{ "Monster", boost::none /*no alias*/ },
		                        ast::union_subtype_decl{ "Weapon", boost::none /*no alias*/ },
		                        ast::union_subtype_decl{ "Pickup", boost::none /*no alias*/ },
		                    },
		                },
		            },
		            ast::schema_elements_variant{
		                ast::struct_decl{
		                    "Vec3",
		                    boost::none, // metadata
		                    {
		                        ast::struct_field_decl{
		                            "x",
		                            ast::struct_fieldtype_variant{ ast::type_tag<float>{} },
		                            boost::none, // metadata
		                        },
		                        ast::struct_field_decl{
		                            "y",
		                            ast::struct_fieldtype_variant{ ast::type_tag<float>{} },
		                            boost::none, // metadata
		                        },
		                        ast::struct_field_decl{
		                            "z",
		                            ast::struct_fieldtype_variant{ ast::type_tag<float>{} },
		                            boost::none, // metadata
		                        },
		                    },
		                },
		            },
		            ast::schema_elements_variant{
		                ast::table_decl{
		                    "Monster",
		                    boost::none, // metadata
		                    {
		                        ast::table_field_decl{
		                            "pos",
		                            ast::table_fieldtype_variant{
		                                ast::object_typeinfo{ ast::typeref{ "Vec3", boost::none } } },
		                            boost::none, // metadata
		                        },
		                        ast::table_field_decl{
		                            "mana",
		                            ast::table_fieldtype_variant{
		                                ast::scalar_typeinfo{ ast::scalar_type<int16_t>{ 150 } } },
		                            boost::none, // metadata
		                        },
		                        ast::table_field_decl{
		                            "hp",
		                            ast::table_fieldtype_variant{
		                                ast::scalar_typeinfo{ ast::scalar_type<int16_t>{ 100 } } },
		                            boost::none, // metadata
		                        },
		                        ast::table_field_decl{
		                            "name",
		                            ast::table_fieldtype_variant{
		                                ast::object_typeinfo{ ast::type_tag<std::string>{} } },
		                            boost::none, // metadata
		                        },
		                        ast::table_field_decl{
		                            "friendly",
		                            ast::table_fieldtype_variant{
		                                ast::scalar_typeinfo{ ast::scalar_type<bool>{ false } } },
		                            ast::metadata{
		                                { "deprecated", boost::none },
		                                { "priority", ast::single_value(ast::scalar(uint64_t(1))) },
		                            },
		                        },
		                        ast::table_field_decl{
		                            "inventory",
		                            ast::table_fieldtype_variant{ ast::vector_typeinfo{ ast::type_tag<uint8_t>{} } },
		                            boost::none, // metadata
		                        },
		                        ast::table_field_decl{
		                            "color",
		                            ast::table_fieldtype_variant{
		                                ast::object_typeinfo{ ast::typeref{ "Color", "Blue"s } } },
		                            boost::none, // metadata
		                        },
		                        ast::table_field_decl{
		                            "test",
		                            ast::table_fieldtype_variant{
		                                ast::object_typeinfo{ ast::typeref{ "Any", boost::none } } },
		                            boost::none, // metadata
		                        },
		                    }, // vector<table_field_decl>
		                }, // table_decl
		            },
		            ast::schema_elements_variant{
		                ast::root_type_decl{ "Monster" },
		            },
		        }, // schema elements
		    }, // schema
		},
	};
}();

bool schema() {
	return runAstTest(rule::schema, schemaInputSet);
}

} // namespace test

int main() {
	auto ok = true;
	ok = ok && test::stringConstant();
	ok = ok && test::scalar();
	ok = ok && test::singleValue();
	ok = ok && test::metadatum();
	ok = ok && test::metadata();
	ok = ok && test::scalarTypeInfo();
	ok = ok && test::objectTypeInfo();
	ok = ok && test::vectorTypeInfo();
	ok = ok && test::tableFieldDecl();
	ok = ok && test::tableDecl();
	ok = ok && test::structFieldDecl();
	ok = ok && test::structDecl();
	ok = ok && test::enumValDecl();
	ok = ok && test::enumDecl();
	ok = ok && test::unionSubTypeDecl();
	ok = ok && test::unionDecl();
	ok = ok && test::include();
	ok = ok && test::fileIdentifierDecl();
	ok = ok && test::fileExtensionDecl();
	ok = ok && test::namespaceDecl();
	ok = ok && test::attributeDecl();
	ok = ok && test::rootTypeDecl();
	ok = ok && test::rpcMethodDecl();
	ok = ok && test::rpcDecl();
	ok = ok && test::schema();
	if (!ok) {
		std::cout << "TEST FAILED\n";
		return 1;
	} else {
		std::cout << "TEST OK\n";
		return 0;
	}
}
