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
	return runAstTest(rule::StringConstant, StringConstantInputSet);
}

InputSet<ast::Scalar> const scalarInputSet = {
	{ "1.0", ast::Scalar(1.0) }, // double
	{ "10", ast::Scalar(uint64_t(10)) }, // uint64
	{ "-10.0", ast::Scalar(-10.0) }, // double
	{ "-10", ast::Scalar(int64_t(-10)) }, // int64
	{ "-3", ast::Scalar(int64_t(-3)) }, // int64
	{ "0xfe", ast::Scalar(uint64_t(0xfe)) }, // uint64
	{ "0xDeadBeefDeadBeef", ast::Scalar(uint64_t(0xdeadbeefdeadbeefUL)) }, // uint64
	{ "0xdeadbeefdeadbeef", ast::Scalar(uint64_t(0xdeadbeefdeadbeefUL)) }, // uint64
	{ "-0xdeadbeefdeadbeef",
	  boost::none }, // negative hex int64 not supported yet TODO: make this throw a parse error with message
	{ "true", ast::Scalar(true) }, // bool
	{ "false", ast::Scalar(false) }, // bool
	{ "1", ast::Scalar(uint64_t(1)) }, // uint64
	{ "0", ast::Scalar(uint64_t(0)) }, // uint64
};

bool scalar() {
	return runAstTest(rule::Scalar, scalarInputSet);
}

// reuse scalar/string_constant test input for single value test
InputSet<ast::SingleValue> const singleValueInputSet = []() -> InputSet<ast::SingleValue> {
	auto ret = InputSet<ast::SingleValue>();
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
	return runAstTest(rule::SingleValue, singleValueInputSet);
}

InputSet<ast::Metadatum> const metadatumInputSet = []() {
	using namespace std::string_literals;
	auto ret = InputSet<ast::Metadatum>{
		{ "attr", ast::Metadatum{ "attr", boost::none } },
		{ "abc: \"def\"", ast::Metadatum{ "abc", ast::SingleValue("def"s) } },
		{ "id: 42", ast::Metadatum{ "id", ast::SingleValue(ast::Scalar(uint64_t(42))) } },
		{ "deprecated", ast::Metadatum{ "deprecated", boost::none } },
		{ "required", ast::Metadatum{ "required", boost::none } },
		{ "force_align: 16", ast::Metadatum{ "force_align", ast::SingleValue(ast::Scalar(uint64_t(16))) } },
		{ "nested_flatbuffer: \"someTable\"", ast::Metadatum{ "nested_flatbuffer", ast::SingleValue("someTable"s) } },
	};
	return ret;
}();

bool metadatum() {
	return runAstTest(rule::Metadatum, metadatumInputSet);
}

InputSet<ast::Metadata> const metadataInputSet = []() {
	using namespace std::string_literals;
	InputSet<ast::Metadata> ret = {
		{
		    "(a: 10, b: \"c\", d, e: true)"s,
		    ast::Metadata{ { "a", ast::SingleValue(ast::Scalar(uint64_t(10))) },
		                   { "b", ast::SingleValue("c"s) },
		                   { "d", boost::none },
		                   { "e", ast::SingleValue(ast::Scalar(true)) } },
		},
		{
		    // because x3 parser's parse-into-container class/function template uses map.insert() which does not
		    // update entries in-place, any insertions to the existing key are silently ignored, which isn't great.
		    "(a, b: 4, a: 3)",
		    ast::Metadata{
		        { "a", boost::none },
		        { "b", ast::SingleValue(ast::Scalar(uint64_t(4))) },
		    },
		},
		{
		    "(a: true, a: \"def\")",
		    ast::Metadata{
		        { "a", ast::SingleValue(ast::Scalar(true)) },
		    },
		},
		{ "()", boost::none }, // empty metadata fails x3::list parser rule
	};
	return ret;
}();

bool metadata() {
	return runAstTest(rule::Metadata, metadataInputSet);
}

InputSet<ast::ScalarTypeInfo> scalarTypeInfoInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::ScalarTypeInfo>{
		{
		    "int = 10",
		    ast::ScalarTypeInfo{ ast::ScalarType<int32_t>{ 10 } },
		},
		{
		    "bool",
		    ast::ScalarTypeInfo{ ast::ScalarType<bool>{ boost::none } },
		},
		{
		    "bool = true",
		    ast::ScalarTypeInfo{ ast::ScalarType<bool>{ true } },
		},
		{
		    "ulong",
		    ast::ScalarTypeInfo{ ast::ScalarType<uint64_t>{ boost::none } },
		},
		{
		    "int64 = -5",
		    ast::ScalarTypeInfo{ ast::ScalarType<int64_t>{ -5 } },
		},
		{
		    "double = -3.14",
		    ast::ScalarTypeInfo{ ast::ScalarType<double>{ -3.14 } },
		},
		{
		    "double",
		    ast::ScalarTypeInfo{ ast::ScalarType<double>{ boost::none } },
		},
	};
}();

bool scalarTypeInfo() {
	return runAstTest(rule::ScalarTypeInfo, scalarTypeInfoInputSet);
}

InputSet<ast::ObjectTypeInfo> objectTypeInfoInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::ObjectTypeInfo>{
		{
		    "string",
		    ast::ObjectTypeInfo{ ast::TypeTag<std::string>{} },
		},
		{
		    "def",
		    ast::ObjectTypeInfo{ ast::TypeRef{ "def", boost::none } },
		},
		{
		    "someType",
		    ast::ObjectTypeInfo{ ast::TypeRef{ "someType", boost::none } },
		},
		{
		    "1someType", // identifiers cannot start with a number
		    boost::none,
		},
	};
}();

bool objectTypeInfo() {
	return runAstTest(rule::ObjectTypeInfo, objectTypeInfoInputSet);
}

InputSet<ast::VectorTypeInfo> vectorTypeInfoInputSet = []() {
	return InputSet<ast::VectorTypeInfo>{
		{
		    "[string]",
		    ast::VectorTypeInfo{ ast::TypeTag<std::string>{} },
		},
		{
		    "[def]",
		    ast::VectorTypeInfo{ ast::TypeRef{ "def", boost::none } },
		},
		{
		    "[someType]",
		    ast::VectorTypeInfo{ ast::TypeRef{ "someType", boost::none } },
		},
		{
		    "[1someType]", // identifiers cannot start with a number
		    boost::none,
		},
		{
		    "[uint]",
		    ast::VectorTypeInfo{ ast::TypeTag<uint32_t>{} },
		},
		{
		    "[uint32]",
		    ast::VectorTypeInfo{ ast::TypeTag<uint32_t>{} },
		},
		{
		    "[int]",
		    ast::VectorTypeInfo{ ast::TypeTag<int32_t>{} },
		},
		{
		    "[int32]",
		    ast::VectorTypeInfo{ ast::TypeTag<int32_t>{} },
		},
		{
		    "[uint64]",
		    ast::VectorTypeInfo{ ast::TypeTag<uint64_t>{} },
		},
		{
		    "[ulong]",
		    ast::VectorTypeInfo{ ast::TypeTag<uint64_t>{} },
		},
		{
		    "[int64]",
		    ast::VectorTypeInfo{ ast::TypeTag<int64_t>{} },
		},
		{
		    "[long]",
		    ast::VectorTypeInfo{ ast::TypeTag<int64_t>{} },
		},
		{
		    "[uint16]",
		    ast::VectorTypeInfo{ ast::TypeTag<uint16_t>{} },
		},
		{
		    "[ushort]",
		    ast::VectorTypeInfo{ ast::TypeTag<uint16_t>{} },
		},
		{
		    "[int16]",
		    ast::VectorTypeInfo{ ast::TypeTag<int16_t>{} },
		},
		{
		    "[short]",
		    ast::VectorTypeInfo{ ast::TypeTag<int16_t>{} },
		},
		{
		    "[uint8]",
		    ast::VectorTypeInfo{ ast::TypeTag<uint8_t>{} },
		},
		{
		    "[ubyte]",
		    ast::VectorTypeInfo{ ast::TypeTag<uint8_t>{} },
		},
		{
		    "[int8]",
		    ast::VectorTypeInfo{ ast::TypeTag<int8_t>{} },
		},
		{
		    "[byte]",
		    ast::VectorTypeInfo{ ast::TypeTag<int8_t>{} },
		},
		{
		    "[bool]",
		    ast::VectorTypeInfo{ ast::TypeTag<bool>{} },
		},
	};
}();

bool vectorTypeInfo() {
	return runAstTest(rule::VectorTypeInfo, vectorTypeInfoInputSet);
}

InputSet<ast::TableFieldDecl> tableFieldDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::TableFieldDecl>{
		{
		    "a : int = 10;",
		    ast::TableFieldDecl{
		        "a",
		        ast::TableFieldTypesVariant{ ast::ScalarTypeInfo{ ast::ScalarType<int32_t>{ 10 } } },
		        boost::none,
		    },
		},
		{
		    "Names: [string] (required);",
		    ast::TableFieldDecl{
		        "Names",
		        ast::TableFieldTypesVariant{ ast::VectorTypeInfo{ ast::TypeTag<std::string>{} } },
		        ast::Metadata{ { "required", boost::none } },
		    },
		},
	};
}();

bool tableFieldDecl() {
	return runAstTest(rule::TableFieldDecl, tableFieldDeclInputSet);
}

InputSet<ast::TableDecl> tableDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::TableDecl>{
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
		    ast::TableDecl{
		        "Object",
		        ast::Metadata{
		            { "id", ast::SingleValue(ast::Scalar(uint64_t(10))) },
		        },
		        {
		            ast::TableFieldDecl{
		                "name",
		                ast::TableFieldTypesVariant{
		                    ast::ObjectTypeInfo{ ast::TypeTag<std::string>{} },
		                },
		                ast::Metadata{
		                    { "required", boost::none },
		                    { "priority", ast::SingleValue(ast::Scalar(uint64_t(1))) },
		                },
		            },
		            ast::TableFieldDecl{
		                "points",
		                ast::TableFieldTypesVariant{
		                    ast::VectorTypeInfo{ ast::TypeRef{ "Point", boost::none } },
		                },
		                boost::none,
		            },
		            ast::TableFieldDecl{
		                "shape",
		                ast::TableFieldTypesVariant{
		                    ast::ObjectTypeInfo{ ast::TypeRef{ "Shape", "Rectangle"s } },
		                },
		                ast::Metadata{ { "required", boost::none } },
		            },
		        },
		    },
		},
	};
}();

bool tableDecl() {
	return runAstTest(rule::TableDecl, tableDeclInputSet);
}

InputSet<ast::StructFieldDecl> structFieldDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::StructFieldDecl>{
		{
		    "a : int = 10;",
		    boost::none, // struct fields cannot have defaults
		},
		{
		    "a : int;",
		    ast::StructFieldDecl{
		        "a",
		        ast::StructFieldTypesVariant{ ast::TypeTag<int>{} },
		        boost::none,
		    },
		},
		{
		    "s : string;",
		    boost::none, // string should not be allowed as type
		},
		{
		    "s : string_;",
		    ast::StructFieldDecl{
		        "s",
		        ast::StructFieldTypesVariant{ ast::TypeRef{ "string_", boost::none } },
		        boost::none,
		    }, // string_ could technically be another struct or an enum
		},
	};
}();

bool structFieldDecl() {
	return runAstTest(rule::StructFieldDecl, structFieldDeclInputSet);
}

InputSet<ast::StructDecl> structDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::StructDecl>{
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
		    ast::StructDecl{
		        "SomeStruct",
		        ast::Metadata{
		            { "id", ast::SingleValue(ast::Scalar(uint64_t(10))) },
		        },
		        {
		            ast::StructFieldDecl{
		                "x",
		                ast::StructFieldTypesVariant{ ast::TypeTag<int>{} },
		                ast::Metadata{
		                    { "priority", ast::SingleValue(ast::Scalar(uint64_t(1))) },
		                },
		            },
		            ast::StructFieldDecl{
		                "Y",
		                ast::StructFieldTypesVariant{ ast::TypeTag<float>{} },
		                boost::none,
		            },
		            ast::StructFieldDecl{
		                "shape",
		                ast::StructFieldTypesVariant{ ast::TypeRef{ "Shape", boost::none } },
		                boost::none,
		            },
		        },
		    },
		},
	};
}();

bool structDecl() {
	return runAstTest(rule::StructDecl, structDeclInputSet);
}

InputSet<ast::EnumValDecl> enumValDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::EnumValDecl>{
		{ "Red = 1", ast::EnumValDecl{ "Red", ast::EnumValVariant(uint64_t(1)) } },
		{ "Blue", ast::EnumValDecl{ "Blue", boost::none } },
		{ "Yellow = -3", ast::EnumValDecl{ "Yellow", ast::EnumValVariant(int64_t(-3)) } },
		{ "Yellow = -3 (deprecated)", boost::none }, // no metadata allowed for enum values
	};
}();

bool enumValDecl() {
	return runAstTest(rule::EnumValDecl, enumValDeclInputSet);
}

InputSet<ast::EnumDecl> enumDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::EnumDecl>{
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
		    ast::EnumDecl{
		        "Colors",
		        ast::EnumTypesVariant{ ast::TypeTag<int>{} },
		        ast::Metadata{ { "deprecated", boost::none } },
		        {
		            ast::EnumValDecl{ "Red", ast::EnumValVariant(uint64_t(1)) },
		            ast::EnumValDecl{ "Blue", ast::EnumValVariant(uint64_t(0xab)) },
		            ast::EnumValDecl{ "Yellow", ast::EnumValVariant(int64_t(-5)) },
		            ast::EnumValDecl{ "Green", boost::none },
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
	return runAstTest(rule::EnumDecl, enumDeclInputSet);
}

InputSet<ast::UnionSubTypeDecl> unionSubTypeDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::UnionSubTypeDecl>{
		{ "SomeAlias: SomeType", ast::UnionSubTypeDecl{ "SomeType", "SomeAlias"s } },
		{ "SomeType", ast::UnionSubTypeDecl{ "SomeType", boost::none } },
		{ "SomeAlias: SomeType = SomeValue", boost::none }, // union subtypes cannot default to a value
		{ "SomeAlias: SomeType (deprecated)", boost::none }, // union subtypes are not allowed metadata
	};
}();

bool unionSubTypeDecl() {
	return runAstTest(rule::UnionSubTypeDecl, unionSubTypeDeclInputSet);
}

InputSet<ast::UnionDecl> unionDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::UnionDecl>{
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
		    ast::UnionDecl{
		        "U",
		        ast::Metadata{
		            { "priority", ast::SingleValue(ast::Scalar(uint64_t(1))) },
		            { "deprecated", boost::none },
		        },
		        {
		            ast::UnionSubTypeDecl{ "MarkerPosition", "Start"s },
		            ast::UnionSubTypeDecl{ "PointPosition", "Point"s },
		            ast::UnionSubTypeDecl{ "Direction", boost::none },
		            ast::UnionSubTypeDecl{ "MarkerPosition", "Finish"s },
		        },
		    },
		},
	};
}();

bool unionDecl() {
	return runAstTest(rule::UnionDecl, unionDeclInputSet);
}

InputSet<ast::Include> includeInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::Include>{
		{ "include \"schema.fbs\";", ast::Include{ "schema.fbs" } },
		{ "include schema.fbs;", boost::none },
		{ "include \"\";", ast::Include{ "" } }, // empty include file is not filtered out at parser level
	};
}();

bool include() {
	return runAstTest(rule::Include, includeInputSet);
}

InputSet<ast::FileIdentifierDecl> fileIdentifierDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::FileIdentifierDecl>{
		{ "file_identifier \"Zxvf\";", ast::FileIdentifierDecl{ "Zxvf" } },
		{ "file_identifier \"Zxf\";", boost::none }, // file id must be exactly 4 characters
		{ "file_identifier \"Zxvfr\";", boost::none }, // file id must be exactly 4 characters
		{ "file_identifier \"Zx\\xacv\";", ast::FileIdentifierDecl{ "Zx\xacv" } }, // hex bytes allowed
		{ "file_identifier \"\";", boost::none },
		{ "file_identifier \"T\";", boost::none },
		{ "file_identifier \"xU\";", boost::none },
	};
}();

bool fileIdentifierDecl() {
	return runAstTest(rule::FileIdentifierDecl, fileIdentifierDeclInputSet);
}

InputSet<ast::FileExtensionDecl> fileExtensionDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::FileExtensionDecl>{
		{ "file_extension \"fbs\";", ast::FileExtensionDecl{ "fbs" } },
		{ "file_extension \"bin\";", ast::FileExtensionDecl{ "bin" } },
		{ "file_extension fbs;", boost::none }, // needs double quote
	};
}();

bool fileExtensionDecl() {
	return runAstTest(rule::FileExtensionDecl, fileExtensionDeclInputSet);
}

InputSet<ast::NamespaceDecl> namespaceDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::NamespaceDecl>{
		{ "namespace FlatBuffers;", ast::NamespaceDecl{ { "FlatBuffers" } } },
		{ "namespace FlatBuffers.Schema;", ast::NamespaceDecl{ { "FlatBuffers", "Schema" } } },
		{ "namespace A.B.C;", ast::NamespaceDecl{ { "A", "B", "C" } } },
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
	return runAstTest(rule::NamespaceDecl, namespaceDeclInputSet);
}

InputSet<ast::AttributeDecl> attributeDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::AttributeDecl>{
		{ "attribute \"attr\";", ast::AttributeDecl{ "attr" } },
		{ "attribute attr;", ast::AttributeDecl{ "attr" } },
		{ "attribute \"_Attr\";", ast::AttributeDecl{ "_Attr" } },
		{ "attribute _Attr;", ast::AttributeDecl{ "_Attr" } },
		{ "attribute \"1_Attr\";", boost::none },
		{ "attribute 1_Attr;", boost::none },
		{ "attribute A#ttr;", boost::none },
		{ "attribute \"A#ttr\";", boost::none },
	};
}();

bool attributeDecl() {
	return runAstTest(rule::AttributeDecl, attributeDeclInputSet);
}

InputSet<ast::RootTypeDecl> rootTypeDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::RootTypeDecl>{
		{ "root_type MyRoot;", ast::RootTypeDecl{ "MyRoot" } },
		{ "root_type _MyRoot;", ast::RootTypeDecl{ "_MyRoot" } },
		{ "root_type 1Root;", boost::none },
		{ "root_type R#oot;", boost::none },
	};
}();

bool rootTypeDecl() {
	return runAstTest(rule::RootTypeDecl, rootTypeDeclInputSet);
}

InputSet<ast::RpcMethodDecl> rpcMethodDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::RpcMethodDecl>{
		{
		    "Service(Request): Response;",
		    ast::RpcMethodDecl{ "Service", "Request", "Response", boost::none },
		},
		{
		    "Service(Request): Response (id: 7);",
		    ast::RpcMethodDecl{
		        "Service",
		        "Request",
		        "Response",
		        ast::Metadata{ { "id", ast::SingleValue(ast::Scalar(uint64_t(7))) } },
		    },
		},
	};
}();

bool rpcMethodDecl() {
	return runAstTest(rule::RpcMethodDecl, rpcMethodDeclInputSet);
}

InputSet<ast::RpcDecl> rpcDeclInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::RpcDecl>{
		{
		    "rpc_service Service {"
		    "\n"
		    "  Store(Monster): StoreResponse;"
		    "\n"
		    "  Retrieve(MonsterId):Monster (deprecated);"
		    "\n"
		    "}",
		    ast::RpcDecl{
		        "Service",
		        {
		            ast::RpcMethodDecl{ "Store", "Monster", "StoreResponse", boost::none },
		            ast::RpcMethodDecl{
		                "Retrieve",
		                "MonsterId",
		                "Monster",
		                ast::Metadata{ { "deprecated", boost::none } },
		            },
		        },
		    },
		},
	};
}();

bool rpcDecl() {
	return runAstTest(rule::RpcDecl, rpcDeclInputSet);
}

InputSet<ast::Schema> schemaInputSet = []() {
	using namespace std::string_literals;
	return InputSet<ast::Schema>{
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
		    ast::Schema{
		        // includes
		        { ast::Include{ "common.fbs" } },
		        // elements
		        {
		            ast::SchemaElementsVariant{
		                ast::NamespaceDecl{ { "MyGame" } },
		            },
		            ast::SchemaElementsVariant{
		                ast::AttributeDecl{ "priority" },
		            },
		            ast::SchemaElementsVariant{
		                ast::EnumDecl{
		                    "Color",
		                    ast::EnumTypesVariant{ ast::TypeTag<int8_t>{} },
		                    boost::none, // metadata
		                    {
		                        ast::EnumValDecl{ "Red", ast::EnumValVariant{ uint64_t(1) } },
		                        ast::EnumValDecl{ "Green", boost::none },
		                        ast::EnumValDecl{ "Blue", boost::none },
		                    },
		                },
		            },
		            ast::SchemaElementsVariant{
		                ast::UnionDecl{
		                    "Any",
		                    boost::none, // metadata
		                    {
		                        ast::UnionSubTypeDecl{ "Monster", boost::none /*no alias*/ },
		                        ast::UnionSubTypeDecl{ "Weapon", boost::none /*no alias*/ },
		                        ast::UnionSubTypeDecl{ "Pickup", boost::none /*no alias*/ },
		                    },
		                },
		            },
		            ast::SchemaElementsVariant{
		                ast::StructDecl{
		                    "Vec3",
		                    boost::none, // metadata
		                    {
		                        ast::StructFieldDecl{
		                            "x",
		                            ast::StructFieldTypesVariant{ ast::TypeTag<float>{} },
		                            boost::none, // metadata
		                        },
		                        ast::StructFieldDecl{
		                            "y",
		                            ast::StructFieldTypesVariant{ ast::TypeTag<float>{} },
		                            boost::none, // metadata
		                        },
		                        ast::StructFieldDecl{
		                            "z",
		                            ast::StructFieldTypesVariant{ ast::TypeTag<float>{} },
		                            boost::none, // metadata
		                        },
		                    },
		                },
		            },
		            ast::SchemaElementsVariant{
		                ast::TableDecl{
		                    "Monster",
		                    boost::none, // metadata
		                    {
		                        ast::TableFieldDecl{
		                            "pos",
		                            ast::TableFieldTypesVariant{
		                                ast::ObjectTypeInfo{ ast::TypeRef{ "Vec3", boost::none } } },
		                            boost::none, // metadata
		                        },
		                        ast::TableFieldDecl{
		                            "mana",
		                            ast::TableFieldTypesVariant{
		                                ast::ScalarTypeInfo{ ast::ScalarType<int16_t>{ 150 } } },
		                            boost::none, // metadata
		                        },
		                        ast::TableFieldDecl{
		                            "hp",
		                            ast::TableFieldTypesVariant{
		                                ast::ScalarTypeInfo{ ast::ScalarType<int16_t>{ 100 } } },
		                            boost::none, // metadata
		                        },
		                        ast::TableFieldDecl{
		                            "name",
		                            ast::TableFieldTypesVariant{ ast::ObjectTypeInfo{ ast::TypeTag<std::string>{} } },
		                            boost::none, // metadata
		                        },
		                        ast::TableFieldDecl{
		                            "friendly",
		                            ast::TableFieldTypesVariant{
		                                ast::ScalarTypeInfo{ ast::ScalarType<bool>{ false } } },
		                            ast::Metadata{
		                                { "deprecated", boost::none },
		                                { "priority", ast::SingleValue(ast::Scalar(uint64_t(1))) },
		                            },
		                        },
		                        ast::TableFieldDecl{
		                            "inventory",
		                            ast::TableFieldTypesVariant{ ast::VectorTypeInfo{ ast::TypeTag<uint8_t>{} } },
		                            boost::none, // metadata
		                        },
		                        ast::TableFieldDecl{
		                            "color",
		                            ast::TableFieldTypesVariant{
		                                ast::ObjectTypeInfo{ ast::TypeRef{ "Color", "Blue"s } } },
		                            boost::none, // metadata
		                        },
		                        ast::TableFieldDecl{
		                            "test",
		                            ast::TableFieldTypesVariant{
		                                ast::ObjectTypeInfo{ ast::TypeRef{ "Any", boost::none } } },
		                            boost::none, // metadata
		                        },
		                    }, // vector<table_field_decl>
		                }, // table_decl
		            },
		            ast::SchemaElementsVariant{
		                ast::RootTypeDecl{ "Monster" },
		            },
		        }, // schema elements
		    }, // schema
		},
	};
}();

bool schema() {
	return runAstTest(rule::Schema, schemaInputSet);
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
