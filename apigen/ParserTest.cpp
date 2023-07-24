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

input_set_type<ast::file_extension_decl> file_extension_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::file_extension_decl>{
		{ "file_extension \"fbs\";", ast::file_extension_decl{"fbs"}},
		{ "file_extension \"bin\";", ast::file_extension_decl{"bin"} },
		{ "file_extension fbs;", boost::none }, // needs double quote
	};
}();

bool file_extension_decl() {
	return run_ast_test(rule::file_extension_decl, file_extension_decl_input_set);
}

input_set_type<ast::namespace_decl> namespace_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::namespace_decl>{
		{ "namespace FlatBuffers;", ast::namespace_decl{{"FlatBuffers"}}},
		{ "namespace FlatBuffers.Schema;", ast::namespace_decl{{"FlatBuffers", "Schema"}}},
		{ "namespace A.B.C;", ast::namespace_decl{{"A", "B", "C"}}},
		{ "namespace 1A.B.C;", boost::none }, // namespace elements are identifiers, which can only start with alphabet or _
		{ "namespace ;", boost::none }, // namespace elements cannot be empty
		{ "namespace a", boost::none }, // namespace declaration must end with a semicolon
		{ "namespace a.b.;", boost::none }, // namespace string cannot end with a period
		{ "namespace a.", boost::none }, // namespace declaration must end with a semicolon and namespace name cannot end with a period
	};
}();

bool namespace_decl() {
	return run_ast_test(rule::namespace_decl, namespace_decl_input_set);
}

input_set_type<ast::attribute_decl> attribute_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::attribute_decl>{
		{ "attribute \"attr\";", ast::attribute_decl{"attr"}},
		{ "attribute attr;", ast::attribute_decl{"attr"}},
		{ "attribute \"_Attr\";", ast::attribute_decl{"_Attr"}},
		{ "attribute _Attr;", ast::attribute_decl{"_Attr"}},
		{ "attribute \"1_Attr\";", boost::none },
		{ "attribute 1_Attr;", boost::none },
		{ "attribute A#ttr;", boost::none },
		{ "attribute \"A#ttr\";", boost::none },
	};
}();

bool attribute_decl() {
	return run_ast_test(rule::attribute_decl, attribute_decl_input_set);
}

input_set_type<ast::root_type_decl> root_type_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::root_type_decl>{
		{ "root_type MyRoot;", ast::root_type_decl{"MyRoot"}},
		{ "root_type _MyRoot;", ast::root_type_decl{"_MyRoot"}},
		{ "root_type 1Root;", boost::none },
		{ "root_type R#oot;", boost::none },
	};
}();

bool root_type_decl() {
	return run_ast_test(rule::root_type_decl, root_type_decl_input_set);
}

input_set_type<ast::rpc_method_decl> rpc_method_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::rpc_method_decl>{
		{
			"Service(Request): Response;",
			ast::rpc_method_decl{
				"Service", "Request", "Response", boost::none },
		},
		{
			"Service(Request): Response (id: 7);",
			ast::rpc_method_decl{
				"Service",
				"Request",
				"Response",
				ast::metadata{
					{"id", ast::single_value(ast::scalar(uint64_t(7))) }
				},
			},
		},
	};
}();

bool rpc_method_decl() {
	return run_ast_test(rule::rpc_method_decl, rpc_method_decl_input_set);
}

input_set_type<ast::rpc_decl> rpc_decl_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::rpc_decl>{
		{
			"rpc_service Service {"                          "\n"
			"  Store(Monster): StoreResponse;"               "\n"
			"  Retrieve(MonsterId):Monster (deprecated);"    "\n"
			"}",
			ast::rpc_decl{
				"Service",
				{
					ast::rpc_method_decl{
						"Store", "Monster", "StoreResponse",
						boost::none},
					ast::rpc_method_decl{
						"Retrieve", "MonsterId", "Monster",
						ast::metadata{{"deprecated", boost::none}},
					},
				},
			},
		},
	};
}();

bool rpc_decl() {
	return run_ast_test(rule::rpc_decl, rpc_decl_input_set);
}

input_set_type<ast::schema> schema_input_set = []() {
	using namespace std::string_literals;
	return input_set_type<ast::schema>{
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
				{
					ast::include{"common.fbs"}
				},
				// elements
				{
					ast::schema_elements_variant{
						ast::namespace_decl{{"MyGame"}},
					},
					ast::schema_elements_variant{
						ast::attribute_decl{"priority"},
					},
					ast::schema_elements_variant{
						ast::enum_decl{
							"Color",
							ast::enum_type_variant{ast::type_tag<int8_t>{}},
							boost::none, // metadata
							{
								ast::enumval_decl{"Red", ast::enumval_variant{uint64_t(1)}},
								ast::enumval_decl{"Green", boost::none},
								ast::enumval_decl{"Blue", boost::none},
							},
						},
					},
					ast::schema_elements_variant{
						ast::union_decl{
							"Any",
							boost::none, // metadata
							{
								ast::union_subtype_decl{"Monster", boost::none /*no alias*/},
								ast::union_subtype_decl{"Weapon", boost::none /*no alias*/},
								ast::union_subtype_decl{"Pickup", boost::none /*no alias*/},
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
									ast::struct_fieldtype_variant{ast::type_tag<float>{}},
									boost::none, // metadata
								},
								ast::struct_field_decl{
									"y",
									ast::struct_fieldtype_variant{ast::type_tag<float>{}},
									boost::none, // metadata
								},
								ast::struct_field_decl{
									"z",
									ast::struct_fieldtype_variant{ast::type_tag<float>{}},
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
										ast::object_typeinfo{
											ast::typeref{"Vec3", boost::none}}},
									boost::none, // metadata
								},
								ast::table_field_decl{
									"mana",
									ast::table_fieldtype_variant{
										ast::scalar_typeinfo{
											ast::scalar_type<int16_t>{150}}},
									boost::none, // metadata
								},
								ast::table_field_decl{
									"hp",
									ast::table_fieldtype_variant{
										ast::scalar_typeinfo{
											ast::scalar_type<int16_t>{100}}},
									boost::none, // metadata
								},
								ast::table_field_decl{
									"name",
									ast::table_fieldtype_variant{
										ast::object_typeinfo{
											ast::type_tag<std::string>{}}},
									boost::none, // metadata
								},
								ast::table_field_decl{
									"friendly",
									ast::table_fieldtype_variant{
										ast::scalar_typeinfo{
											ast::scalar_type<bool>{false}}},
									ast::metadata{
										{"deprecated", boost::none},
										{"priority", ast::single_value(ast::scalar(uint64_t(1)))},
									},
								},
								ast::table_field_decl{
									"inventory",
									ast::table_fieldtype_variant{
										ast::vector_typeinfo{
											ast::type_tag<uint8_t>{}}},
									boost::none, // metadata
								},
								ast::table_field_decl{
									"color",
									ast::table_fieldtype_variant{
										ast::object_typeinfo{
											ast::typeref{"Color", "Blue"s}}},
									boost::none, // metadata
								},
								ast::table_field_decl{
									"test",
									ast::table_fieldtype_variant{
										ast::object_typeinfo{
											ast::typeref{"Any", boost::none}}},
									boost::none, // metadata
								},
							}, // vector<table_field_decl>
						}, // table_decl
					},
					ast::schema_elements_variant{
						ast::root_type_decl{"Monster"},
					},
				}, // schema elements
			}, // schema
		},
	};
}();

bool schema() {
	return run_ast_test(rule::schema, schema_input_set);
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
	ok = ok && test::file_extension_decl();
	ok = ok && test::namespace_decl();
	ok = ok && test::attribute_decl();
	ok = ok && test::root_type_decl();
	ok = ok && test::rpc_method_decl();
	ok = ok && test::rpc_decl();
	ok = ok && test::schema();
	if (!ok) {
		std::cout << "TEST FAILED\n";
		return 1;
	} else {
		std::cout << "TEST OK\n";
		return 0;
	}
}
