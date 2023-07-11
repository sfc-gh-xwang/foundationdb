#include <boost/spirit/home/x3.hpp>
#include <boost/spirit/home/x3/support/ast/variant.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/fusion/include/std_pair.hpp>
#include <boost/fusion/include/io.hpp>

#include <iostream>
#include <fstream>
#include <string>
#include <map>

// Implements parser for grammar taken from https://flatbuffers.dev/flatbuffers_grammar.html
// with one exception: parsing "object" is not supported and will result in an error.

///////////////////////////////////////////////////////////////////////////////
//  Our AST
///////////////////////////////////////////////////////////////////////////////
namespace client { namespace ast
{
    namespace fusion = boost::fusion;
    namespace x3 = boost::spirit::x3;

	// inclusion of other flatbuffer schema: e.g. include "otherfile.fbs";
	struct include {
		std::string filename;
	};

	struct namespace_decl {
		std::vector<std::string> namespace; // top to bottom, separated by '.'
	};

	// when a table member is another table/struct or a struct member is another struct
	struct type_ref {
		std::string name;
	};

	struct metadata_value : x3::variant<int64_t, double, std::string> {};

	// Metadata is an annotation which follow type and field declarations, a comma-separated list of names with matching values enclosed in parentheses.
	// Values, if exists, follow '[name]:', e.g. '(id: 10, deprecated)'
	// Schema writing guide at https://flatbuffers.dev/flatbuffers_guide_writing_schema.html
	// refers to these as "attributes". Grammar reference refers to this as "metadata",
	// distinguishing from a standalone attribute declaration such as 'attribute "priority";' from the guide example.
	using metadata_map = std::map<std::string, boost::optional<metadata_value>>;

	struct attribute_decl {
		std::string name;
	};

	template <typename T>
	struct field {
		std::string name;
		boost::optional<T> default_;
		metadata_map metadata;
	};

	// strings are not allowed to have default values other than null (empty)
	template <>
	struct field<std::string> {
		std::string name;
		metadata_map metadata;
	};

	// tables and structs aren't allowed to have default values other than null
	template <>
	struct field<type_ref> {
		std::string name;
		metadata_map metadata;
	};

	using singular_field = x3::variant<
		field<int8_t>,
		field<uint8_t>,
		field<int16_t>,
		field<uint16_t>,
		field<int32_t>,
		field<uint32_t>,
		field<int64_t>,
		field<uint64_t>,
		field<float>,
		field<double>,
		field<std::string>,
		field<type_ref>
	>;

	struct vector_field : singular_field {
		using base_type::base_type;
		using base_type::operator=;
	};

	struct type : x3::variant<singular_field, vector_field> {
		using base_type::base_type;
		using base_type::operator=;
	};

	// structs may have only scalars or other structs as members who may not have default values themselves. We don't enforce this upon parsing.
	struct struct_decl {
		std::string name;
		metadata_map metadata;
		std::vector<type> fields;
	};

	struct table_decl {
		std::string name;
		metadata_map metadata;
		std::vector<type> fields;
	};

	template <typename T>
	struct enum_ {
		std::string name;
		std::vector<std::pair<std::string, T>> values;
	};

	struct enum_decl : x3::variant<enum_<int8_t>,
								   enum_<uint8_t>,
								   enum_<int16_t>,
								   enum_<uint16_t>,
								   enum_<int32_t>,
								   enum_<uint32_t>,
								   enum_<int64_t>,
								   enum_<uint64_t>> {
	};

	struct root_decl {
		std::string name;
	};

	struct rpc_method {
		std::string name;
		std::string request_type;
		std::string response_type;
		metadata_map metadata;
	};

	struct rpc_decl {
		std::string name;
		std::vector<rpc_method> methods;
	};

	struct union_decl {
		std::string name;
		metadata_map metadata;
		std::vector<type> fields;
	};

	struct file_identifier_decl {
		std::string id;
	};

	struct file_extension_decl {
		std::string ext;
	};
}}

// We need to tell fusion about our rexpr struct
// to make it a first-class fusion citizen
BOOST_FUSION_ADAPT_STRUCT(client::ast::rexpr,
    entries
)

///////////////////////////////////////////////////////////////////////////////
//  AST processing
///////////////////////////////////////////////////////////////////////////////
namespace client { namespace ast
{
    ///////////////////////////////////////////////////////////////////////////
    //  Print out the rexpr tree
    ///////////////////////////////////////////////////////////////////////////
    int const tabsize = 4;

    struct rexpr_printer
    {
        typedef void result_type;

        rexpr_printer(int indent = 0)
          : indent(indent) {}

        void operator()(rexpr const& ast) const
        {
            std::cout << '{' << std::endl;
            for (auto const& entry : ast.entries)
            {
                tab(indent+tabsize);
                std::cout << '"' << entry.first << "\" = ";
                boost::apply_visitor(rexpr_printer(indent+tabsize), entry.second);
            }
            tab(indent);
            std::cout << '}' << std::endl;
        }

        void operator()(std::string const& text) const
        {
            std::cout << '"' << text << '"' << std::endl;
        }

        void tab(int spaces) const
        {
            for (int i = 0; i < spaces; ++i)
                std::cout << ' ';
        }

        int indent;
    };
}}

///////////////////////////////////////////////////////////////////////////////
//  Our rexpr grammar
///////////////////////////////////////////////////////////////////////////////
namespace client { namespace parser
{
    namespace x3 = boost::spirit::x3;
    namespace ascii = boost::spirit::x3::ascii;

	using x3::double_;
	using x3::hex;
    using x3::lit;
    using x3::lexeme;
	using x3::long_;

    using ascii::alpha;
    using ascii::alnum;
    using ascii::bool_;
    using ascii::char_;
    using ascii::string;

	x3::rule<class ident, std::string> ident = "ident";
	x3::rule<class ident, std::string> ident = "ident";
#ifdef DECLARE_RULE
#error "redefinition of DECLARE_RULE macro"
#endif
#define DECLARE_RULE(NAME) x3::rule<class NAME, ast::NAME> NAME = #NAME
    DECLARE_RULE(include);
    DECLARE_RULE(namespace_decl);
    DECLARE_RULE(struct_decl);
    DECLARE_RULE(table_decl);
    DECLARE_RULE(enum_decl);
    DECLARE_RULE(root_decl);
    DECLARE_RULE(file_extension_decl);
    DECLARE_RULE(file_identifier_decl);
    DECLARE_RULE(attribute_decl);
    DECLARE_RULE(rpc_decl);
    DECLARE_RULE(object);
#undef DECLARE_RULE
	// identifiers: names of namespaces, attributes, types, and fields
	auto const ident_def = (ascii::alpha | '_') >> *(ascii::alnum | '_');

	// quoted string constants with escapes. missing feature: "\uXXXX" which should parses into utf8
	auto const string_constant_def = x3::lexeme['"' > *(
             "\\n" >> x3::attr('\n')
           | "\\t" >> x3::attr('\t')
           | "\\r" >> x3::attr('\r')
           | "\\b" >> x3::attr('\b')
           | "\\f" >> x3::attr('\f')
           | "\\"  >> x3::char_("\"\\/") // escaped double-quote, backslash, and slash
           | "\\x" >> x3::int_parser<char, 16, 2, 2>() // bytes in hex: e.g. "\xff"; always two digits
           | ~x3::char_('"')
       ) > '"'];
	auto const boolean_constant = bool_;
	auto const integer_constant = long_ | hex;
	auto const float_constant = double_;
	auto const scalar = boolean_constant | integer_constant | float_constant;
	auto const single_value = scalar | string_constant;
	auto const metadata = *(ident >> -(lit(':') >> single_value));

	auto const namespace_decl_def = lit("namespace") >> ident >> *('.' >> ident);
	auto const attribute_decl_def = lit("attribute") >> (ident | ('"' >> ident >> '"'));
	auto const struct_decl_def = lit("struct") >> ident >> 
    auto const rexpr_key_value_def =
        quoted_string >> '=' >> rexpr_value;

    auto const rexpr_def =
        '{' >> *rexpr_key_value >> '}';

    BOOST_SPIRIT_DEFINE(rexpr_value, rexpr, rexpr_key_value);
}}

///////////////////////////////////////////////////////////////////////////////
//  Main program
///////////////////////////////////////////////////////////////////////////////
int main(int argc, char **argv)
{
    char const* filename;
    if (argc > 1)
    {
        filename = argv[1];
    }
    else
    {
        std::cerr << "Error: No input file provided." << std::endl;
        return 1;
    }

    std::ifstream in(filename, std::ios_base::in);

    if (!in)
    {
        std::cerr << "Error: Could not open input file: "
            << filename << std::endl;
        return 1;
    }

    std::string storage; // We will read the contents here.
    in.unsetf(std::ios::skipws); // No white space skipping!
    std::copy(
        std::istream_iterator<char>(in),
        std::istream_iterator<char>(),
        std::back_inserter(storage));

    using client::parser::rexpr; // Our grammar
    client::ast::rexpr ast; // Our tree

    using boost::spirit::x3::ascii::space;
    std::string::const_iterator iter = storage.begin();
    std::string::const_iterator end = storage.end();
    bool r = phrase_parse(iter, end, rexpr, space, ast);

    if (r && iter == end)
    {
        std::cout << "-------------------------\n";
        std::cout << "Parsing succeeded\n";
        std::cout << "-------------------------\n";
        client::ast::rexpr_printer printer;
        printer(ast);
        return 0;
    }
    else
    {
        std::string::const_iterator some = iter+30;
        std::string context(iter, (some>end)?end:some);
        std::cout << "-------------------------\n";
        std::cout << "Parsing failed\n";
        std::cout << "stopped at: \": " << context << "...\"\n";
        std::cout << "-------------------------\n";
        return 1;
    }
}
