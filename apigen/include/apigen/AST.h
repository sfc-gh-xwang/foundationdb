#ifndef FLATBUFFERS_AST_H
#define FLATBUFFERS_AST_H

#include <map>
#include <string>
#include <vector>
#include <boost/spirit/home/x3.hpp>
#include <boost/spirit/home/x3/support/ast/variant.hpp>

namespace boost::spirit::x3 {

// utility for AST comparisons in tests
template <typename... Types>
bool operator==(const variant<Types...>& lhs, const variant<Types...>& rhs) {
	return lhs.get() == rhs.get();
}

} // namespace boost::spirit::x3

namespace ast {

namespace x3 = boost::spirit::x3;

struct scalar : x3::variant<bool, double, uint64_t, int64_t> {
	using base_type::base_type;
	using base_type::operator=;

	static constexpr double EPS = 1e-10;

	bool operator==(const scalar& other) const noexcept = default;
};

struct single_value : x3::variant<scalar, std::string> {
	using base_type::base_type;
	using base_type::operator=;

	bool operator==(const single_value& other) const noexcept = default;
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

struct file_extension_decl {
	std::string ext;
	bool operator==(const file_extension_decl& other) const noexcept = default;
};

struct namespace_decl {
	std::vector<std::string> ns;
	bool operator==(const namespace_decl& other) const noexcept = default;
};

struct attribute_decl {
	std::string name;
	bool operator==(const attribute_decl& other) const noexcept = default;
};

struct root_type_decl {
	std::string name;
	bool operator==(const root_type_decl& other) const noexcept = default;
};

struct rpc_method_decl {
	std::string name;
	std::string request_type;
	std::string response_type;
	boost::optional<metadata> meta;
	bool operator==(const rpc_method_decl& other) const noexcept = default;
};

struct rpc_decl {
	std::string name;
	std::vector<rpc_method_decl> methods;
	bool operator==(const rpc_decl& other) const noexcept = default;
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

using scalar_types_variant = x3::variant<scalar_type<bool>,
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
	// If T is scalar, it's always used in the context of a vector_typeinfo (non-vector case is covered by scalar_type)
	using type = T;
	bool operator==(const type_tag& other) const noexcept { return true; }
};

using object_types_variant = x3::variant<type_tag<std::string>, typeref>;

struct object_typeinfo : object_types_variant {
	using base_type::base_type;
	using base_type::operator=;

	bool operator==(const object_typeinfo& other) const noexcept = default;
};

using singular_types_variant = x3::variant<type_tag<bool>,
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

	bool operator==(const vector_typeinfo& other) const noexcept = default;
};

using table_fieldtype_variant = x3::variant<scalar_typeinfo, object_typeinfo, vector_typeinfo>;

struct table_field_decl {
	std::string name;
	table_fieldtype_variant typeinfo;
	boost::optional<metadata> meta;

	bool operator==(const table_field_decl& other) const noexcept = default;
};

struct table_decl {
	std::string name;
	boost::optional<metadata> meta;
	std::vector<table_field_decl> fields;
	bool operator==(const table_decl& other) const noexcept = default;
};

using struct_fieldtype_variant = x3::variant<type_tag<bool>,
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
	bool operator==(const struct_field_decl& other) const noexcept = default;
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

using enum_type_variant = x3::variant<type_tag<int8_t>,
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

using schema_elements_variant = x3::variant<namespace_decl,
                                            table_decl,
                                            struct_decl,
                                            enum_decl,
                                            union_decl,
                                            root_type_decl,
                                            file_extension_decl,
                                            file_identifier_decl,
                                            attribute_decl,
                                            rpc_decl>;

struct schema {
	std::vector<include> includes;
	std::vector<schema_elements_variant> elements;
	bool operator==(const schema& other) const noexcept = default;
};

} // namespace ast

#endif /*FLATBUFFERS_AST_H*/
