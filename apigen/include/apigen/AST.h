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

struct Scalar : x3::variant<bool, double, uint64_t, int64_t> {
	using base_type::base_type;
	using base_type::operator=;

	static constexpr double EPS = 1e-10;

	bool operator==(const Scalar& other) const noexcept = default;
};

struct SingleValue : x3::variant<Scalar, std::string> {
	using base_type::base_type;
	using base_type::operator=;

	bool operator==(const SingleValue& other) const noexcept = default;
};

using Metadatum = std::pair<std::string, boost::optional<SingleValue>>;
using Metadata = std::map<std::string, boost::optional<SingleValue>>;

struct Include {
	std::string filename;
	bool operator==(const Include& other) const noexcept = default;
};

struct FileIdentifierDecl {
	std::string id;
	bool operator==(const FileIdentifierDecl& other) const noexcept = default;
};

struct FileExtensionDecl {
	std::string ext;
	bool operator==(const FileExtensionDecl& other) const noexcept = default;
};

struct NamespaceDecl {
	std::vector<std::string> ns;
	bool operator==(const NamespaceDecl& other) const noexcept = default;
};

struct AttributeDecl {
	std::string name;
	bool operator==(const AttributeDecl& other) const noexcept = default;
};

struct RootTypeDecl {
	std::string name;
	bool operator==(const RootTypeDecl& other) const noexcept = default;
};

struct RpcMethodDecl {
	std::string name;
	std::string requestType;
	std::string responseType;
	boost::optional<Metadata> meta;
	bool operator==(const RpcMethodDecl& other) const noexcept = default;
};

struct RpcDecl {
	std::string name;
	std::vector<RpcMethodDecl> methods;
	bool operator==(const RpcDecl& other) const noexcept = default;
};

// when a table member is another table/struct/union or a struct member is another struct
struct TypeRef {
	std::string name;
	// support for when *name* refers to an enum type and the field has a default value
	boost::optional<std::string> default_;
	bool operator==(const TypeRef& other) const noexcept = default;
};

template <class T>
struct ScalarType {
	boost::optional<T> default_;
	bool operator==(const ScalarType& other) const noexcept = default;
};

using ScalarTypesVariant = x3::variant<ScalarType<bool>,
                                       ScalarType<int8_t>,
                                       ScalarType<uint8_t>,
                                       ScalarType<int16_t>,
                                       ScalarType<uint16_t>,
                                       ScalarType<int64_t>,
                                       ScalarType<uint64_t>,
                                       ScalarType<int32_t>,
                                       ScalarType<uint32_t>,
                                       ScalarType<float>,
                                       ScalarType<double>>;

struct ScalarTypeInfo : ScalarTypesVariant {
	using base_type::base_type;
	using base_type::operator=;

	bool operator==(const ScalarTypeInfo& other) const noexcept = default;
};

template <typename T>
struct TypeTag {
	// If T is scalar, it's always used in the context of a VectorTypeInfo (non-vector case is covered by ScalarType)
	using type = T;
	bool operator==(const TypeTag& other) const noexcept { return true; }
};

using ObjectTypesVariant = x3::variant<TypeTag<std::string>, TypeRef>;

struct ObjectTypeInfo : ObjectTypesVariant {
	using base_type::base_type;
	using base_type::operator=;

	bool operator==(const ObjectTypeInfo& other) const noexcept = default;
};

using SingularTypesVariant = x3::variant<TypeTag<bool>,
                                         TypeTag<int8_t>,
                                         TypeTag<uint8_t>,
                                         TypeTag<int16_t>,
                                         TypeTag<uint16_t>,
                                         TypeTag<int64_t>,
                                         TypeTag<uint64_t>,
                                         TypeTag<int32_t>,
                                         TypeTag<uint32_t>,
                                         TypeTag<float>,
                                         TypeTag<double>,
                                         TypeTag<std::string>,
                                         TypeRef>;

struct VectorTypeInfo : SingularTypesVariant {
	using base_type::base_type;
	using base_type::operator=;

	bool operator==(const VectorTypeInfo& other) const noexcept = default;
};

using TableFieldTypesVariant = x3::variant<ScalarTypeInfo, ObjectTypeInfo, VectorTypeInfo>;

struct TableFieldDecl {
	std::string name;
	TableFieldTypesVariant typeinfo;
	boost::optional<Metadata> meta;

	bool operator==(const TableFieldDecl& other) const noexcept = default;
};

struct TableDecl {
	std::string name;
	boost::optional<Metadata> meta;
	std::vector<TableFieldDecl> fields;
	bool operator==(const TableDecl& other) const noexcept = default;
};

using StructFieldTypesVariant = x3::variant<TypeTag<bool>,
                                            TypeTag<int8_t>,
                                            TypeTag<uint8_t>,
                                            TypeTag<int16_t>,
                                            TypeTag<uint16_t>,
                                            TypeTag<int64_t>,
                                            TypeTag<uint64_t>,
                                            TypeTag<int32_t>,
                                            TypeTag<uint32_t>,
                                            TypeTag<float>,
                                            TypeTag<double>,
                                            TypeRef>;

struct StructFieldDecl {
	std::string name;
	StructFieldTypesVariant typeinfo;
	boost::optional<Metadata> meta;
	bool operator==(const StructFieldDecl& other) const noexcept = default;
};

struct StructDecl {
	std::string name;
	boost::optional<Metadata> meta;
	std::vector<StructFieldDecl> fields;
	bool operator==(const StructDecl& other) const noexcept = default;
};

using EnumValVariant = x3::variant<uint64_t, int64_t>;

struct EnumValDecl {
	std::string name;
	// whether the assigned value fits in the type that backs enum is deferred to code generator
	boost::optional<EnumValVariant> value;
	bool operator==(const EnumValDecl& other) const noexcept = default;
};

using EnumTypesVariant = x3::variant<TypeTag<int8_t>,
                                     TypeTag<uint8_t>,
                                     TypeTag<int16_t>,
                                     TypeTag<uint16_t>,
                                     TypeTag<int64_t>,
                                     TypeTag<uint64_t>,
                                     TypeTag<int32_t>,
                                     TypeTag<uint32_t>>;

struct EnumDecl {
	std::string name;
	EnumTypesVariant typeinfo;
	boost::optional<Metadata> meta;
	std::vector<EnumValDecl> values;
	bool operator==(const EnumDecl& other) const noexcept = default;
};

struct UnionSubTypeDecl {
	std::string type;
	boost::optional<std::string> alias;
	bool operator==(const UnionSubTypeDecl& other) const noexcept = default;
};

struct UnionDecl {
	std::string name;
	boost::optional<Metadata> meta;
	std::vector<UnionSubTypeDecl> subtypes;
	bool operator==(const UnionDecl& other) const noexcept = default;
};

using SchemaElementsVariant = x3::variant<NamespaceDecl,
                                          TableDecl,
                                          StructDecl,
                                          EnumDecl,
                                          UnionDecl,
                                          RootTypeDecl,
                                          FileExtensionDecl,
                                          FileIdentifierDecl,
                                          AttributeDecl,
                                          RpcDecl>;

struct Schema {
	std::vector<Include> includes;
	std::vector<SchemaElementsVariant> elements;
	bool operator==(const Schema& other) const noexcept = default;
};

} // namespace ast

#endif /*FLATBUFFERS_AST_H*/
