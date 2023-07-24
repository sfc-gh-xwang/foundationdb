#ifndef FLATBUFFERS_ADAPTED_AST_H
#define FLATBUFFERS_ADAPTED_AST_H

#include "apigen/AST.h"
#include <boost/fusion/include/adapt_struct.hpp>

BOOST_FUSION_ADAPT_STRUCT(ast::FileIdentifierDecl, id)
BOOST_FUSION_ADAPT_STRUCT(ast::FileExtensionDecl, ext)
BOOST_FUSION_ADAPT_STRUCT(ast::Include, filename)
BOOST_FUSION_ADAPT_STRUCT(ast::NamespaceDecl, ns)
BOOST_FUSION_ADAPT_STRUCT(ast::AttributeDecl, name)
BOOST_FUSION_ADAPT_STRUCT(ast::TypeRef, name, default_)
BOOST_FUSION_ADAPT_STRUCT(ast::TableFieldDecl, name, typeinfo, meta)
BOOST_FUSION_ADAPT_STRUCT(ast::TableDecl, name, meta, fields)
BOOST_FUSION_ADAPT_STRUCT(ast::StructFieldDecl, name, typeinfo, meta)
BOOST_FUSION_ADAPT_STRUCT(ast::StructDecl, name, meta, fields)
BOOST_FUSION_ADAPT_STRUCT(ast::EnumValDecl, name, value)
BOOST_FUSION_ADAPT_STRUCT(ast::EnumDecl, name, typeinfo, meta, values)
BOOST_FUSION_ADAPT_STRUCT(ast::UnionSubTypeDecl, alias, type) // member order reversed to accommodate the parse order
BOOST_FUSION_ADAPT_STRUCT(ast::UnionDecl, name, meta, subtypes)
BOOST_FUSION_ADAPT_STRUCT(ast::RootTypeDecl, name)
BOOST_FUSION_ADAPT_STRUCT(ast::RpcMethodDecl, name, requestType, responseType, meta)
BOOST_FUSION_ADAPT_STRUCT(ast::RpcDecl, name, methods)
BOOST_FUSION_ADAPT_STRUCT(ast::Schema, includes, elements)

BOOST_FUSION_ADAPT_TPL_STRUCT((T), (ast::ScalarType)(T), (boost::optional<T>, default_))

#endif /*FLATBUFFERS_ADAPTED_AST_H*/
