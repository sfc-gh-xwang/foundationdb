#ifndef FLATBUFFERS_ADAPTED_AST_H
#define FLATBUFFERS_ADAPTED_AST_H

#include "apigen/AST.h"
#include <boost/fusion/include/adapt_struct.hpp>

BOOST_FUSION_ADAPT_STRUCT(ast::file_identifier_decl, id)
BOOST_FUSION_ADAPT_STRUCT(ast::file_extension_decl, ext)
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
BOOST_FUSION_ADAPT_STRUCT(ast::root_type_decl, name)
BOOST_FUSION_ADAPT_STRUCT(ast::rpc_method_decl, name, request_type, response_type, meta)
BOOST_FUSION_ADAPT_STRUCT(ast::rpc_decl, name, methods)
BOOST_FUSION_ADAPT_STRUCT(ast::schema, includes, elements)

BOOST_FUSION_ADAPT_TPL_STRUCT((T), (ast::scalar_type)(T), (boost::optional<T>, default_))

#endif /*FLATBUFFERS_ADAPTED_AST_H*/
