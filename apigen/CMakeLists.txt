set(APIGEN_PARSER_SRCS
  include/apigen/AST.h
  include/apigen/AdaptedAST.h
  include/apigen/Config.h
  include/apigen/Parser.h
  include/apigen/ParserDef.h
)

if(OPEN_FOR_IDE)
    add_library(parser_test OBJECT ParserTest.cpp)
else()
    add_executable(parser_test ParserTest.cpp)
endif()

target_link_libraries(parser_test PRIVATE boost_target)
target_include_directories(parser_test PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
