antlr_target(QSMySqlLexer QSMySqlLexer.g4 LEXER
    OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

antlr_target(QSMySqlParser QSMySqlParser.g4 PARSER LISTENER
    DEPENDS ${ANTLR_QSMySqlLexer_OUTPUTS}
    OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMPILE_FLAGS -lib ${CMAKE_CURRENT_BINARY_DIR}
)

add_library(parser OBJECT)

target_include_directories(parser PRIVATE 
    ${ANTLR4_INCLUDE_DIR}
)

target_compile_options(parser PRIVATE
    -fno-char8_t -Wno-deprecated
)

target_sources(parser PRIVATE
    ParseException.cc
    ${ANTLR_QSMySqlLexer_CXX_OUTPUTS}
    ${ANTLR_QSMySqlParser_CXX_OUTPUTS}
)

target_link_libraries(parser PUBLIC
    log
    antlr4-runtime
)
