add_library(ccontrol SHARED)

target_sources(ccontrol PRIVATE
    HyriseAdapter.cc
    HyriseDiagnostics.cc
    MergingHandler.cc
    QueryState.cc
    UserQueryAsyncResult.cc
    UserQueryExplain.cc
    UserQueryFactory.cc
    UserQueryProcessList.cc
    UserQueryQueries.cc
    UserQuerySelectCountStar.cc
    UserQueryResultDelete.cc
    UserQueryResources.cc
    UserQuerySelect.cc
    UserQuerySet.cc
    UserQueryType.cc
)

target_link_libraries(ccontrol PUBLIC
    boost_regex
    cconfig
    css
    global
    hyrise_sql_parser::sqlparser
    log
    sphgeom
)

install(TARGETS ccontrol)

FUNCTION(ccontrol_tests)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            cconfig
            ccontrol
            czar
            global
            qana
            qdisp
            qproc
            css
            qmeta
            query
            rproc
            Boost::unit_test_framework
            Threads::Threads
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

ccontrol_tests(
    testCControl
    testHyriseGeneratedIR
    testParserCorpus
    testUserQueryType
)

target_compile_definitions(testParserCorpus PRIVATE
    QSERV_PARSER_CORPUS_DIR="${CMAKE_CURRENT_SOURCE_DIR}/testdata/parser-corpus"
)
