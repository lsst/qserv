add_library(ccontrol SHARED)
add_dependencies(ccontrol proto)

target_include_directories(ccontrol PRIVATE
    ${ANTLR4_INCLUDE_DIR}
    ${XROOTD_INCLUDE_DIRS}
)

target_sources(ccontrol PRIVATE
    HyriseAdapter.cc
    HyriseDiagnostics.cc
    MergingHandler.cc
    ParseAdapters.cc
    ParseListener.cc
    ParseRunner.cc
    QueryState.cc
    UserQueryAsyncResult.cc
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
    parser
    replica
    sphgeom
    xrdreq
    XrdCl
)

if(QSERV_USE_HYRISE_SQL_PARSER)
    target_compile_definitions(ccontrol PUBLIC QSERV_USE_HYRISE_SQL_PARSER)
endif()

install(TARGETS ccontrol)

FUNCTION(ccontrol_tests)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            cconfig
            ccontrol
            czar
            parser
            qana
            qdisp
            qproc
            css
            qmeta
            query
            rproc
            xrdreq
            Boost::unit_test_framework
            Threads::Threads
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

ccontrol_tests(
    testAntlr4GeneratedIR
    testCControl
    testUserQueryType
)
