add_library(ccontrol SHARED)

target_include_directories(ccontrol PRIVATE
    ${ANTLR4_INCLUDE_DIR}
)

target_sources(ccontrol PRIVATE
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
    log
    parser
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
            parser
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
    testAntlr4GeneratedIR
    testCControl
    testUserQueryType
)
