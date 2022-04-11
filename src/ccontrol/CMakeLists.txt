add_library(ccontrol OBJECT)
add_dependencies(ccontrol proto)

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
    UserQueryDrop.cc
    UserQueryFactory.cc
    UserQueryFlushChunksCache.cc
    UserQueryProcessList.cc
    UserQuerySelectCountStar.cc
    UserQueryQservManager.cc
    UserQueryResources.cc
    UserQuerySelect.cc
    UserQueryType.cc
)

target_link_libraries(ccontrol PUBLIC
    boost_regex
    log
    parser
    sphgeom
)

FUNCTION(ccontrol_tests)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            ccontrol
            czar
            parser
            qana
            qdisp
            qproc
            qserv_css
            qserv_meta
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
