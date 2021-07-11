add_library(qana OBJECT)
add_dependencies(qana proto)

target_sources(qana PRIVATE
    AggregatePlugin.cc
    AggregatePlugin.h
    AnalysisError.h
    CheckAggregation.h
    ColumnVertexMap.cc
    ColumnVertexMap.h
    DuplSelectExprPlugin.cc
    DuplSelectExprPlugin.h
    InvalidTableError.h
    MatchTablePlugin.cc
    MatchTablePlugin.h
    PluginNotFoundError.h
    PostPlugin.cc
    PostPlugin.h
    QservRestrictorPlugin.cc
    QservRestrictorPlugin.h
    QueryMapping.cc
    QueryMapping.h
    QueryNotEvaluableError.h
    QueryPlugin.h
    RelationGraph.cc
    RelationGraph.h
    ScanTablePlugin.cc
    ScanTablePlugin.h
    TableInfo.cc
    TableInfo.h
    TableInfoPool.cc
    TableInfoPool.h
    TablePlugin.cc
    TablePlugin.h
    WherePlugin.cc
    WherePlugin.h
)

target_link_libraries(qana PUBLIC
    log
)

FUNCTION(QANA_TESTS)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            ccontrol
            czar
            parser
            qana
            qdisp
            qproc
            query
            qserv_css
            qserv_meta
            rproc
            Boost::unit_test_framework
            Threads::Threads
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

qana_tests(
    testDuplSelectExprPlugin
    testPlugins
    testTablePlugin
)
