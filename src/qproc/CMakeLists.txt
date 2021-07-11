add_library(qproc OBJECT)
add_dependencies(qproc proto)

target_sources(qproc PRIVATE
    ChunkQuerySpec.cc
    ChunkQuerySpec.h
    ChunkSpec.cc
    ChunkSpec.h
    DatabaseModels.cc
    DatabaseModels.h
    fakeGeometry.h
    geomAdapter.h
    IndexMap.cc
    IndexMap.h
    QueryProcessingBug.h
    QueryProcessingError.h
    QuerySession.cc
    QuerySession.h
    SecondaryIndex.cc
    SecondaryIndex.h
    TaskMsgFactory.cc
    TaskMsgFactory.h
)

target_link_libraries(qproc PRIVATE
    log
    sphgeom
)

FUNCTION(QPROC_TESTS)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc ../tests/QueryAnaHelper.cc)
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

qproc_tests(
    testChunkSpec
    testGeomAdapter
    testIndexMap
    testQueryAnaAggregation
    testQueryAnaBetween
    testQueryAnaDuplSelectExpr
    testQueryAnaGeneral
    testQueryAnaIn
    testQueryAnaOrderBy
)
