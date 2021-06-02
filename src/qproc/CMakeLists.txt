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


add_executable(testChunkSpec testChunkSpec.cc)
target_link_libraries(testChunkSpec
    ccontrol
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
add_test(NAME testChunkSpec COMMAND testChunkSpec)


add_executable(testGeomAdapter testGeomAdapter.cc)
target_link_libraries(testGeomAdapter
    ccontrol
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
add_test(NAME testGeomAdapter COMMAND testGeomAdapter)


add_executable(testIndexMap testIndexMap.cc)
target_link_libraries(testIndexMap
    ccontrol
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
add_test(NAME testIndexMap COMMAND testIndexMap)


add_executable(testQueryAnaAggregation testQueryAnaAggregation.cc)
target_sources(qproc PRIVATE
    ../tests/QueryAnaHelper.cc
    ../tests/QueryAnaHelper.h
)
target_link_libraries(testQueryAnaAggregation
    ccontrol
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
add_test(NAME testQueryAnaAggregation COMMAND testQueryAnaAggregation)


add_executable(testQueryAnaBetween testQueryAnaBetween.cc)
target_link_libraries(testQueryAnaBetween
    ccontrol
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
add_test(NAME testQueryAnaBetween COMMAND testQueryAnaBetween)


add_executable(testQueryAnaDuplSelectExpr testQueryAnaDuplSelectExpr.cc)
target_link_libraries(testQueryAnaDuplSelectExpr
    ccontrol
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
add_test(NAME testQueryAnaDuplSelectExpr COMMAND testQueryAnaDuplSelectExpr)


add_executable(testQueryAnaGeneral testQueryAnaGeneral.cc)
target_link_libraries(testQueryAnaGeneral
    ccontrol
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
add_test(NAME testQueryAnaGeneral COMMAND testQueryAnaGeneral)


add_executable(testQueryAnaIn testQueryAnaIn.cc)
target_link_libraries(testQueryAnaIn
    ccontrol
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
add_test(NAME testQueryAnaIn COMMAND testQueryAnaIn)


add_executable(testQueryAnaOrderBy testQueryAnaOrderBy.cc)
target_link_libraries(testQueryAnaOrderBy
    ccontrol
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
add_test(NAME testQueryAnaOrderBy COMMAND testQueryAnaOrderBy)
