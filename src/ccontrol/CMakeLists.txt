find_package(antlr4-runtime REQUIRED)

add_library(ccontrol OBJECT)
add_dependencies(ccontrol proto)

target_include_directories(ccontrol PRIVATE
    ${ANTLR4_INCLUDE_DIR}
)

target_sources(ccontrol PRIVATE
    A4UserQueryFactory.cc
    A4UserQueryFactory.h
    Callback.h
    ConfigError.h
    ConfigMap.h
    MergingHandler.cc
    MergingHandler.h
    MissingUserQuery.h
    msgCode.h
    ParseAdaptersCBH.h
    ParseAdapters.cc
    ParseAdapters.h
    ParseHelpers.h
    ParseListener.cc
    ParseListener.h
    ParseRunner.cc
    ParseRunner.h
    QueryState.cc
    QueryState.h
    SessionManager.h
    TmpTableName.h
    UserQueryAsyncResult.cc
    UserQueryAsyncResult.h
    UserQueryDrop.cc
    UserQueryDrop.h
    UserQueryError.h
    UserQueryFactory.cc
    UserQueryFactory.h
    UserQueryFlushChunksCache.cc
    UserQueryFlushChunksCache.h
    UserQuery.h
    UserQueryInvalid.h
    UserQueryProcessList.cc
    UserQueryProcessList.h
    UserQueryQservManager.cc
    UserQueryQservManager.h
    UserQueryResources.cc
    UserQueryResources.h
    UserQuerySelect.cc
    UserQuerySelect.h
    UserQueryType.cc
    UserQueryType.h
)

target_link_libraries(ccontrol PUBLIC
    boost_regex
    log
    parser
    sphgeom
)


add_executable(testAntlr4GeneratedIR testAntlr4GeneratedIR.cc)
target_link_libraries(testAntlr4GeneratedIR
    ccontrol
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
add_test(NAME testAntlr4GeneratedIR COMMAND testAntlr4GeneratedIR)


add_executable(testCControl testCControl.cc)
target_link_libraries(testCControl
    ccontrol
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
add_test(NAME testCControl COMMAND testCControl)


add_executable(testUserQueryType testUserQueryType.cc)
target_link_libraries(testUserQueryType
    ccontrol
    qdisp
    parser
    qana
    qproc
    qserv_css
    qserv_meta
    query
    rproc
    Boost::unit_test_framework
    Threads::Threads
)
add_test(NAME testUserQueryType COMMAND testUserQueryType)
