find_package(XRootD REQUIRED)

add_library(wdb OBJECT)
add_dependencies(wdb proto)

target_sources(wdb PRIVATE
    ChunkResource.cc
    ChunkResource.h
    QueryRunner.cc
    QueryRunner.h
    QuerySql_Batch.h
    QuerySql.cc
    QuerySql.h
    SQLBackend.cc
    SQLBackend.h
)

target_include_directories(wdb PRIVATE
    ${XROOTD_INCLUDE_DIRS}
    ${XROOTD_INCLUDE_DIRS}/private
)

target_link_libraries(wdb PUBLIC
    log
    XrdSsiLib
)


add_executable(testChunkResource testChunkResource.cc)
target_link_libraries(testChunkResource PRIVATE
    crypto
    global
    mysql
    proto
    util
    wbase
    ssl
    xrdsvc
    Boost::unit_test_framework
    Threads::Threads
)
add_test(NAME testChunkResource COMMAND testChunkResource)


add_executable(testQueryRunner testQueryRunner.cc)
target_include_directories(testQueryRunner PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)
target_link_libraries(testQueryRunner PRIVATE
    crypto
    global
    mysql
    proto
    util
    wbase
    ssl
    xrdsvc
    Boost::unit_test_framework
    Threads::Threads
)
add_test(NAME testQueryRunner COMMAND testQueryRunner)


add_executable(testQuerySql testQuerySql.cc)
target_link_libraries(testQuerySql PRIVATE
    crypto
    global
    mysql
    proto
    util
    wbase
    ssl
    xrdsvc
    Boost::unit_test_framework
    Threads::Threads
)
add_test(NAME testQuerySql COMMAND testQuerySql)
