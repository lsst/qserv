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

FUNCTION(WDB_TESTS)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_include_directories(${TEST} PRIVATE
            ${XROOTD_INCLUDE_DIRS}
            ${XROOTD_INCLUDE_DIRS}/private
        )
        target_link_libraries(${TEST} PUBLIC
            crypto
            xrdsvc
            Boost::unit_test_framework
            Threads::Threads    
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

wdb_tests(
    testChunkResource
    testQueryRunner
    testQuerySql
)
