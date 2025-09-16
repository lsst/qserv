add_library(wdb SHARED)
add_dependencies(wdb proto)

target_sources(wdb PRIVATE
    QueryRunner.cc
    QuerySql.cc
)

target_include_directories(wdb PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(wdb PUBLIC
    log
    XrdSsiLib
)

install(
    TARGETS wdb
)

FUNCTION(wdb_tests)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_include_directories(${TEST} PRIVATE
            ${XROOTD_INCLUDE_DIRS}
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
    testQueryRunner
    testQuerySql
)

set_tests_properties(testQueryRunner PROPERTIES WILL_FAIL 1)
