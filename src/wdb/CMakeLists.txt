add_library(wdb SHARED)

target_sources(wdb PRIVATE
    ChunkResource.cc
    QueryRunner.cc
    SQLBackend.cc
)


target_link_libraries(wdb PUBLIC
    log
)

install(
	TARGETS wdb
)

install(
    TARGETS wdb
)

FUNCTION(wdb_tests)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            crypto
            global
            http
            mysql
            protojson
            sql
            util
            wbase
            wconfig
            wcontrol
            wdb
            wpublish
            wsched
            Boost::unit_test_framework
            Threads::Threads
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

wdb_tests(
    testChunkResource
)

