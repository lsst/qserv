add_library(global SHARED)

target_sources(global PRIVATE
    DbTable.cc
    debugUtil.cc
    ResourceUnit.cc
    sqltoken.cc
    stringUtil.cc
)

install(
	TARGETS global
)

target_link_libraries(global PUBLIC
    log
)

FUNCTION(global_tests)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            global
            Boost::unit_test_framework
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

global_tests(
    testStringUtil
)

