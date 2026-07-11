add_library(cconfig SHARED)

target_sources(cconfig PRIVATE
    CzarConfig.cc
    DataManagementEvent.cc
    EventService.cc
)

target_include_directories(cconfig PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(cconfig PUBLIC
    log
    XrdSsiLib
    boost_system
    util
)

install(
    TARGETS cconfig
)

FUNCTION(cconfig_tests)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            cconfig
            Boost::unit_test_framework
            Threads::Threads
            mysql
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

cconfig_tests(
    testEventService
)
