add_library(css SHARED)

target_sources(css PRIVATE
    CssAccess.cc
    CssConfig.cc
    DbInterfaceMySql.cc
    EmptyChunks.cc
    KvInterfaceImplMem.cc
    KvInterfaceImplMySql.cc
)

target_link_libraries(css PUBLIC
    mysql
    sql
    log
    util
)

install(TARGETS css)
install(DIRECTORY schema/ DESTINATION ${CMAKE_INSTALL_PREFIX}/qserv/smig/css/schema/)

FUNCTION(css_tests)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            css
            Boost::unit_test_framework
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

css_tests(
    testEmptyChunks
    testKvInterfaceImpl
#    testKvInterfaceImplMySql # crashes because of a missing file.
    testCssAccess
)

# Even though this and the test is commented out, leave this so it's findable by
# searching for text similar to other WILL_FAIL tests.
# set_tests_properties(testKvInterfaceImplMySql PROPERTIES WILL_FAIL 1)

#-----------------------------------------------------------------------------

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/python/ DESTINATION ${CMAKE_INSTALL_PREFIX}/python/lsst/qserv/css)
