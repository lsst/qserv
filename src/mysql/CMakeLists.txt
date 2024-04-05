add_library(mysql SHARED)

target_sources(mysql PRIVATE
    CsvBuffer.cc
    CsvMemDisk.cc
    LocalInfile.cc
    MySqlConfig.cc
    MySqlConnection.cc
    MySqlUtils.cc
    SchemaFactory.cc
)

target_link_libraries(mysql PUBLIC
    log
    mysqlclient_r
    util
)

install(
    TARGETS mysql
)

add_executable(testLocalInfile testLocalInfile.cc)

target_link_libraries(testLocalInfile
    mysql
    util
    Boost::unit_test_framework
)

FUNCTION(mysql_tests)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            mysql
            Boost::unit_test_framework
            Threads::Threads
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

mysql_tests(
    testMySqlUtils
    testCsvBuffer
)

# Even though the test is commented out, leave this so it's findable by searching for
# text similar to other WILL_FAIL tests.
# WILL_FAIL does not work, I think because it exits with an error "Failed to connect to MySQL: Error"
# add_test(NAME testLocalInfile COMMAND testLocalInfile)
# set_tests_properties(testLocalInfile PROPERTIES WILL_FAIL 1)
