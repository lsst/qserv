add_library(sql SHARED)

target_sources(sql PRIVATE
    MockSql.cc
    MySqlConnection.cc
    SqlBulkInsert.cc
    SqlConnectionFactory.cc
    SqlErrorObject.cc
    SqlFragmenter.cc
    SqlResults.cc
    SqlTransaction.cc
    SqlTransactionScope.cc
    statement.cc
)

install(
	TARGETS sql
)

target_link_libraries(sql PUBLIC
    log
)

FUNCTION(sql_tests)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            sql
            global
            util
            crypto
            mysql
            Boost::unit_test_framework
            Threads::Threads
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

sql_tests(
    # don't add tests; need fix DM-30562: fix Qserv unit tests that prompt for an sql connection
    # testSqlConnection
    # testSqlTransaction
)
