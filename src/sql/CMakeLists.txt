add_library(sql OBJECT)

target_sources(sql PRIVATE
    MockSql.cc
    MockSql.h
    MySqlConnection.cc
    MySqlConnection.h
    Schema.h
    SqlBulkInsert.cc
    SqlBulkInsert.h
    SqlConfig.h
    SqlConnection.h
    SqlConnectionFactory.cc
    SqlConnectionFactory.h
    SqlErrorObject.cc
    SqlErrorObject.h
    SqlException.h
    SqlFragmenter.cc
    SqlFragmenter.h
    SqlResults.cc
    SqlResults.h
    SqlTransaction.cc
    SqlTransaction.h
    SqlTransactionScope.cc
    SqlTransactionScope.h
    statement.cc
    statement.h
)

target_link_libraries(sql PUBLIC
    log
)
