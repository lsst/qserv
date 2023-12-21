add_library(replica_mysql OBJECT)
add_dependencies(replica_mysql replica_proto)
target_sources(replica_mysql PRIVATE
    DatabaseMySQL.cc
    DatabaseMySQLGenerator.cc
    DatabaseMySQLRow.cc
    DatabaseMySQLTypes.cc
    DatabaseMySQLUtils.cc
)
target_link_libraries(replica_mysql PUBLIC
    log
)
