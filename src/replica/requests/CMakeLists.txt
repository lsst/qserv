add_library(replica_requests OBJECT)
add_dependencies(replica_requests replica_proto)
target_sources(replica_requests PRIVATE
    http/HttpCreateReplicaRequest.cc
    http/HttpDeleteReplicaRequest.cc
    http/HttpDirectorIndexRequest.cc
    http/HttpEchoRequest.cc
    http/HttpFindAllRequest.cc
    http/HttpFindRequest.cc
    http/HttpMessenger.cc
    http/HttpMessengerConnector.cc
    http/HttpMessengerRequest.cc
    http/HttpRequest.cc
    http/HttpSqlAlterTablesRequest.cc
    http/HttpSqlCreateDbRequest.cc
    http/HttpSqlCreateIndexesRequest.cc
    http/HttpSqlCreateTableRequest.cc
    http/HttpSqlCreateTablesRequest.cc
    http/HttpSqlDeleteDbRequest.cc
    http/HttpSqlDeleteTablePartitionRequest.cc
    http/HttpSqlDeleteTableRequest.cc
    http/HttpSqlDisableDbRequest.cc
    http/HttpSqlDropIndexesRequest.cc
    http/HttpSqlEnableDbRequest.cc
    http/HttpSqlGetIndexesRequest.cc
    http/HttpSqlGrantAccessRequest.cc
    http/HttpSqlQueryRequest.cc
    http/HttpSqlRemoveTablePartitionsRequest.cc
    http/HttpSqlRequest.cc
    http/HttpSqlRowStatsRequest.cc
    http/HttpStatusRequest.cc
    http/HttpStopRequest.cc
    http/HttpSvcMgtRequest.cc
    http/HttpSvcMgtRequestBase.cc
    RequestI.cc
    SqlResultSet.cc
)
target_link_libraries(replica_requests PUBLIC
    log
)
