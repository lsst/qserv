add_library(replica_qserv OBJECT)
target_include_directories(replica_qserv PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)
target_sources(replica_qserv PRIVATE
    AddReplicaQservMgtRequest.cc
    GetReplicasQservMgtRequest.cc
    GetDbStatusQservMgtRequest.cc
    GetConfigQservCzarMgtRequest.cc
    GetConfigQservMgtRequest.cc
    GetQueryProgressQservCzarMgtRequest.cc
    GetResultFilesQservMgtRequest.cc
    GetStatusQservCzarMgtRequest.cc
    GetStatusQservMgtRequest.cc
    QservCzarMgtRequest.cc
    QservMgtRequest.cc
    QservMgtServices.cc
    QservWorkerMgtRequest.cc
    RemoveReplicaQservMgtRequest.cc
    SetReplicasQservMgtRequest.cc
    TestEchoQservMgtRequest.cc
)
target_link_libraries(replica_qserv PUBLIC
    log
)
