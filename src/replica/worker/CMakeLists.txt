add_library(replica_worker OBJECT)
add_dependencies(replica_worker replica_proto)
target_sources(replica_worker PRIVATE
    FileClient.cc
    FileServer.cc
    FileServerConnection.cc
    WorkerDeleteRequest.cc
    WorkerDirectorIndexRequest.cc
    WorkerEchoRequest.cc
    WorkerExporterHttpSvc.cc
    WorkerExporterHttpSvcMod.cc
    WorkerFindAllRequest.cc
    WorkerFindRequest.cc
    WorkerProcessor.cc
    WorkerProcessorThread.cc
    WorkerReplicationRequest.cc
    WorkerRequest.cc
    WorkerServer.cc
    WorkerServerConnection.cc
    http/WorkerCreateReplicaHttpRequest.cc
    http/WorkerDeleteReplicaHttpRequest.cc
    http/WorkerDirectorIndexHttpRequest.cc
    http/WorkerEchoHttpRequest.cc
    http/WorkerFindAllReplicasHttpRequest.cc
    http/WorkerFindReplicaHttpRequest.cc
    http/WorkerHttpProcessor.cc
    http/WorkerHttpProcessorThread.cc
    http/WorkerHttpRequest.cc
    http/WorkerHttpSvc.cc
    http/WorkerHttpSvcMod.cc
    http/WorkerSqlHttpRequest.cc
    WorkerSqlRequest.cc
    WorkerUtils.cc
)
target_link_libraries(replica_worker PUBLIC
    log
    sphgeom
)
