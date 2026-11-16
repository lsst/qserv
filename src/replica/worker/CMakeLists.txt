add_library(replica_worker OBJECT)
add_dependencies(replica_worker replica_proto)
target_sources(replica_worker PRIVATE
    FileClient.cc
    FileServer.cc
    FileServerConnection.cc
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
    WorkerExporterHttpSvc.cc
    WorkerExporterHttpSvcMod.cc
    WorkerUtils.cc
)
target_link_libraries(replica_worker PUBLIC
    log
    sphgeom
)
