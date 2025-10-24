add_library(replica_worker OBJECT)
add_dependencies(replica_worker replica_proto)
target_sources(replica_worker PRIVATE
    FileClient.cc
    FileServer.cc
    FileServerConnection.cc
    WorkerDeleteRequest.cc
    WorkerDirectorIndexRequest.cc
    WorkerEchoRequest.cc
    WorkerFindAllRequest.cc
    WorkerFindRequest.cc
    WorkerProcessor.cc
    WorkerProcessorThread.cc
    WorkerReplicationRequest.cc
    WorkerRequest.cc
    WorkerServer.cc
    WorkerServerConnection.cc
    WorkerSqlRequest.cc
    WorkerUtils.cc
)
target_link_libraries(replica_worker PUBLIC
    log
)
