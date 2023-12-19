add_library(replica_worker OBJECT)
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
    WorkerRequestFactory.cc
    WorkerServer.cc
    WorkerServerConnection.cc
    WorkerSqlRequest.cc
)
target_link_libraries(replica_worker PUBLIC
    log
)
