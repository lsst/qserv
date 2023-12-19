add_library(replica_util OBJECT)
target_sources(replica_util PRIVATE
    ChunkNumber.cc
    ChunkedTable.cc
    Common.cc
    Csv.cc
    FileUtils.cc
    HttpSvc.cc
    Mutex.cc
    NamedMutexRegistry.cc
    Performance.cc
    ProtocolBuffer.cc
    ReplicaInfo.cc
    SqlSchemaUtils.cc
    SuccessRateGenerator.cc
)
target_link_libraries(replica_util PUBLIC
    log
    sphgeom
)
