add_library(replica_services OBJECT)
target_sources(replica_services PRIVATE
    ChunkLocker.cc
    DatabaseServices.cc
    DatabaseServicesMySQL.cc
    DatabaseServicesPool.cc
    ServiceProvider.cc
)
target_link_libraries(replica_services PUBLIC
    log
)
