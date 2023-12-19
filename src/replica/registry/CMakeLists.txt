add_library(replica_registry OBJECT)
target_sources(replica_registry PRIVATE
    Registry.cc
    RegistryHttpSvc.cc
    RegistryHttpSvcMod.cc
    RegistryServices.cc
)
target_link_libraries(replica_registry PUBLIC
    log
)
