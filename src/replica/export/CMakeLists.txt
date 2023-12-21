add_library(replica_export OBJECT)
add_dependencies(replica_export replica_proto)
target_sources(replica_export PRIVATE
    ExportClient.cc
    ExportServer.cc
    ExportServerConnection.cc
)
target_link_libraries(replica_export PUBLIC
    log
)
