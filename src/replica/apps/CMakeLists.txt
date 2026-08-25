add_library(replica_apps OBJECT)

add_dependencies(replica_apps replica_proto)

target_include_directories(replica_apps PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_sources(replica_apps PRIVATE
    Application.cc
    ApplicationColl.cc
    ApplicationTypes.cc
    TestAwsS3App.cc
    MasterControllerHttpApp.cc
    RegistryHttpApp.cc
    WorkerApp.cc
)

target_link_libraries(replica_apps PUBLIC
    log
)
