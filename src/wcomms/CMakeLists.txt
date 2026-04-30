add_library(wcomms SHARED)

target_sources(wcomms PRIVATE
    HttpModule.cc
    HttpMonitorModule.cc
    HttpReplicaMgtModule.cc
    HttpSvc.cc
    HttpWorkerCzarModule.cc
)

target_link_libraries(wcomms PUBLIC
    log
)

install(TARGETS wcomms)
