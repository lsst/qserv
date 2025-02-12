add_library(xrdsvc SHARED)

target_sources(xrdsvc PRIVATE
    HttpModule.cc
    HttpMonitorModule.cc
    HttpReplicaMgtModule.cc
    HttpSvc.cc
    HttpWorkerCzarModule.cc
    SsiProvider.cc
    SsiService.cc
)

target_include_directories(xrdsvc PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(xrdsvc PUBLIC
    boost_filesystem
    boost_system
    global
    http
    log
    memman
    mysql
    protojson
    sql
    wbase
    wconfig
    wcontrol
    wdb
    wpublish
    wsched
    XrdSsiLib
)

install(
    TARGETS xrdsvc
)
