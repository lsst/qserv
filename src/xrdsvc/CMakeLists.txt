add_library(xrdsvc SHARED)
add_dependencies(xrdsvc proto)

target_sources(qserv_xrdsvc PRIVATE
    HttpModule.cc
    HttpMonitorModule.cc
    HttpReplicaMgtModule.cc
    HttpWorkerCzarModule.cc
    HttpSvc.cc
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
    log
    mysql
    http
    sql
    wbase
    wconfig
    wcontrol
    wpublish
    wsched
    XrdSsiLib
)

install(
    TARGETS xrdsvc
)
