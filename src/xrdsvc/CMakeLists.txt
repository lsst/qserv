add_library(xrdsvc SHARED)
add_dependencies(xrdsvc proto)

target_sources(xrdsvc PRIVATE
    ChannelStream.cc
    HttpModule.cc
    HttpMonitorModule.cc
    HttpReplicaMgtModule.cc
    HttpWorkerCzarModule.cc
    HttpSvc.cc
    SsiProvider.cc
    SsiRequest.cc
    SsiService.cc
    StreamBuffer.cc
)

target_include_directories(xrdsvc PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(xrdsvc PUBLIC
    boost_filesystem
    boost_system
    global
    log
    memman
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
