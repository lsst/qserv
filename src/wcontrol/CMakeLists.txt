add_library(wcontrol OBJECT)
add_dependencies(wcontrol proto)

target_sources(wcontrol PRIVATE
    Foreman.cc
    SqlConnMgr.cc
    TransmitMgr.cc
    WorkerStats.cc
)

target_include_directories(wcontrol PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(wcontrol PUBLIC
    log
    XrdSsiLib
    qhttp
)
