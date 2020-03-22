find_package(XRootD REQUIRED)

add_library(wcontrol OBJECT)
add_dependencies(wcontrol proto)

target_sources(wcontrol PRIVATE
    Foreman.cc
    Foreman.h
    SqlConnMgr.cc
    SqlConnMgr.h
    TransmitMgr.cc
    TransmitMgr.h
)

target_include_directories(wcontrol PRIVATE
    ${XROOTD_INCLUDE_DIRS}
    ${XROOTD_INCLUDE_DIRS}/private
)

target_link_libraries(wcontrol PUBLIC
    log
    XrdSsiLib
)
