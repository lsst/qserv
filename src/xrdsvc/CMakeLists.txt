find_package(XRootD REQUIRED)

add_library(qserv_xrdsvc OBJECT)
add_dependencies(qserv_xrdsvc proto)

target_sources(qserv_xrdsvc PRIVATE
    ChannelStream.cc
    ChannelStream.h
    SsiProvider.cc
    SsiProvider.h
    SsiRequest.cc
    SsiRequest.h
    SsiService.cc
    SsiService.h
    StreamBuffer.cc
    StreamBuffer.h
    XrdName.h
)

target_include_directories(qserv_xrdsvc PRIVATE
    ${XROOTD_INCLUDE_DIRS}
    ${XROOTD_INCLUDE_DIRS}/private
)

target_link_libraries(qserv_xrdsvc PUBLIC
    log
    XrdSsiLib
)
