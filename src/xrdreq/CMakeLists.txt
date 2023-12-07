add_library(xrdreq OBJECT)
add_dependencies(xrdreq proto)

target_sources(xrdreq PRIVATE
    QservRequest.cc
    QueryManagementAction.cc
    QueryManagementRequest.cc
)

target_include_directories(xrdreq PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(xrdreq PUBLIC
    log
    proto
    protobuf
    XrdSsiLib
    XrdCl
)

FUNCTION(XRDREQ_UTILS)
    FOREACH(UTIL IN ITEMS ${ARGV})
        add_executable(${UTIL})
        target_sources(${UTIL} PRIVATE ${UTIL}.cc)
        target_include_directories(${UTIL} PRIVATE ${XROOTD_INCLUDE_DIRS})
        target_link_libraries(${UTIL} PRIVATE
            crypto
            pthread
            proto
            util
            global
            xrdreq
        )
        install(TARGETS ${UTIL})
    ENDFOREACH()
ENDFUNCTION()

xrdreq_utils(
    qserv-query-management
)

install(
    TARGETS xrdreq
)
