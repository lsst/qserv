add_library(wpublish OBJECT)
add_dependencies(wpublish proto)

target_sources(wpublish PRIVATE
    AddChunkGroupCommand.cc
    ChunkGroupQservRequest.cc
    ChunkInventory.cc
    ChunkListCommand.cc
    ChunkListQservRequest.cc
    GetChunkListCommand.cc
    GetChunkListQservRequest.cc
    GetStatusCommand.cc
    GetStatusQservRequest.cc
    QservRequest.cc
    QueriesAndChunks.cc
    QueryManagementAction.cc
    QueryManagementRequest.cc
    RemoveChunkGroupCommand.cc
    ResourceMonitor.cc
    SetChunkListCommand.cc
    SetChunkListQservRequest.cc
    TestEchoCommand.cc
    TestEchoQservRequest.cc
)

target_include_directories(wpublish PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(wpublish PUBLIC
    log
    proto
    protobuf
    XrdSsiLib
    XrdCl
)

add_executable(testChunkInventory testChunkInventory.cc)

target_link_libraries(testChunkInventory PUBLIC
    xrdsvc
    Boost::unit_test_framework
    Threads::Threads
)

add_test(NAME testChunkInventory COMMAND testChunkInventory)


FUNCTION(WPUBLISH_UTILS)
    FOREACH(UTIL IN ITEMS ${ARGV})
        add_executable(${UTIL})
        target_sources(${UTIL} PRIVATE ${UTIL}.cc)
        target_include_directories(${UTIL} PRIVATE ${XROOTD_INCLUDE_DIRS})
        target_link_libraries(${UTIL} PRIVATE
            crypto
            pthread
            util
            wpublish
            xrdsvc
        )
        install(TARGETS ${UTIL})
    ENDFOREACH()
ENDFUNCTION()

wpublish_utils(
    qserv-query-management
)

install(
    TARGETS wpublish
)
