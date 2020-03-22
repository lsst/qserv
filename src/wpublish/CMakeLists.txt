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
    RemoveChunkGroupCommand.cc
    ResourceMonitor.cc
    SetChunkListCommand.cc
    SetChunkListQservRequest.cc
    TestEchoCommand.cc
    TestEchoQservRequest.cc
)

target_include_directories(wpublish PRIVATE
    ${XROOTD_INCLUDE_DIRS}
    ${XROOTD_INCLUDE_DIRS}/private
)

target_link_libraries(wpublish PUBLIC
    log
    XrdSsiLib
)

add_executable(testChunkInventory testChunkInventory.cc)

target_link_libraries(testChunkInventory PUBLIC
    xrdsvc
    Boost::unit_test_framework
    Threads::Threads
)

add_test(NAME testChunkInventory COMMAND testChunkInventory)
