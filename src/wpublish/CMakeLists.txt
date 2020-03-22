find_package(XRootD REQUIRED)

add_library(wpublish OBJECT)
add_dependencies(wpublish proto)

target_sources(wpublish PRIVATE
    AddChunkGroupCommand.cc
    AddChunkGroupCommand.h
    ChunkGroupQservRequest.cc
    ChunkGroupQservRequest.h
    ChunkInventory.cc
    ChunkInventory.h
    ChunkListCommand.cc
    ChunkListCommand.h
    ChunkListQservRequest.cc
    ChunkListQservRequest.h
    GetChunkListCommand.cc
    GetChunkListCommand.h
    GetChunkListQservRequest.cc
    GetChunkListQservRequest.h
    GetStatusCommand.cc
    GetStatusCommand.h
    GetStatusQservRequest.cc
    GetStatusQservRequest.h
    QservRequest.cc
    QservRequest.h
    QueriesAndChunks.cc
    QueriesAndChunks.h
    RemoveChunkGroupCommand.cc
    RemoveChunkGroupCommand.h
    ResourceMonitor.cc
    ResourceMonitor.h
    SetChunkListCommand.cc
    SetChunkListCommand.h
    SetChunkListQservRequest.cc
    SetChunkListQservRequest.h
    TestEchoCommand.cc
    TestEchoCommand.h
    TestEchoQservRequest.cc
    TestEchoQservRequest.h
)

target_include_directories(wpublish PRIVATE
    ${XROOTD_INCLUDE_DIRS}
    ${XROOTD_INCLUDE_DIRS}/private
)

target_link_libraries(wpublish PUBLIC
    log
    XrdSsiLib
)