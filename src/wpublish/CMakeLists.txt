add_library(wpublish OBJECT)
add_dependencies(wpublish proto)

target_sources(wpublish PRIVATE
    ChunkInventory.cc
    QueriesAndChunks.cc
    QueryStatistics.cc
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

install(
    TARGETS wpublish
)
