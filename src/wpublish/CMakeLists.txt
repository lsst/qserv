add_library(wpublish SHARED)
add_dependencies(wpublish proto)

target_sources(wpublish PRIVATE
    ChunkInventory.cc
    QueriesAndChunks.cc
    QueryStatistics.cc
)

install(
    TARGETS wpublish
)

target_link_libraries(wpublish PUBLIC
    log
    proto
    protobuf
)

add_executable(testChunkInventory testChunkInventory.cc)

target_link_libraries(testChunkInventory PUBLIC
    xrdsvc
    Boost::unit_test_framework
    Threads::Threads
)

add_test(NAME testChunkInventory COMMAND testChunkInventory)
