add_library(wpublish SHARED)

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
    protobuf
)

add_executable(testChunkInventory testChunkInventory.cc)

target_link_libraries(testChunkInventory PUBLIC
    global
    http
    mysql
    protojson
    sql
    util
    wbase
    wconfig
    wcontrol
    wdb
    wpublish
    wsched
    Boost::unit_test_framework
    Threads::Threads
)

add_test(NAME testChunkInventory COMMAND testChunkInventory)
