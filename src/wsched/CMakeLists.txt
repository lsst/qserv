add_library(wsched SHARED)

target_sources(wsched PRIVATE
    BlendScheduler.cc
    ChunkTasksQueue.cc
    GroupScheduler.cc
    ScanScheduler.cc
    SchedulerBase.cc
)

target_link_libraries(wsched PUBLIC
    log
)

install(TARGETS wsched)

add_executable(testSchedulers testSchedulers.cc)

target_link_libraries(testSchedulers PUBLIC
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

add_test(NAME testSchedulers COMMAND testSchedulers)
