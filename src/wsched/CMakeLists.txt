add_library(wsched SHARED)
add_dependencies(wsched proto)

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

install(
    TARGETS wsched
)

add_executable(testSchedulers testSchedulers.cc)

target_include_directories(testSchedulers PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(testSchedulers PUBLIC
    wsched
    xrdsvc
    Boost::unit_test_framework
    Threads::Threads
)

add_test(NAME testSchedulers COMMAND testSchedulers)
