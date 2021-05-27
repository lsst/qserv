add_library(wsched OBJECT)
add_dependencies(wsched proto)

target_sources(wsched PRIVATE
    BlendScheduler.cc
    BlendScheduler.h
    ChunkDisk.cc
    ChunkDisk.h
    ChunkTaskCollection.h
    ChunkTasksQueue.cc
    ChunkTasksQueue.h
    FifoScheduler.h
    GroupScheduler.cc
    GroupScheduler.h
    ScanScheduler.cc
    ScanScheduler.h
    SchedulerBase.cc
    SchedulerBase.h
)

target_link_libraries(wsched PUBLIC
    log
)

add_executable(testSchedulers testSchedulers.cc)

target_include_directories(testSchedulers PRIVATE
    ${XROOTD_INCLUDE_DIRS}
    ${XROOTD_INCLUDE_DIRS}/private
)

target_link_libraries(testSchedulers PUBLIC
    wsched
    xrdsvc
    Boost::unit_test_framework
    Threads::Threads
)

add_test(NAME testSchedulers COMMAND testSchedulers)
