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
