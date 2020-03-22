add_library(memman OBJECT)

target_sources(memman PRIVATE
    MemFile.cc
    MemFile.h
    MemFileSet.cc
    MemFileSet.h
    MemMan.cc
    MemMan.h
    MemManNone.h
    MemManReal.cc
    Memory.cc
    Memory.h
)

target_link_libraries(memman PUBLIC
    log
)
