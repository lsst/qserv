add_library(memman OBJECT)

target_sources(memman PRIVATE
    MemFile.cc
    MemFileSet.cc
    MemMan.cc
    MemManReal.cc
    Memory.cc
)

target_link_libraries(memman PUBLIC
    log
)
