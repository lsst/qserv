add_library(memman SHARED)

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

install(
    TARGETS memman
)
