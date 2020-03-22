add_library(rproc OBJECT)
add_dependencies(rproc proto)

target_sources(rproc PRIVATE
    InfileMerger.cc
    InfileMerger.h
    ProtoRowBuffer.cc
    ProtoRowBuffer.h
)

target_link_libraries(rproc PUBLIC
    boost_regex
    log
)
