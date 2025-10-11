add_library(rproc SHARED)
add_dependencies(rproc proto)

target_sources(rproc PRIVATE
    InfileMerger.cc
)

target_link_libraries(rproc PUBLIC
    boost_regex
    cconfig
    log
)

install(TARGETS rproc
    EXPORT qserv
)
