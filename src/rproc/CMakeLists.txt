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

install(DIRECTORY python/ DESTINATION ${CMAKE_INSTALL_PREFIX}/python/lsst/qserv/rproc)
install(DIRECTORY schema/ DESTINATION ${CMAKE_INSTALL_PREFIX}/qserv/smig/rproc/schema/)


