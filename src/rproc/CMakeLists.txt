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

install(DIRECTORY python/ DESTINATION ${CMAKE_INSTALL_PREFIX}/python/lsst/qserv/rproc)
install(DIRECTORY schema/ DESTINATION ${CMAKE_INSTALL_PREFIX}/qserv/smig/rproc/schema/)
