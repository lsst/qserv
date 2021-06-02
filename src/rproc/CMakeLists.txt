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


add_executable(testInvalidJobAttemptMgr testInvalidJobAttemptMgr.cc)
target_link_libraries(testInvalidJobAttemptMgr
    ccontrol
    parser
    qana
    qdisp
    qproc
    query
    qserv_css
    qserv_meta
    rproc
    Boost::unit_test_framework
    Threads::Threads
)
add_test(NAME testInvalidJobAttemptMgr COMMAND testInvalidJobAttemptMgr)


add_executable(testProtoRowBuffer testProtoRowBuffer.cc)
target_link_libraries(testProtoRowBuffer
    ccontrol
    parser
    qana
    qdisp
    qproc
    query
    qserv_css
    qserv_meta
    rproc
    Boost::unit_test_framework
    Threads::Threads
)
add_test(NAME testProtoRowBuffer COMMAND testProtoRowBuffer)
