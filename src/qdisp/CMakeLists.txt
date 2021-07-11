add_library(qdisp OBJECT)
add_dependencies(qdisp proto)

target_sources(qdisp PRIVATE
    ChunkMeta.cc
    ChunkMeta.h
    Executive.cc
    Executive.h
    JobDescription.cc
    JobDescription.h
    JobQuery.cc
    JobQuery.h
    JobStatus.cc
    JobStatus.h
    MessageStore.cc
    MessageStore.h
    QdispPool.cc
    QdispPool.h
    QueryRequest.cc
    QueryRequest.h
    ResponseHandler.h
    XrdSsiMocks.cc
    XrdSsiMocks.h
)

target_include_directories(qdisp PRIVATE
    ${XROOTD_INCLUDE_DIRS}
    ${XROOTD_INCLUDE_DIRS}/private
)

target_link_libraries(qdisp PUBLIC
    log
    XrdSsiLib
)

add_executable(testQDisp testQDisp.cc)

target_include_directories(testQDisp PRIVATE
    ${XROOTD_INCLUDE_DIRS}
    ${XROOTD_INCLUDE_DIRS}/private
)

target_link_libraries(testQDisp
    ccontrol
    czar
    parser
    qana
    qdisp
    qproc
    qserv_css
    qserv_meta
    query
    rproc
    Boost::unit_test_framework
    Threads::Threads
)

add_test(NAME testQDisp COMMAND testQDisp)
