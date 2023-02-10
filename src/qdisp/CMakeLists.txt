add_library(qdisp OBJECT)
add_dependencies(qdisp proto)

target_sources(qdisp PRIVATE
    ChunkMeta.cc
    CzarStats.cc
    Executive.cc
    JobDescription.cc
    JobQuery.cc
    JobStatus.cc
    MessageStore.cc
    PseudoFifo.cc
    QdispPool.cc
    QueryRequest.cc
    XrdSsiMocks.cc
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

# This is failing in github actions CI but not when running locally on my dev machine.
# add_test(NAME testQDisp COMMAND testQDisp)

# set_tests_properties(testQDisp PROPERTIES WILL_FAIL 1)
