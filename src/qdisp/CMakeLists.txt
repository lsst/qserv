add_library(qdisp SHARED)

target_sources(qdisp PRIVATE
    ChunkMeta.cc
    CzarStats.cc
    Executive.cc
    JobDescription.cc
    JobQuery.cc
    UberJob.cc
)

target_link_libraries(qdisp PUBLIC
    cconfig
    log
)

install(TARGETS qdisp)

add_executable(testQDisp testQDisp.cc)

target_link_libraries(testQDisp
    cconfig
    ccontrol
    czar
    global
    mysql
    parser
    qana
    qdisp
    qproc
    css
    qmeta
    query
    rproc
    Boost::unit_test_framework
    Threads::Threads
)

# This is failing in github actions CI but not when running locally on my dev machine.
add_test(NAME testQDisp COMMAND testQDisp)

# set_tests_properties(testQDisp PROPERTIES WILL_FAIL 1)
