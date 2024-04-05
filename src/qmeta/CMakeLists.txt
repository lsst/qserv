add_library(qmeta SHARED)

target_sources(qserv_meta PRIVATE
    JobStatus.cc
    MessageStore.cc
    QMeta.cc
    QMetaMysql.cc
    QMetaSelect.cc
    QMetaTransaction.cc
    QProgress.cc
    QProgressHistory.cc
    UserTables.cc
    UserTableIngestRequest.cc
)

target_link_libraries(qmeta PUBLIC
    cconfig
    global
    mysql
    qdisp
    log
    util
    mysqlclient_r
    http 
)
# &&& see if there's a way to remove some target_link_libraries
# &&&   ccontrol
# &&&    czar
# &&&    parser
# &&&    rproc
# &&&    qana
# &&&    qproc
# &&&    query  

install(TARGETS qmeta)

add_executable(testQMeta testQMeta.cc)

target_link_libraries(testQMeta
    cconfig
    proto
    qmeta
    sql
    Boost::unit_test_framework
    Threads::Threads
)

# don't add test, needs fix; DM-30562: fix Qserv unit tests that prompt for an sql connection
# add_test(NAME testQMeta COMMAND testQMeta)
