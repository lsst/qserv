add_library(qmeta SHARED)

target_sources(qmeta PRIVATE
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
    http
    mysql
    log
    util 
)

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
