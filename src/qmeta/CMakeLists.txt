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

install(TARGETS qmeta)
install(DIRECTORY schema/ DESTINATION ${CMAKE_INSTALL_PREFIX}/qserv/smig/qmeta/schema/)

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

#-----------------------------------------------------------------------------

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/python/ DESTINATION ${CMAKE_INSTALL_PREFIX}/python/lsst/qserv/qmeta)
