add_library(qserv_meta SHARED)

target_sources(qserv_meta PRIVATE
    QMeta.cc
    QMetaMysql.cc
    QMetaSelect.cc
    QMetaTransaction.cc
    QStatusMysql.cc
)

target_link_libraries(qserv_meta PUBLIC
    qserv_common
    log
    mysqlclient_r
    qdisp
)

install(TARGETS qserv_meta)
install(DIRECTORY schema/ DESTINATION ${CMAKE_INSTALL_PREFIX}/qserv/smig/qmeta/schema/)

add_executable(testQMeta testQMeta.cc)

target_link_libraries(testQMeta
    qserv_meta
    qdisp
    Boost::unit_test_framework
    Threads::Threads
)

# don't add test, needs fix; DM-30562: fix Qserv unit tests that prompt for an sql connection
# add_test(NAME testQMeta COMMAND testQMeta)

#-----------------------------------------------------------------------------

pybind11_add_module(qmetaLib MODULE)

target_include_directories(qmetaLib PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)

target_sources(qmetaLib PRIVATE
    qmetaPythonWrapper.cc
)

target_link_libraries(qmetaLib PRIVATE
    qserv_meta
)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/python/ DESTINATION ${CMAKE_INSTALL_PREFIX}/python/lsst/qserv/qmeta)
install(TARGETS qmetaLib DESTINATION ${CMAKE_INSTALL_PREFIX}/python/lsst/qserv/qmeta)
