add_library(qserv_meta SHARED)

target_sources(qserv_meta PRIVATE
    Exceptions.h
    QInfo.h
    QMeta.cc
    QMeta.h
    QMetaMysql.cc
    QMetaMysql.h
    QMetaSelect.cc
    QMetaSelect.h
    QMetaTransaction.cc
    QMetaTransaction.h
    QStats.h
    QStatus.h
    QStatusMysql.cc
    QStatusMysql.h
    types.h
)

target_link_libraries(qserv_meta PUBLIC
    qserv_common
    log
    mysqlclient_r
)

install(
    TARGETS qserv_meta
)

#-----------------------------------------------------------------------------

find_package(pybind11 REQUIRED)

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

#-----------------------------------------------------------------------------

install(DIRECTORY python/ DESTINATION ${CMAKE_INSTALL_PREFIX}/python/lsst/qserv/qmeta)
install(TARGETS qmetaLib DESTINATION ${CMAKE_INSTALL_PREFIX}/python/lsst/qserv/qmeta)
install(DIRECTORY schema/ DESTINATION ${CMAKE_INSTALL_PREFIX}/qserv/smig/qmeta/schema/)
