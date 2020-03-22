add_library(qserv_css SHARED)

target_sources(qserv_css PRIVATE
    constants.h
    CssAccess.cc
    CssAccess.h
    CssConfig.cc
    CssConfig.h
    CssError.h
    DbInterfaceMySql.cc
    DbInterfaceMySql.h
    EmptyChunks.cc
    EmptyChunks.h
    KvInterface.h
    KvInterfaceImplMem.cc
    KvInterfaceImplMem.h
    KvInterfaceImplMySql.cc
    KvInterfaceImplMySql.h
    MatchTableParams.h
    ScanTableParams.h
    StripingParams.h
    TableParams.h
)

target_link_libraries(qserv_css PUBLIC
    qserv_common
    log
)

install(TARGETS qserv_css)

#-----------------------------------------------------------------------------

find_package(pybind11 REQUIRED)

pybind11_add_module(cssLib MODULE)

target_include_directories(cssLib PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..
)

target_sources(cssLib PRIVATE
    cssPythonWrapper.cc
)

target_link_libraries(cssLib PRIVATE
    qserv_css
)

#-----------------------------------------------------------------------------

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/python/ DESTINATION ${CMAKE_INSTALL_PREFIX}/python/lsst/qserv/css)
install(TARGETS cssLib DESTINATION ${CMAKE_INSTALL_PREFIX}/python/lsst/qserv/css)
