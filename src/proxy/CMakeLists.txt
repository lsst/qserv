add_library(czarProxy MODULE)

set_target_properties(czarProxy PROPERTIES
    PREFIX ""
)

target_sources(czarProxy PRIVATE
    czarProxy.cc
    czarProxy.h
    czarProxyLuaWrapper.cc
)

target_link_libraries(czarProxy PRIVATE
    log
    lua
    qserv_czar
)

install(TARGETS czarProxy DESTINATION ${CMAKE_INSTALL_PREFIX}/lua/qserv/lib)
install(FILES mysqlProxy.lua DESTINATION ${CMAKE_INSTALL_PREFIX}/lua/qserv/scripts)
