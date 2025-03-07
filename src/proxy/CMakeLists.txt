add_library(czarProxy MODULE)

set_target_properties(czarProxy PROPERTIES
    PREFIX ""
)

target_sources(czarProxy PRIVATE
    czarProxy.cc
    czarProxyLuaWrapper.cc
)

target_include_directories(czarProxy PRIVATE
    ${LUA_INCLUDE_DIR}
)

target_link_libraries(czarProxy PRIVATE
    log
    lua-5.1
    cconfig
    ccontrol
    czar
    parser
    qana
    query
    qdisp
    qproc
    rproc
    css
    qmeta
    xrdreq)

install(TARGETS czarProxy DESTINATION lua/qserv/lib)
install(FILES mysqlProxy.lua DESTINATION lua/qserv/scripts)
