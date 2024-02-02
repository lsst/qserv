add_library(czar OBJECT)

target_sources(czar PRIVATE
    Czar.cc
    HttpCzarSvc.cc
    HttpCzarQueryModule.cc
    HttpModule.cc
    HttpMonitorModule.cc
    HttpSvc.cc
    MessageTable.cc
)

target_include_directories(czar PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(czar PUBLIC
    cconfig
    http
    qdisp
    qhttp
    util
    log
    XrdSsiLib
)

function(CZAR_UTILS)
    foreach(UTIL IN ITEMS ${ARGV})
        add_executable(${UTIL})
        target_sources(${UTIL} PRIVATE ${UTIL}.cc)
        target_include_directories(${UTIL} PRIVATE ${XROOTD_INCLUDE_DIRS})
        target_link_libraries(${UTIL} PRIVATE
            cconfig
            ccontrol
            czar
            global
            mysql
            parser
            qana
            qdisp
            qproc
            qserv_meta
            query
            rproc
            sql
        )
        install(TARGETS ${UTIL})
    endforeach()
endfunction()

czar_utils(
    qserv-czar-http
)