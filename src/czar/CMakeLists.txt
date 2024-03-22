add_library(czar SHARED)

target_sources(czar PRIVATE
    ChttpModule.cc
    Czar.cc
    CzarChunkMap.cc
    HttpCzarIngestCsvModule.cc
    HttpCzarIngestModuleBase.cc
    HttpCzarIngestModule.cc
    HttpCzarQueryModule.cc
    HttpCzarSvc.cc
    HttpMonitorModule.cc
    HttpSvc.cc
    MessageTable.cc
    QhttpModule.cc
    WorkerIngestProcessor.cc
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
    cpp-httplib
    boost_program_options
)

install(
    TARGETS czar
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
            qmeta
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
