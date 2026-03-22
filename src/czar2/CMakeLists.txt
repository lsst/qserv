add_library(czar2 SHARED)

target_sources(czar2 PRIVATE
    ChttpModule.cc
    Czar.cc
    CzarThreads.cc
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

target_include_directories(czar2 PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(czar2 PUBLIC
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
    TARGETS czar2
)

function(CZAR2_UTILS)
    foreach(UTIL IN ITEMS ${ARGV})
        add_executable(${UTIL})
        target_sources(${UTIL} PRIVATE ${UTIL}.cc)
        target_include_directories(${UTIL} PRIVATE ${XROOTD_INCLUDE_DIRS})
        target_link_libraries(${UTIL} PRIVATE
            cconfig
            ccontrol
            czar2
            global
            mysql
            parser
            partition
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

czar2_utils(
    qserv-czar2-http
)
