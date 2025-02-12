add_library(czar SHARED)
add_dependencies(czar proto)

target_sources(czar PRIVATE
    ActiveWorker.cc
    ChttpModule.cc
    Czar.cc
    CzarChunkMap.cc
    CzarRegistry.cc
    CzarThreads.cc
    HttpCzarIngestCsvModule.cc
    HttpCzarIngestModuleBase.cc
    HttpCzarIngestModule.cc
    HttpCzarQueryModule.cc
    HttpCzarSvc.cc
    HttpCzarWorkerModule.cc
    HttpMonitorModule.cc
    HttpSvc.cc
    MessageTable.cc
    QhttpModule.cc
    WorkerIngestProcessor.cc
)


target_link_libraries(czar PUBLIC
    cconfig
    http
    partition
    protojson
    qdisp
    qhttp
    util
    log
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

function(czar_tests)
    foreach(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
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
            Boost::unit_test_framework
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    endforeach()
endfunction()

czar_tests(
    testCzar
)

