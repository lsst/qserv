add_library(workerm SHARED)

target_sources(workerm PRIVATE
    WorkerMain.cc
)

target_link_libraries(workerm PUBLIC
    boost_filesystem
    boost_system
    global
    http
    log
    mysql
    protojson
    sql
    wbase
    wcomms
    wconfig
    wcontrol
    wdb
    wpublish
    wsched
    boost_program_options
)

install(TARGETS workerm)

function(WORKERM_UTILS)
    foreach(UTIL IN ITEMS ${ARGV})
        add_executable(${UTIL})
        target_sources(${UTIL} PRIVATE ${UTIL}.cc)
        target_link_libraries(${UTIL} PRIVATE
            global
            workerm
        )
        install(TARGETS ${UTIL})
    endforeach()
endfunction()

workerm_utils(
    qserv-worker-http
)
