set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(ANTLR_EXECUTABLE /usr/share/java/antlr-4.8-complete.jar)
find_package(ANTLR REQUIRED)
find_package(antlr4-runtime REQUIRED)

find_package(Boost REQUIRED
    filesystem REQUIRED
    program_options REQUIRED
    regex REQUIRED
    system REQUIRED
    unit_test_framework REQUIRED
)

find_package(Lua51 REQUIRED)
find_package(Protobuf REQUIRED)
find_package(pybind11 REQUIRED)
find_package(Threads REQUIRED)
find_package(XRootD REQUIRED)

add_compile_options(-Og -g -Wall -Wextra -Wno-unused-parameter)
add_compile_definitions(BOOST_TEST_DYN_LINK=1)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
)

#-----------------------------------------------------------------------------

add_subdirectory(admin)
add_subdirectory(ccontrol)
add_subdirectory(css)
add_subdirectory(czar)
add_subdirectory(global)
add_subdirectory(memman)
add_subdirectory(mysql)
add_subdirectory(parser)
add_subdirectory(proto)
add_subdirectory(proxy)
add_subdirectory(qana)
add_subdirectory(qdisp)
add_subdirectory(qhttp)
add_subdirectory(qmeta)
add_subdirectory(qproc)
add_subdirectory(query)
add_subdirectory(replica)
add_subdirectory(rproc)
add_subdirectory(schema)
add_subdirectory(sql)
add_subdirectory(util)
add_subdirectory(wbase)
add_subdirectory(wconfig)
add_subdirectory(wcontrol)
add_subdirectory(worker)
add_subdirectory(wdb)
add_subdirectory(wpublish)
add_subdirectory(wsched)
add_subdirectory(xrdlog)
add_subdirectory(xrdsvc)

#-----------------------------------------------------------------------------

add_library(qserv_common SHARED)

target_link_libraries(qserv_common PUBLIC
    global
    memman
    proto
    mysql
    sql
    util
)

install(
    TARGETS qserv_common
)

#-----------------------------------------------------------------------------

add_library(xrdsvc SHARED)

target_link_libraries(xrdsvc PUBLIC
    wbase
    wcontrol
    wconfig
    wdb
    wpublish
    wsched
    qserv_xrdsvc
    qserv_common
)

install(
    TARGETS xrdsvc
)

#-----------------------------------------------------------------------------

add_library(qserv_czar SHARED)

target_link_libraries(qserv_czar PUBLIC
    ccontrol
    czar
    parser
    qana
    query
    qdisp
    qproc
    rproc
    qserv_css
    qserv_meta
)

install(
    TARGETS qserv_czar
)
