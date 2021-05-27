add_library(qhttp SHARED)

target_sources(qhttp PRIVATE
    AjaxEndpoint.cc
    AjaxEndpoint.h
    Path.cc
    Path.h
    Request.cc
    Request.h
    Response.cc
    Response.h
    Server.cc
    Server.h
    StaticContent.cc
    StaticContent.h
)

target_link_libraries(qhttp PUBLIC
    Boost::filesystem
    Boost::regex
    Boost::system
)

install(TARGETS qhttp)

add_executable(testqhttp testqhttp.cc)

target_link_libraries(testqhttp PUBLIC 
    qhttp
    Boost::unit_test_framework
    Threads::Threads
    curl
)

add_test(NAME test_qhttp COMMAND testqhttp)
