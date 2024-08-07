add_library(qhttp SHARED)

target_sources(qhttp PRIVATE
    AjaxEndpoint.cc
    MultiPartParser.cc
    Path.cc
    Request.cc
    Response.cc
    Server.cc
    StaticContent.cc
)

target_link_libraries(qhttp PUBLIC
    log
    Boost::filesystem
    Boost::regex
    Boost::system
)

install(TARGETS qhttp)

add_executable(testqhttp testqhttp.cc)

target_link_libraries(testqhttp PUBLIC
    curl
    qhttp
    Boost::unit_test_framework
    Threads::Threads
)

add_test(NAME testqhttp COMMAND testqhttp -- --data=${CMAKE_CURRENT_SOURCE_DIR}/testdata/ --retries=2 --retry-delay=1 --threads=2 --client-threads=4)
