add_library(http SHARED)

target_sources(http PRIVATE
    AsyncReq.cc
    BinaryEncoding.cc
    ChttpModule.cc
    Client.cc
    ClientConnPool.cc
    ClientConfig.cc
    ClientConnPool.cc
    Exceptions.cc
    MetaModule.cc
    Method.cc
    Module.cc
    QhttpModule.cc
    RequestBodyJSON.cc
    RequestQuery.cc
    Url.cc
)

target_link_libraries(http PUBLIC
    curl
    log
    qhttp
    util
    Boost::filesystem
    Boost::regex
    Boost::system
    cpp-httplib
)

install(TARGETS http)

function(HTTP_TESTS)
    foreach(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
            global
            http
            Boost::unit_test_framework
            Threads::Threads
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    endforeach()
endfunction()

http_tests(
    testAsyncReq
    testRequestQuery
    testUrl
)
