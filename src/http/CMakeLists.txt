add_library(http SHARED)

target_sources(http PRIVATE
    AsyncReq.cc
    Client.cc
    Exceptions.cc
    Method.cc
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
)

install(TARGETS http)

function(HTTP_TESTS)
    foreach(TEST IN ITEMS ${argv})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PUBLIC
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
