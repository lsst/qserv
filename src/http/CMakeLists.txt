add_library(http SHARED)

target_sources(http PRIVATE
    AsyncReq.cc
    Client.cc
    ClientConfig.cc
    Exceptions.cc
    MetaModule.cc
    Method.cc
    ModuleBase.cc
    RequestBody.cc
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
