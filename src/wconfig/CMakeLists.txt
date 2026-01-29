add_library(wconfig SHARED)

target_sources(wconfig PRIVATE
    WorkerConfig.cc
)

target_link_libraries(wconfig PUBLIC
    log
)

install(TARGETS wconfig)

add_executable(testSanityCheck testSanityCheck.cc)

target_link_libraries(testSanityCheck
    Boost::unit_test_framework
    Threads::Threads
)

add_test(NAME testSanityCheck COMMAND testSanityCheck)
