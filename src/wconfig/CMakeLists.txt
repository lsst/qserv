add_library(wconfig OBJECT)
add_dependencies(wconfig proto)

target_sources(wconfig PRIVATE
    WorkerConfig.cc
)

target_link_libraries(wconfig PUBLIC
    log
    proto
)

add_executable(testSanityCheck testSanityCheck.cc)

target_link_libraries(testSanityCheck
    Boost::unit_test_framework
    Threads::Threads
)

add_test(NAME testSanityCheck COMMAND testSanityCheck)

install(
    TARGETS wconfig
)
