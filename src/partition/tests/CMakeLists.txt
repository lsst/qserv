FUNCTION(partition_tests)
    FOREACH(TEST IN ITEMS ${ARGV})
        add_executable(${TEST} ${TEST}.cc)
        target_link_libraries(${TEST} PRIVATE
            partition
            Boost::filesystem
            Boost::unit_test_framework
        )
        add_test(NAME ${TEST} COMMAND ${TEST})
    ENDFOREACH()
ENDFUNCTION()

partition_tests(
    chunkIndex
    configStore
    csv
    fileUtils
    geometry
    htmIndex
    mapReduce
    objectIndex
    vector
)
