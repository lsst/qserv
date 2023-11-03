add_library(global OBJECT)

target_sources(global PRIVATE
    DbTable.cc
    debugUtil.cc
    ResourceUnit.cc
    sqltoken.cc
    stringUtil.cc
)

target_link_libraries(global PUBLIC
    log
)

add_executable(testResourceUnit testResourceUnit.cc)

target_link_libraries(testResourceUnit
    global
    Boost::unit_test_framework
)

add_test(NAME testResourceUnit COMMAND testResourceUnit)
