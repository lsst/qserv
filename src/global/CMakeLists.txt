add_library(global OBJECT)

target_sources(global PRIVATE
    Bug.cc
    Bug.h
    ConfigError.h
    constants.h
    DbTable.cc
    DbTable.h
    debugUtil.cc
    debugUtil.h
    intTypes.h
    LogContext.h
    MsgReceiver.h
    ResourceUnit.cc
    ResourceUnit.h
    sqltoken.cc
    sqltoken.h
    stringTypes.h
    stringUtil.h
    UnsupportedError.h
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
