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
