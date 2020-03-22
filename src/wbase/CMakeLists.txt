find_package(XRootD REQUIRED)

add_library(wbase OBJECT)
add_dependencies(wbase proto)

target_sources(wbase PRIVATE
    Base.cc
    Base.h
    MsgProcessor.h
    SendChannel.cc
    SendChannel.h
    Task.cc
    Task.h
    WorkerCommand.cc
    WorkerCommand.h
)

target_include_directories(wbase PRIVATE
    ${XROOTD_INCLUDE_DIRS}
    ${XROOTD_INCLUDE_DIRS}/private
)

target_link_libraries(wbase PUBLIC
    boost_regex
    log
    XrdSsiLib
)
