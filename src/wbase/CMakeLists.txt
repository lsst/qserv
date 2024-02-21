add_library(wbase OBJECT)
add_dependencies(wbase proto)

target_sources(wbase PRIVATE
    Base.cc
    FileChannelShared.cc
    SendChannel.cc
    Task.cc
    UserQueryInfo.cc
    WorkerCommand.cc
)

target_include_directories(wbase PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(wbase PUBLIC
    boost_regex
    log
    XrdSsiLib
)
