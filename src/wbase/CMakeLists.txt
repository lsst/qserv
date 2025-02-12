add_library(wbase SHARED)
add_dependencies(wbase proto)

target_sources(wbase PRIVATE
    Base.cc
    FileChannelShared.cc
    SendChannel.cc
    Task.cc
    UberJobData.cc
    UserQueryInfo.cc
)

install(
    TARGETS wbase
)


target_link_libraries(wbase PUBLIC
    boost_regex
    log
)
