add_library(wbase SHARED)

target_sources(wbase PRIVATE
    Base.cc
    FileChannelShared.cc
    SendChannel.cc
    Task.cc
    UberJobData.cc
    UserQueryInfo.cc
)

target_link_libraries(wbase PUBLIC
    boost_regex
    log
)

install(TARGETS wbase)
