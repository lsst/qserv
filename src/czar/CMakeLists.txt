add_library(czar OBJECT)

target_sources(czar PRIVATE
    Czar.cc
    CzarConfig.cc
    MessageTable.cc
)

target_include_directories(czar PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(czar PUBLIC
    log
    XrdSsiLib
)
