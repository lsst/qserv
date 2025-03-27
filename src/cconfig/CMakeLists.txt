add_library(cconfig SHARED)

target_sources(cconfig PRIVATE
    CzarConfig.cc
)

target_include_directories(cconfig PRIVATE
    ${XROOTD_INCLUDE_DIRS}
)

target_link_libraries(cconfig PUBLIC
    log
    XrdSsiLib
)

install(
    TARGETS cconfig
)
