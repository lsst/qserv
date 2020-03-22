find_package(XRootD REQUIRED)

add_library(czar OBJECT)

target_sources(czar PRIVATE
    Czar.cc
    CzarConfig.cc
    CzarConfig.h
    CzarErrors.h
    Czar.h
    MessageTable.cc
    MessageTable.h
    SubmitResult.h
)

target_include_directories(czar PRIVATE
    ${XROOTD_INCLUDE_DIRS}
    ${XROOTD_INCLUDE_DIRS}/private
)

target_link_libraries(czar PUBLIC
    log
    XrdSsiLib
)
