find_package(XRootD REQUIRED)

add_library(xrdlog MODULE)

target_sources(xrdlog PRIVATE
    XrdLogging.cc
)

target_include_directories(xrdlog PRIVATE
    ${XROOTD_INCLUDE_DIRS}
    ${XROOTD_INCLUDE_DIRS}/private
)

target_link_libraries(xrdlog PUBLIC
    log
    XrdSsiLib
)

install(
    TARGETS xrdlog DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
