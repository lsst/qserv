cmake_minimum_required(VERSION 3.12)

project(Qserv 
    VERSION 0.8
    DESCRIPTION "The Qserv distributed parallel database"
    LANGUAGES CXX
)

include(GNUInstallDirs)
set(CMAKE_INSTALL_PREFIX ${PROJECT_BINARY_DIR}/install)

add_subdirectory(extern/base)
add_subdirectory(extern/db)
add_subdirectory(extern/log)
add_subdirectory(extern/pex_exceptions)
add_subdirectory(extern/partition)
add_subdirectory(extern/sphgeom)
add_subdirectory(extern/utils)

add_subdirectory(src)
