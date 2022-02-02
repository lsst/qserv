cmake_minimum_required(VERSION 3.12)

project(Qserv
    VERSION 0.8
    DESCRIPTION "The Qserv distributed parallel database"
    LANGUAGES CXX
)

enable_testing()

include(GNUInstallDirs)
set(CMAKE_INSTALL_PREFIX ${PROJECT_BINARY_DIR}/install)

add_subdirectory(extern/log)
add_subdirectory(extern/partition)
add_subdirectory(extern/qserv_web)
add_subdirectory(extern/sphgeom)

add_subdirectory(src)
