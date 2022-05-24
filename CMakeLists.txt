cmake_minimum_required(VERSION 3.12)

project(Qserv
    DESCRIPTION "The Qserv distributed parallel database"
    LANGUAGES CXX
)

enable_testing()

include(GNUInstallDirs)
set(CMAKE_INSTALL_PREFIX ${PROJECT_BINARY_DIR}/install)

add_subdirectory(doc)
add_subdirectory(extern/log)
add_subdirectory(extern/sphgeom)
add_subdirectory(src)
