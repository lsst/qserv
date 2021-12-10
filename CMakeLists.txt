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
add_subdirectory(extern/partition)
add_subdirectory(extern/qserv_web)
add_subdirectory(extern/sphgeom)
add_subdirectory(src)

# utils is Python-only, so just install with pip instead of embedded cmake
install(CODE "execute_process(COMMAND pip3 install ${CMAKE_CURRENT_SOURCE_DIR}/extern/utils -t ${CMAKE_INSTALL_PREFIX}/python --no-deps --no-compile)")
