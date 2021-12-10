set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(Sphinx REQUIRED)

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/conf.py.in"
    "${CMAKE_CURRENT_BINARY_DIR}/conf.py"
    @ONLY
)

add_custom_target(docs-html
    COMMENT "Building HTML documentation with Sphinx"
    COMMAND ${SPHINX_EXECUTABLE}
        -b html
        -c ${CMAKE_CURRENT_BINARY_DIR}
        -d ${CMAKE_CURRENT_BINARY_DIR}/doctrees
        -n -W
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}/html
    BYPRODUCTS
        ${CMAKE_CURRENT_BINARY_DIR}/doctrees
        ${CMAKE_CURRENT_BINARY_DIR}/html
)

add_custom_target(docs-linkcheck
    COMMENT "Checking documentation links with Sphinx"
    COMMAND ${SPHINX_EXECUTABLE}
        -b linkcheck
        -c ${CMAKE_CURRENT_BINARY_DIR}
        -d ${CMAKE_CURRENT_BINARY_DIR}/doctrees
        -n -W
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}/linkcheck
    BYPRODUCTS
        ${CMAKE_CURRENT_BINARY_DIR}/doctrees
        ${CMAKE_CURRENT_BINARY_DIR}/linkcheck
)
