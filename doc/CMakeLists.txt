set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(Sphinx REQUIRED)

add_custom_target(docs-html
    COMMENT "Building HTML documentation with Sphinx"
    COMMAND ${SPHINX_EXECUTABLE}
        -b html
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
        -d ${CMAKE_CURRENT_BINARY_DIR}/doctrees
        -n -W
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}/linkcheck
    BYPRODUCTS
        ${CMAKE_CURRENT_BINARY_DIR}/doctrees
        ${CMAKE_CURRENT_BINARY_DIR}/linkcheck
)
