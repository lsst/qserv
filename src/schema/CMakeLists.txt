install(DIRECTORY python/ DESTINATION ${CMAKE_INSTALL_PREFIX}/python/lsst/qserv/schema)
install(DIRECTORY bin/ DESTINATION ${CMAKE_INSTALL_BINDIR})
install(DIRECTORY tests/ DESTINATION ${CMAKE_INSTALL_PREFIX}/python/tests/schema)

add_test(NAME test_SchemaMigMgr
  COMMAND python3 -m unittest tests.schema.test_SchemaMigMgr
  WORKING_DIRECTORY ${CMAKE_INSTALL_PREFIX}/python/
)
