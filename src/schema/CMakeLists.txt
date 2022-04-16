install(DIRECTORY python/ DESTINATION ${CMAKE_INSTALL_PREFIX}/python/lsst/qserv/schema)
install(DIRECTORY bin/ DESTINATION ${CMAKE_INSTALL_BINDIR})

add_test(NAME test_SchemaMigMgr
  COMMAND python3 -m unittest lsst.qserv.schema.tests.test_SchemaMigMgr
  WORKING_DIRECTORY ${CMAKE_INSTALL_PREFIX}/python/
)
