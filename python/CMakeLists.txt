install(
  DIRECTORY . DESTINATION python
  PATTERN CMakeLists.txt EXCLUDE
)

add_test (NAME test_watcher
  COMMAND python3 -m unittest tests.admin.test_watcher
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

set_tests_properties(test_watcher PROPERTIES WILL_FAIL 1)

add_test(NAME test_smig
  COMMAND python3 -m unittest lsst.qserv.admin.tests.test_smig
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

set_tests_properties(test_smig PROPERTIES ENVIRONMENT
  QSERV_SMIG_DIRECTORY=${CMAKE_INSTALL_PREFIX}/qserv/smig
)

add_test(NAME test_qserv_cli
  COMMAND python3 -m unittest lsst.qserv.admin.tests.test_qserv_cli
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(NAME test_itest
  COMMAND python3 -m unittest lsst.qserv.admin.tests.test_itest
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(NAME test_utils
  COMMAND python3 -m unittest lsst.qserv.admin.tests.test_utils
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(NAME test_options_file_option
  COMMAND python3 -m unittest lsst.qserv.admin.tests.test_options_file_option
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(NAME test_qserv_log
  COMMAND python3 -m unittest lsst.qserv.admin.tests.test_qserv_log
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(NAME test_render_targs
  COMMAND python3 -m unittest lsst.qserv.admin.tests.test_render_targs
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

add_test(NAME test_schema_mig_mgr
  COMMAND python3 -m unittest lsst.qserv.schema.tests.test_schema_mig_mgr
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
