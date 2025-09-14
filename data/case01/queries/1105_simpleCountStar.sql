-- Test a simple SELECT COUNT(*), which is "SELECT COUNT(*) FROM myDb"
-- without any other qualifers (e.g. a WHERE clause)
-- see https://jira.lsstcorp.org/browse/DM-32600
-- Note, integration test case01 should have table statistics, enabled
-- by setting build_table_stats to True in integration_test.yaml IE
--
-- test_cases:
--   - id: case01
--     build_table_stats: True
--
-- This tells the replication/ingest system to build table stats when
-- loading integration test data. The table stats database will then
-- contain the information needed to perform the simple-count-star
-- optimization.
--
-- This test does not verify that the simple-count-star optimizaiton was
-- used, but if the optimization is enabled correctly then this test
-- exercicizes the optimization code.


SELECT COUNT(*) FROM Object
