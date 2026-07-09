Place parser corpus SQL files in this directory.

testParserCorpus reads files ending in `.sql`, parses each statement, and
verifies that Qserv IR can be built. It assumes that there is ONE statement
per file.

The two main ideas behind this test:
    * Stress test the parser and Qserv IR adapter
    * Catch parse speed regressions