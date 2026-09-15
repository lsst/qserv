# Cross-database ref-match test

This integration test is designed to exercise situations where a ref-match
table refers to directors that are outside its own database.

It spans three databases:
1. `qcase05_1.RefObject`
2. `qcase05_2.RunDeepSource`
3. `qcase05_matches.RefDeepSrcMatch`

With `qcase05_matches.RefDeepSrcMatch` referring to two external directors.

The data, schemas, and partition configs were copied directly from case03.
