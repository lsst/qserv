-- Select objects in a given area sorted by right ascension
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Base on https://dev.lsstcorp.org/trac/ticket/2051

SELECT * 
 FROM Object
 WHERE qserv_areaspec_box(1.28,1.38,3.18,3.34)
 ORDER BY ra_PS
