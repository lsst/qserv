-- The query fails in Qserv because Qserv requires that columns used
-- in the ORDER BY clause to be explicitly mentoned in the SELECT list.
-- This is probably a bug in Qserv.
--
-- Another note is that the query is using the obsolete area restrictor
-- that's not supported by MySQL. It has to be replaced by the functionally
-- equivalent SciSQL-specific restrictor: scisql_s2PtInBox(ra_PS,decl_PS,1.28,1.38,3.18,3.34)

-- Select objects in a given area sorted by right ascension
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Base on https://dev.lsstcorp.org/trac/ticket/2051

SELECT * 
 FROM Object
 WHERE ra_PS BETWEEN 1.28 AND 3.18   -- noQserv
   AND decl_PS BETWEEN 1.38 AND 3.34 -- noQserv
-- withQserv WHERE qserv_areaspec_box(1.28,1.38,3.18,3.34)
 ORDER BY ra_PS

