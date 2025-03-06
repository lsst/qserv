-- Select objects in a given area sorted by right ascension
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Base on https://dev.lsstcorp.org/trac/ticket/2051

-- Note: Qserv requires that columns used in ORDER BY be explicitly
-- mentioned in the SELECT list. Otherwise the query would fail the
-- query analysis.
-- Also note the area strictor function used to limit the spatial area
-- inspected by the query.

SELECT ra_PS,decl_PS
 FROM Object
 WHERE scisql_s2PtInBox(ra_PS,decl_PS,1.28,1.38,3.18,3.34)
 ORDER BY ra_PS;
