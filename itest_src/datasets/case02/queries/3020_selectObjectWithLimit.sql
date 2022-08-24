-- Select 10 object in a given area
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Base on https://dev.lsstcorp.org/trac/wiki/db/Qserv/Limitations

SELECT *
 FROM Object
 WHERE ra_PS BETWEEN 1 AND 2
   AND decl_PS BETWEEN 3 AND 4
 LIMIT 10;
