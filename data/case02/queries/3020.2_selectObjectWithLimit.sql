-- Select 10 object in a given area
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Base on https://dev.lsstcorp.org/trac/wiki/db/Qserv/Limitations

SELECT *
  FROM Object
  WHERE scisql_s2PtInBox(ra_PS,decl_PS,1,3,2,4)
LIMIT 10;
