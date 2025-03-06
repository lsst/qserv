-- This query can't be executed by MySQL since it's relying on the Qserv-specific
-- area restrictor. The restrictor seem to be obsolete as it can be
-- replaced with the functionally equivalent SciSQL stored orocedure that's also
-- supported in MySQL: scisql_s2PtInBox(ra_PS,decl_PS,1,3,2,4)

-- Select 10 object in a given area
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Base on https://dev.lsstcorp.org/trac/wiki/db/Qserv/Limitations

SELECT *
 FROM Object
 WHERE qserv_areaspec_box(1,3,2,4)
 LIMIT 10;