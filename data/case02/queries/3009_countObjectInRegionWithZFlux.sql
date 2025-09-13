-- Count object in a region with ZFlux in a given interval
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Based on https://dev.lsstcorp.org/trac/wiki/db/queries/020

SELECT count(*) AS OBJ_COUNT
FROM Object
WHERE ra_PS BETWEEN 1.28 AND 1.38
 AND  decl_PS BETWEEN 3.18 AND 3.34
 AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 and 21.5
