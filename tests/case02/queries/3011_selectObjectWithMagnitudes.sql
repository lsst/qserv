-- Select object with magnitudes between 20 and 24
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Based on https://dev.lsstcorp.org/trac/wiki/db/Qserv/OptimalPartitionSize

SELECT objectId, ra_PS, decl_PS, 
       scisql_fluxToAbMag(zFlux_PS) 
 FROM Object 
 WHERE scisql_fluxToAbMag(zFlux_PS)
       BETWEEN 20 AND 24
