-- Select object with Magnitude in intervals
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Based on https://dev.lsstcorp.org/trac/wiki/db/queries/013

SELECT count(*) AS OBJ_COUNT
 FROM   Object 
 WHERE ra_PS BETWEEN 1.28 AND 1.38
   AND decl_PS BETWEEN 3.18 AND 3.34
  AND scisql_fluxToAbMag(zFlux_PS)
      BETWEEN 21 AND 21.5  
  AND scisql_fluxToAbMag(gFlux_PS)
      - scisql_fluxToAbMag(rFlux_PS) 
      BETWEEN 0.3 AND 0.4  
  AND scisql_fluxToAbMag(iFlux_PS)
      - scisql_fluxToAbMag(zFlux_PS) 
      BETWEEN 0.1 AND 0.12
