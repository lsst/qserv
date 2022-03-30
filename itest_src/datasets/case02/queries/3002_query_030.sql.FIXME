-- Select object based on linear flux interval query
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013 
-- Based on https://dev.lsstcorp.org/trac/wiki/db/queries/012

SELECT objectId, ra_PS, decl_PS
FROM   Object
WHERE  (                  
         scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) - 
         (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))/4 - 0.18
        ) BETWEEN -0.2 AND 0.2
  AND  (                 
          (
            (scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS)) - 
            (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))/4 - 0.18 
          ) > (0.45 - 4*(scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))) 
       )
