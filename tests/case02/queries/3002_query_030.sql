SELECT objectId, ra_SG, decl_SG
FROM   Object
WHERE  (                  
         scisql_fluxToAbMag(rFlux_SG)-scisql_fluxToAbMag(iFlux_SG) - 
         (scisql_fluxToAbMag(gFlux_SG)-scisql_fluxToAbMag(rFlux_SG))/4 - 0.18
        ) BETWEEN -0.2 AND 0.2
  AND  (                 
          (
            (scisql_fluxToAbMag(rFlux_SG)-scisql_fluxToAbMag(iFlux_SG)) - 
            (scisql_fluxToAbMag(gFlux_SG)-scisql_fluxToAbMag(rFlux_SG))/4 - 0.18 
          ) > (0.45 - 4*(scisql_fluxToAbMag(gFlux_SG)-scisql_fluxToAbMag(rFlux_SG))) 
       )
