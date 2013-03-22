SELECT  objectId
FROM    Object
WHERE   scisql_fluxToAbMag(uFlux_PS)-scisql_fluxToAbMag(gFlux_PS) <  2.0
   AND  scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) <  0.1
   AND  scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) > -0.8
   AND  scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) <  1.4 
