-- Random sample of the dataq
-- http://dev.lsstcorp.org/trac/wiki/dbQuery004


SELECT fluxToAbMag(uFlux_PS), 
       fluxToAbMag(gFlux_PS), 
       fluxToAbMag(rFlux_PS), 
       fluxToAbMag(iFlux_PS), 
       fluxToAbMag(zFlux_PS), 
       fluxToAbMag(yFlux_PS)
FROM   Object 
WHERE  (objectId % 100 ) = :percentage
