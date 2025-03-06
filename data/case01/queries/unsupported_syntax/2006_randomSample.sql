-- Unsupported syntax due to the placeholder ':percentage'.
-- The query may need to be rewritten to replace the placeholder with
-- so,me meaningful value.

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
