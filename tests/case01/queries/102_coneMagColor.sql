-- Cone-magnitude-color search
-- See http://dev.lsstcorp.org/trac/wiki/dbQuery003

SELECT COUNT(*)
FROM   Object
-- WHERE  areaSpec_box(-10, -6, 4, 6)
 WHERE ra_PS between -10 and 4 and decl_PS between -6 and 6
   AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24
   AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9
   AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0
