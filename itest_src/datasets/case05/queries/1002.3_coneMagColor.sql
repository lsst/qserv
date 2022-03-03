-- Cone-magnitude-color search
-- See http://dev.lsstcorp.org/trac/wiki/db/queries/003

SELECT COUNT(*) AS OBJ_COUNT
FROM   Object
WHERE  scisql_s2PtInEllipse(ra_PS, decl_PS, 72.5, 3.25, 6000, 1700, 0.2) = 1 -- noQserv
-- withQserv WHERE qserv_areaspec_ellipse(72.5, 3.25, 6000, 1700, 0.2)
   AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24
   AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9
   AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0
