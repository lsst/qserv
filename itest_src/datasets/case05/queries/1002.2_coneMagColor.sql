-- Cone-magnitude-color search
-- See http://dev.lsstcorp.org/trac/wiki/db/queries/003

-- pragma sortresult
SELECT COUNT(*) AS OBJ_COUNT
FROM   Object
WHERE  scisql_s2PtInCircle(ra_PS, decl_PS, 72.5, 3.25, 0.6) = 1 -- noQserv
-- withQserv WHERE qserv_areaspec_circle(72.5, 3.25, 0.6)
   AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24
   AND scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) BETWEEN 0.1 AND 0.9
   AND scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) BETWEEN 0.1 AND 1.0
