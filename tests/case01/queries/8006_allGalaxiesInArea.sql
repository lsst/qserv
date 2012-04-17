-- Select all galaxies in a given area
-- http://dev.lsstcorp.org/trac/wiki/dbQuery014

-- Missing in current schema: extendedParameter

SELECT objectId
FROM   Object
WHERE  ra_PS BETWEEN :raMin AND :raMax       -- noQserv
   AND decl_PS BETWEEN :declMin AND :declMax -- noQserv
-- withQserv WHERE qserv_areaspec_box(:raMin, :declMin, :raMax, :declMax)
AND    extendedParameter > 0.8
