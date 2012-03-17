-- Select all galaxies in a given area
-- http://dev.lsstcorp.org/trac/wiki/dbQuery014

-- Missing in current schema: extendedParameter

SELECT objectId
FROM   Object
WHERE  areaSpec_box(:raMin, :declMin, :raMax, :declMax)
AND    extendedParameter > 0.8;

