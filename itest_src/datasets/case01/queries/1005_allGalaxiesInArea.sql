-- Select all galaxies in a given area
-- http://dev.lsstcorp.org/trac/wiki/dbQuery014

-- pragma sortresult

SELECT objectId
FROM   Object
WHERE  ra_PS BETWEEN 1.0 AND 2.0       -- noQserv
   AND decl_PS BETWEEN -6.0 AND -4.0 -- noQserv
-- withQserv WHERE qserv_areaspec_box(1.0, -6.0, 2.0, -4.0)
AND    rRadius_SG > 0.5
