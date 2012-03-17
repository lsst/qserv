-- Select all variable objects in given area
-- http://dev.lsstcorp.org/trac/wiki/dbQuery008

SELECT objectId
FROM   Object
WHERE  areaSpec(:raMin, :declMin, :raMax, :declMax)
AND    variability > 0.8;
