-- Select all variable objects in given area
-- http://dev.lsstcorp.org/trac/wiki/dbQuery008

SELECT objectId
FROM   Object
-- WHERE  areaSpec(-1, 0, 3, 10)
 WHERE ra_PS between -1 and 3 and decl_PS between 0 and 10
--   AND variability > 0.8
