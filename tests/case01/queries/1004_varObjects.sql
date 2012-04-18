-- Select all variable objects in given area
-- http://dev.lsstcorp.org/trac/wiki/dbQuery008

SELECT objectId
FROM   Object
WHERE  ra_PS BETWEEN 0 AND 3   -- noQserv
  AND  decl_PS BETWEEN 0 AND 10 -- noQserv
-- withQserv WHERE qserv_areaspec_box(0, 0, 3, 10)
--   AND variability > 0.8
