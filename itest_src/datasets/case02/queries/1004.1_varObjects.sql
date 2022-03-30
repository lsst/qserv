-- Select all variable objects in given area
-- http://dev.lsstcorp.org/trac/wiki/dbQuery008

-- Test support for multiple ORDER BY fields

SELECT objectId, ra_PS, decl_PS
FROM   Object
WHERE  ra_PS BETWEEN 0 AND 3   -- noQserv
  AND  decl_PS BETWEEN 0 AND 10 -- noQserv
-- withQserv WHERE qserv_areaspec_box(0, 0, 3, 10)
--   AND variability > 0.8
ORDER BY objectId, ra_PS, decl_PS
