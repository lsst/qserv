-- Select all variable objects in given area
-- http://dev.lsstcorp.org/trac/wiki/dbQuery008

SELECT objectId
FROM   Object
WHERE  scisql_s2PtInCPoly(ra_PS, decl_PS, 0, 0, 3, 10, 0, 5, 3, 1) = 1 -- noQserv
-- withQserv WHERE qserv_areaspec_poly(0, 0, 3, 10, 0, 5, 3, 1)
--   AND variability > 0.8
ORDER BY objectId
