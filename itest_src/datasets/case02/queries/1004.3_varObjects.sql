-- Select all variable objects in given area
-- http://dev.lsstcorp.org/trac/wiki/dbQuery008

SELECT objectId
FROM   Object
WHERE  scisql_s2PtInEllipse(ra_PS, decl_PS, 1.5, 3, 3500, 200, 0.5)
ORDER BY objectId
