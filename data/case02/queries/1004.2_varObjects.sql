-- Select all variable objects in given area
-- http://dev.lsstcorp.org/trac/wiki/dbQuery008

SELECT objectId
FROM   Object
WHERE  scisql_s2PtInCircle(ra_PS, decl_PS, 1.5, 3, 1)
ORDER BY objectId
