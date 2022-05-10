-- Select all variable objects in given area
-- http://dev.lsstcorp.org/trac/wiki/dbQuery008

-- tests using a comparison expression with a function in both sides of an AND

SELECT objectId
FROM   Object
WHERE  scisql_s2PtInCircle(ra_PS, decl_PS, 1.5, 3, 1) = 1
AND scisql_s2PtInCircle(ra_PS, decl_PS, 1.6, 3, 1) = 1
ORDER BY objectId
