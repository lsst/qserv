-- Find an object with a particular object id
-- http://dev.lsstcorp.org/trac/wiki/dbQuery009

SELECT *
FROM   Object
WHERE  objectId = :objectId