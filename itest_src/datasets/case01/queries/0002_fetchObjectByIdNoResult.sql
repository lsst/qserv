-- Find an object with a particular object id
-- http://dev.lsstcorp.org/trac/wiki/dbQuery009

-- not working, see ticket #1847

-- pragma sortresult
SELECT *
FROM   Object
WHERE  objectId = 430213989000
