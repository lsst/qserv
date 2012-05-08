-- Find an object with a particular object id
-- http://dev.lsstcorp.org/trac/wiki/dbQuery009

-- note that the data for mysql version is specially
-- precooked for this object to include chunkId and
-- subchunkId

SELECT *
FROM   Object
WHERE  objectId = 430213989148129