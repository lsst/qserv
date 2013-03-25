-- Find an object with a particular object id
-- http://dev.lsstcorp.org/trac/wiki/dbQuery009

-- note that the data for mysql version is specially
-- precooked for this object to include chunkId and
-- subchunkId

SELECT ra_PS+ra_PS_Sigma
FROM   Object
WHERE  objectId = 430213989148129