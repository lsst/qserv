
-- this query exercises secondary index in an interesting way:
-- because Qserv needs to figure out what the director table
-- is and use it when setting up its secondary index.

SELECT sourceId, objectId
FROM Source
WHERE objectId = 386942193651348
ORDER BY sourceId;
