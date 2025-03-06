-- Qserv needs to figure out what the director table
-- is and then retrieve chunkID from secondary index

select COUNT(*) AS N
FROM Source
WHERE objectId BETWEEN 386942193651348 AND 386950783579546

