-- Qserv needs to figure out what the director table
-- is and then retrieve chunkID from secondary index

select COUNT(*) AS N
FROM Source
WHERE objectId IN (386950783579546, 386942193651348)

