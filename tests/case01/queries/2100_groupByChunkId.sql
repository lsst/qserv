

SELECT count(*) AS n, AVG(ra), AVG(decl), chunkId
FROM Object
GROUP BY chunkId;
