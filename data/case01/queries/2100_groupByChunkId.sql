-- chunkId column must be filled in input data so that mysql mode
-- can give same answers as Qserv

SELECT count(*) AS n, AVG(ra_PS), AVG(decl_PS), chunkId
FROM Object
GROUP BY chunkId;
