-- Find an object and its associated sources
-- https://dev.lsstcorp.org/trac/wiki/db/queries/difficulty 

SELECT s.ra, s.decl, o.raRange, o.declRange 
FROM Object o 
JOIN Source s USING (objectId) 
WHERE o.objectId = 390030275138483;
