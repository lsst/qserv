
SELECT count(src.sourceId), avg(o.ra_PS), avg(o.decl_PS)
FROM Object o, Source src
WHERE ra_PS  BETWEEN 0. AND 1.
 AND decl_PS BETWEEN 0. AND 1.
 GROUP BY src.objectId ;
 
