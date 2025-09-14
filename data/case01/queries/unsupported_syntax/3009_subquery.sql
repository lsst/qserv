
SELECT src.sourceId
FROM Source src
WHERE src.objectId IN (
  SELECT objectId
  FROM Object o
  WHERE ra_PS  BETWEEN 0. AND 1.
   AND decl_PS BETWEEN 0. AND 1. 
) ;
