SELECT objectId 
FROM Source 
JOIN Object USING(objectId) 
WHERE ra_PS BETWEEN 0. AND 0.1
  AND decl_PS BETWEEN 0.2 AND 0.3
