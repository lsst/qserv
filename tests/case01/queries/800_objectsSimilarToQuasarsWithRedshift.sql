-- Find all objects similar to the colors of a quasar 
-- with redshift in a given range
-- http://dev.lsstcorp.org/trac/wiki/dbQuery012

-- Missing in current schema: ObjectToType

SELECT  COUNT(*)                                               AS totalCount,
        SUM(CASE WHEN (typeId=3) THEN 1 ELSE 0 END)            AS galaxyCount,
        SUM(CASE WHEN (typeId=6) THEN 1 ELSE 0 END)            AS starCount,
        SUM(CASE WHEN (typeId NOT IN (3,6)) THEN 1 ELSE 0 END) AS otherCount
FROM    Object
JOIN    _Object2Type USING(objectId)
WHERE  (uMag-gMag > 2.0 OR uMag > 22.3)
   AND iMag BETWEEN 0 AND 19 
   AND gMag - rMag > 1.0 
   AND ( (rMag-iMag < 0.08 + 0.42 * (gMag-rMag - 0.96)) OR (gMag-rMag > 2.26 ) )
   AND iMag-zMag < 0.25