-- Cone-magnitude-color search
-- See http://dev.lsstcorp.org/trac/wiki/dbQuery003

SELECT COUNT(*)
FROM   Object
WHERE  areaSpec_box(1, 2, 1.1, 2.1)
   AND zMag      BETWEEN 1 AND 1.1
   AND gMag-rMag BETWEEN 2 AND 2.1
   AND iMag-zMag BETWEEN 3 AND 3.1;
