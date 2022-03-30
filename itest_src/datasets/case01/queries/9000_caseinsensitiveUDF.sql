-- Test case-insensitive names for
-- Qserv spatial restrictor 
-- inspired by 1004_varObjects.sql

SELECT objectId
FROM   Object
WHERE  ra_PS BETWEEN 0 AND 3   -- noQserv
  AND  decl_PS BETWEEN 0 AND 10 -- noQserv
-- withQserv WHERE QsErV_ArEaSpEc_BoX(0, 0, 3, 10)
--   AND variability > 0.8
ORDER BY objectId
