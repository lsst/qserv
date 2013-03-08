-- This is testing syntax (limit after areaspec)

-- See ticket #2200

SELECT COUNT(*) 
FROM   Object 
WHERE  ra_PS BETWEEN 355 AND 356 -- noQserv
  AND  decl_PS BETWEEN 0 AND 1   -- noQserv
-- withQserv WHERE qserv_areaspec_box(355, 0, 356, 1)
LIMIT 10
