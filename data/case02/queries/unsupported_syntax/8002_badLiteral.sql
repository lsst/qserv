

-- note wrong literal: "35. 1" instead of "35.1"

SELECT count(*) 
FROM Object
WHERE ra_PS BETWEEN 35 AND 35. 1 -- noQserv
  AND decl_PS BETWEEN 6 AND 6.0001 -- noQserv
-- withQserv WHERE qserv_areaSpec_box(35, 6, 35. 1, 6.0001);

