-- Note: result ordering before limiting th eresult is required in Qserv
-- to get the deterministic output.

-- This is testing syntax (limit after areaspec)

-- See ticket #2200

SELECT objectId
FROM Object
WHERE ra_PS BETWEEN 0.1 AND 4  -- noQserv
  AND decl_PS BETWEEN -6 AND 6 -- noQserv
-- withQserv WHERE qserv_areaspec_box(0.1, -6, 4, 6)
ORDER BY objectId
LIMIT 10
