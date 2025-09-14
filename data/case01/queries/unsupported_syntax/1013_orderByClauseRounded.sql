-- Unsupported syntax:
-- ERROR 4110 (Proxy): Query processing error: QI=?: Failed to instantiate
--   query: ParseException:Error parsing query, near "ROUND(ABS(iE1_SG),3)",
--   qserv does not support functions in ORDER BY.

-- Variation of the previous query, with "round"
-- (This query does not have real scientific meaning..)

SELECT objectId, ROUND(iE1_SG, 3), ROUND(ABS(iE1_SG), 3)
FROM Object
WHERE iE1_SG between -0.1 and 0.1
ORDER BY ROUND(ABS(iE1_SG), 3);
