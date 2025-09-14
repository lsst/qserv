-- The query syntax is not supported in Qserv:
-- ERROR 4110 (Proxy): Query processing error: QI=?: Failed to instantiate query: ParseException:Error parsing query, near "ABS(iE1_SG)", qserv does not support functions in ORDER BY.

-- Just testing ORDER BY <clause>
-- (This query does not have real scientific meaning..)

SELECT objectId, iE1_SG, ABS(iE1_SG)
FROM Object
WHERE iE1_SG between -0.1 and 0.1
ORDER BY ABS(iE1_SG);
