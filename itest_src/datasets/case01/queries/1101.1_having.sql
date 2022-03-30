-- Tests that the having clause is handled properly
-- pragma sortresult
-- pragma noheader

SELECT objectId,
       MAX(raFlux) - MIN(raFlux)
FROM Source
GROUP BY objectId
HAVING MAX(raFlux)-MIN(raFlux) > 5;
