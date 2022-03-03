-- Select all variable objects in given area
-- http://dev.lsstcorp.org/trac/wiki/dbQuery008

-- ORDER BY is performed by mysql-proxy but
-- AVG() still has to trigger aggregation
-- step. 

-- pragma noheader
SELECT objectId, AVG(ra_PS) as ra
FROM   Object
WHERE  ra_PS BETWEEN 0 AND 3   -- noQserv
  AND  decl_PS BETWEEN 0 AND 10 -- noQserv
-- withQserv WHERE qserv_areaspec_box(0, 0, 3, 10)
GROUP BY objectId
ORDER BY ra
