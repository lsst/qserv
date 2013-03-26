-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013

SELECT objectId 
FROM Source 
JOIN Object USING(objectId) 
WHERE ra_PS BETWEEN 1.28 AND 1.38
 AND  decl_PS BETWEEN 3.18 AND 3.34

