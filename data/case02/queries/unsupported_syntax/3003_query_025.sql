-- Subqueries aren't allowed in Qserv

-- Select pair of objects in dense region
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Based on https://dev.lsstcorp.org/trac/wiki/db/queries/025

SELECT DISTINCT o1.objectId, o1.ra_PS, o1.decl_PS, o2.iauId
FROM   Object o1, Object o2
WHERE  ABS(o2.ra_PS   - o1.ra_PS  ) < o2.raRange/(2*COS(RADIANS(o1.decl_PS)))
   AND ABS(o2.decl_PS - o1.decl_PS) < o2.declRange/2 
   AND (
        SELECT COUNT(o3.objectId)
        FROM   Object o3
        WHERE  o1.objectId <> o3.objectId
          AND  ABS(o1.ra_PS   - o3.ra_PS  ) < 0.1/COS(RADIANS(o3.decl_PS))
          AND  ABS(o1.decl_PS - o3.decl_PS) < 0.1
       ) > 1000
