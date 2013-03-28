-- Select pair of near neighbors object
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Based on https://dev.lsstcorp.org/trac/wiki/db/queries/022

SELECT o1.objectId AS objId1, 
        o2.objectId AS objId2,
        scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance
 FROM   Object o1, 
        Object o2
 WHERE  o1.ra_PS BETWEEN 1.28 AND 1.38
   AND  o1.decl_PS BETWEEN 3.18 AND 3.34
   AND  scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.1
   AND  o1.objectId <> o2.objectId
