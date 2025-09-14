-- The query is not supported by Qserv because of:
-- ERROR 4110 (Proxy): Query processing error: QI=?: Failed to instantiate query: AnalysisError:Query
--   involves partitioned table joins that Qserv does not know how to evaluate using only
--   partition-local data

-- Find near-neighbor objects in a given region 
-- https://dev.lsstcorp.org/trac/wiki/db/queries/022

SELECT o1.objectId AS objId1, 
        o2.objectId AS objId2,
        scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance
 FROM   Object o1, 
        Object o2
 WHERE  o1.ra_PS BETWEEN 1.28 AND 1.38
   AND  o1.decl_PS BETWEEN 3.18 AND 3.34
   AND  scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1
   AND  o1.objectId <> o2.objectId
