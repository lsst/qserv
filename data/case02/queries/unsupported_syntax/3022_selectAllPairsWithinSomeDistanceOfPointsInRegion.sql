-- Qserv fails to execute the query because of:
-- ERROR 4110 (Proxy): Query processing error: QI=?: Failed to instantiate
--   query: AnalysisError:Query involves partitioned table joins that Qserv
--   does not know how to evaluate using only partition-local data

-- Select all pairs  within some distance of points in region
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- based on https://dev.lsstcorp.org/trac/wiki/db/queries/022

SELECT o1.objectId, o2.objectId
FROM Object o1, Object o2 
WHERE o1.ra_PS BETWEEN 0.04 AND 5.  -- noQserv
  AND o1.decl_PS BETWEEN -3. AND 3. -- noQserv
-- withQserv WHERE   qserv_areaspec_box(0.04, 5., -3., 3.)
  AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_Ps) < 5.

