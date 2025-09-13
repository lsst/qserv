-- Select all pairs  within some distance of points in region
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- based on https://dev.lsstcorp.org/trac/wiki/db/queries/022

SELECT o1.objectId, o2.objectId
FROM Object o1, Object o2 
WHERE o1.ra_PS BETWEEN 0.04 AND 5.  -- noQserv
  AND o1.decl_PS BETWEEN -3. AND 3. -- noQserv
-- withQserv WHERE   qserv_areaspec_box(0.04, -3., 5., 3.)
  AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_Ps) < 5.

