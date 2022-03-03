-- Select transient variable objects near a known galaxy
-- http://dev.lsstcorp.org/trac/wiki/dbQuery015

-- we don't have variability and extendedParamer columns

-- Our case01 dataset is fairly sparse, all objects are futher
-- apart than is supported by qserv near neighbor queries.
-- To allow this test to run, object 90030275138483 was duplicated
-- and its ra_PS value was changed slightly so that it would appear
-- to be a usable near neighbor.

SELECT v.objectId, v.ra_PS, v.decl_PS
FROM   Object v, Object o
WHERE  o.objectId = 90030275138483
   AND o.objectId != v.objectId
   AND scisql_angSep(v.ra_PS, v.decl_PS, o.ra_PS, o.decl_PS) < 0.016666
   AND v.rFlux_PS_Sigma > 1e-32;
