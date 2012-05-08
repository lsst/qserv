-- Select transient variable objects near a known galaxy
-- http://dev.lsstcorp.org/trac/wiki/dbQuery015

SELECT v.objectId, v.ra, v.decl
FROM   Object v, Object o
WHERE  o.objectId = :objectId
   AND spDist(v.ra, v.decl, o.ra, o.decl, :dist)
   AND v.variability > 0.8
   AND o.extendedParameter > 0.8

