-- Find near-neighbor objects in a given region
--
-- Variant of 1051_nn.sql that expresses the area restrictor using
-- scisql_angSep(...) <= r instead of qserv_areaspec_box(...)

-- pragma sortresult

SELECT o1.objectId AS objId1,
       o2.objectId AS objId2,
       scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance
  FROM Object o1,
       Object o2
 WHERE scisql_angSep(o1.ra_PS, o1.decl_PS, 1.25, 3.35) <= 0.08
   AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.016
   AND o1.objectId <> o2.objectId
