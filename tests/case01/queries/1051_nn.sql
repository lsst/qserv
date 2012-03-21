-- Find near-neighbor objects in a given region

SELECT o1.objectId AS objId1,
       o2.objectId AS objId2,
       scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance
  FROM Object o1, 
       Object o2
-- WHERE areaSpec_box(0, 3, 0.2, 5)
 WHERE o1.ra_PS between 0 and 0.2 and o1.decl_PS between 3 and 5
   AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1
   AND o1.objectId <> o2.objectId