-- Find near-neighbor objects in a given region


-- See ticket #1840

SELECT o1.objectId AS objId1,
       o2.objectId AS objId2,
       scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance
  FROM Object o1, 
       Object o2
 WHERE o1.ra_PS BETWEEN 0 AND 0.2 -- noQserv
   AND o1.decl_PS between 0 and 1 -- noQserv
-- withQserv WHERE qserv_areaspec_box(0, 0, 0.2, 1)
   AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1
   AND o1.objectId <> o2.objectId
