-- Find near-neighbor objects in a given region


-- See ticket #1840

-- pragma sortresult

SELECT o1.objectId AS objId1,
       o2.objectId AS objId2,
       scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance
  FROM Object o1, 
       Object o2
 WHERE o1.ra_PS BETWEEN 1.2 AND 1.3 -- noQserv
   AND o1.decl_PS between 3.3 and 3.4 -- noQserv
   AND o2.ra_PS BETWEEN 1.2 AND 1.3 -- noQserv
   AND o2.decl_PS between 3.3 and 3.4 -- noQserv
-- withQserv WHERE qserv_areaspec_box(1.2, 3.3, 1.3, 3.4)
   AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.016
   AND o1.objectId <> o2.objectId
