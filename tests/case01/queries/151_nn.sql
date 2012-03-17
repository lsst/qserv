-- Find near-neighbor objects in a given region

SELECT o1.objectId AS objId1 
       o2.objectId AS objId2
       spDist(o1.ra, o1.decl, o2.ra, o2.decl) AS distance
  FROM Object o1, 
       Object o2
 WHERE areaSpec_box(1, 20, 1.1, 20.2)
   AND spDist(o1.ra, o1.decl, o2.ra, o2.decl) < 0.1
   AND o1.objectId <> o2.objectId