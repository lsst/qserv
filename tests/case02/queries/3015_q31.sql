SELECT o1.objectId AS objId1, 
        o2.objectId AS objId2,
        scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance
 FROM   Object o1, 
        Object o2
 WHERE o1.ra_PS BETWEEN 0 AND 0.02
   AND o2.ra_PS BETWEEN 0 AND 0.02
   AND o1.decl_PS BETWEEN 0.01 AND 0.03
   AND o2.decl_PS BETWEEN 0.01 AND 0.03
   AND o1.objectId <> o2.objectId
