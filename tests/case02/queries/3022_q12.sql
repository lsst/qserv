SELECT o1.objectId, o2.objectId
FROM Object o1, Object o2 
WHERE o1.ra_PS BETWEEN 0. AND 0.5
  AND o1.decl_PS BETWEEN 0. AND 1.5
  AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_Ps) < 0.2
