-- Bad area spec box

SELECT o1.objectId, o2.objectId
FROM Object o1, Object o2 
WHERE o1.ra_PS BETWEEN 0.04 AND -3.  -- noQserv
  AND o1.decl_PS BETWEEN 5. AND 3. -- noQserv
-- withQserv WHERE   qserv_areaspec_box(0.04, 5., -3., 3.)
  AND scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_Ps) < 5.

