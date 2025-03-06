SELECT COUNT(*)
  FROM Object
  WHERE scisql_s2PtInBox(ra_PS,decl_PS,1,3,2,4);