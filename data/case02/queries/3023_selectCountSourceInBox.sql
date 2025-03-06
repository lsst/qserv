SELECT COUNT(*)
  FROM Source
  WHERE scisql_s2PtInBox(ra,decl,1,3,2,4);