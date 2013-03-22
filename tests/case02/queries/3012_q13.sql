SELECT count(*)
 FROM Object
 WHERE scisql_angSep(ra_PS, decl_PS, 0., 0.) < 0.2
