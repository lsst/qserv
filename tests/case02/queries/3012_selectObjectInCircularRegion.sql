-- Select object in a circular region
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013


SELECT count(*)
 FROM Object
 WHERE scisql_angSep(ra_PS, decl_PS, 0., 0.) < 0.2
