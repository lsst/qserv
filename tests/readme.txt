
directory structure:
  case<number>
    queries
    data.sql.gz

data from case<number> will be loaded into databases called 
 - qservTest_case<number>_m
 - qservTest_case<number>_q




format of the files containing queries: <idA>_<descr>.sql
where <idA>:
  0xxx - supported, trivial (single object)
  1xxx - supported, simple (small area)
  2xxx - supported, medium difficulty (full scan)
  3xxx - supported, difficult /expensive (e.g. full sky joins)
  4xxx - supported, very difficult (eg near neighbor for large area)
  8xxx - will be supported in the future
  9xxx - unknown support

./runTest.py --authFile=/u/sf/becla/.lsst/dbAuth.txt -o /tmp -s 7999
