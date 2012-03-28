
directory structure:
  case<number>/
    queries/
    data/
     readme.txt - contains info about data
     <table>.schema - contains schema info per table
     <table>.csv.gz - contains data


To generate .schema and data files use:

mysqldump -u<user> -p<pass> <db> <table> -T/tmp/
then copy <table>.sql and <table>.txt


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
