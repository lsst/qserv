Create a user configuration file for Qserv (at the moment it's the same
than the build configuration file):
  $ mkdir ~/.lsst/
  $ ln -s /home/qserv/src/qserv/qserv-build.conf ~/.lsst/qserv.conf  
  $ ln -s /home/qserv/src/qserv/qserv-build.default.conf ~/.lsst/qserv.default.conf 

Check that Qserv is running and then run in QSERV_SRC/tests :
./runTest.py
or 
./runTest.py --stop-at=8000

In order to load and run queries against PT1.1 data set :
  $ cd case03/
  $ ln -s /data/lsst/pt11 data
where /data/lsst/pt11 contains unzipped pt11 data
  $ ln -s ../case02/queries queries
  $ ./runTest.py --case-no=03


Results are stored in /opt/qserv-dev/tmp/qservTest_case<number>/outputs/, and erased
before each run.
-----

Directory structure for a test case :
-------------------------------------
  case<number>/
    README.txt - contains info about data
    queries/
    data/
     <table>.schema - contains schema info per table
     <table>.csv.gz - contains data

data from case<number> will be loaded into databases called 
 - qservTest_case<number>_mysql
 - qservTest_case<number>_qserv

- format of the files containing queries: <idA>_<descr>.sql
where <idA>:
  0xxx - supported, trivial (single object)
  1xxx - supported, simple (small area)
  2xxx - supported, medium difficulty (full scan)
  3xxx - supported, difficult /expensive (e.g. full sky joins)
  4xxx - supported, very difficult (eg near neighbor for large area)
  8xxx - queries with bad syntax. They can fail, but should not crash the server
  9xxx - unknown support

files that are not yet supported should have extension .FIXME

