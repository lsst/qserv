
----
TEMPORARY QUICK AND DIRTY START GUIDE

in order to deploy python package associated to this feature run next command 
in QSERV_SRC/tests directory :
cp -r python/* /opt/qserv-dev/lib/python2.6/site-packages/
where /opt/qserv-dev/ is your qserv install base directory.

then create a user configuration file for Qserv (at the moment it's the same
than the build configuration file):
  $ mkdir ~/.lsst/
  $ ln -s /home/qserv/src/qserv/qserv-build.conf ~/.lsst/qserv.conf  
  $ ln -s /home/qserv/src/qserv/qserv-build.default.conf ~/.lsst/qserv.default.conf 

Check that Qserv is running and then run in QSERV_SRC/tests :
./runTest.py
or 
./runTest.py --stop-at=8000

In order to load PT1.1 data set :
cd case02/
ln -s /data/lsst/pt11 data
where /data/lsst/pt11 contains unzipped pt11 data
./runTest.py --case-no=02


Results are stored in /opt/qserv-dev/tmp/qservTestCase??/outputs/, and erased
before each run.
-----

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
  8xxx - queries with bad syntax. They can fail, but should not crash the server
  9xxx - unknown support

files that are not yet supported should have extension .FIXME


./runTest.py --authFile=/u/sf/becla/.lsst/dbAuth.txt -o /tmp -s 7999
