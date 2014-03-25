WARN : The install procedure described in README.txt doesn't install current Qserv code, but the one which is
located on Qserv distribution server, please read eups documentation in order to know how to install this current distribution

In order to download data for test case, please run next commands 
in current directory :

  $ git submodule init
  $ git submodule update 

If you want to modify tests data, please clone Qserv test data repository :
  $ cd ~/src/
  $ git clone ssh://git@dev.lsstcorp.org/LSST/DMS/testdata/qservdata.git
  $ cd ~/src/qserv/
  $ git submodule update --remote
