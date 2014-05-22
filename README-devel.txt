Quick start guide for developpers :
===================================

Setup current Qserv version in eups :
-------------------------------------

Once Qserv is installed (cf. README.txt), in order to install your Qserv development version please use next commands,

  | # path to your Qserv git repository
  | QSERV_SRC_DIR=${HOME}/src/qserv/
  | # Build and install your Qserv version
  | cd $QSERV_SRC_DIR
  | setup -r .
  | eupspkg -er build               # build
  | eupspkg -er install             # install to EUPS stack directory
  | eupspkg -er decl                # declare it to EUPS
  | # Enable your Qserv version, and dependencies, in eups
  | # $VERSION is available by using : 
  | eups list
  | setup qserv $VERSION

Updating test cases :
--------------------- 

If you want to modify tests datasets, please clone Qserv test data repository :
  | cd ~/src/
  | git clone ssh://git@dev.lsstcorp.org/LSST/DMS/testdata/qserv_testdata.git

In order to test it with your Qserv version :
  | QSERV_TESTDATA_SRC_DIR=${HOME}/src/qserv_testdata/
  | cd $QSERV_TESTDATA_SRC_DIR
  | setup -r .
  | eupspkg -er build               # build
  | eupspkg -er install             # install to EUPS stack directory
  | eupspkg -er decl                # declare it to EUPS
  | # Enable your Qserv version, and dependencies, in eups
  | # $VERSION is available by using : 
  | eups list
  | setup qserv_testdata $VERSION
