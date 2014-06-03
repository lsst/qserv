#!/bin/bash

# Standard LSST install procedure
STACK_DIR=$HOME/stack
rm -rf $STACK_DIR &&
mkdir $STACK_DIR &&
cd $STACK_DIR ||
{   
    echo "Unable to go to install directory : ${STACK_DIR}"
    exit 1
}
curl -O http://sw.lsstcorp.org/eupspkg/newinstall.sh
bash newinstall.sh
source loadLSST.sh

# Installing Qserv
eups distrib install qserv -r http://lsst-web.ncsa.illinois.edu/~fjammes/qserv &&
setup qserv ||
{   
    echo "Unable to install Qserv"
    exit 1
}

eups distrib install qserv_testdata -r http://lsst-web.ncsa.illinois.edu/~fjammes/qserv
setup qserv_testdata ||
{   
    echo "Unable to install Qserv test datasets"
    exit 1
}


# Configuring Qserv
cd $QSERV_DIR/admin
scons

# Testing Qserv
qserv-start.sh
qserv-testdata.py
