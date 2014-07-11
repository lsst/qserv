#!/bin/bash -x

# Standard LSST install procedure

STACK_DIR=$HOME/stack
NEWINSTALL_URL=http://sw.lsstcorp.org/eupspkg/newinstall.sh
DISTSERVER_URL=http://lsst-web.ncsa.illinois.edu/~fjammes/qserv
#REF=u.fjammes.DM-622-g0d03590e30

rm -rf $STACK_DIR &&
mkdir $STACK_DIR &&
cd $STACK_DIR ||
{
    echo "Unable to go to install directory : ${STACK_DIR}"
    exit 1
}
echo
echo "Installing eups"
echo "==============="
echo
curl -O ${NEWINSTALL_URL} ||
{
    echo "Unable to download from ${NEWINSTALL_URL}"
    exit 1
}
time bash newinstall.sh
source loadLSST.sh

echo
echo "Installing Qserv"
echo "================"
echo
time eups distrib install qserv ${REF} -r ${DISTSERVER_URL} &&
setup qserv ||
{
    echo "Unable to install Qserv"
    exit 1
}
echo
echo "Installing Qserv integration tests datasets"
echo "==========================================="
echo
time eups distrib install qserv_testdata -r ${DISTSERVER_URL} &&
setup qserv_testdata ||
{
    echo "Unable to install Qserv test datasets"
    exit 1
}

echo
echo "Configuring Qserv"
echo "================="
echo
qserv-configure.py --all ||
{
    echo "Unable to configure Qserv as a mono-node instance"
    exit 1
}

echo
echo "Starting Qserv"
echo "=============="
echo
qserv-start.sh ||
{
    echo "Unable to start Qserv"
    exit 1
}

echo
echo "Running Qserv integration tests"
echo "==============================="
echo
qserv-test-integration.py ||
{
    echo "Integration tests failed"
    exit 1
}

echo
echo "Stopping Qserv"
echo "=============="
echo
qserv-stop.sh ||
{
    echo "Unable to stop Qserv"
    exit 1
}

