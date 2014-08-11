#!/bin/bash

# Standard LSST install procedure
set -e
DIR=$(cd "$(dirname "$0")"; pwd -P)
# . $DIR/../etc/settings.cfg.sh

usage()
{
cat << EOF
usage: $0 [-r <path/to/local/distserver>] [-i <path/to/install/dir>] [-v <version>] 

This script install Qserv according to LSST packaging standards.

OPTIONS:
   -h      Show this message
   -r      Local distribution server root directory 
   -i      Install directory : MANDATORY
   -v      Version to instll 
EOF
}

while getopts ":r:i:v:h" o; do
        case "$o" in
        r)
                LOCAL_DISTSERVER_ROOT="${OPTARG}"
                ;;
        i)
                STACK_DIR="${OPTARG}"
                ;;
        v)
                VERSION="${OPTARG}"
                ;;
        h)
                usage
                ;;
        esac
done

if [[ -z ${STACK_DIR} ]]
then
     usage
     exit 1
fi

if [ -d ${STACK_DIR} ]; then
    chmod -R 755 $STACK_DIR &&
    rm -rf $STACK_DIR ||
    {
        echo "Unable to remove install directory previous content : ${STACK_DIR}"
        exit 1
    }
fi
mkdir $STACK_DIR &&
cd $STACK_DIR ||
{
    echo "Unable to go to install directory : ${STACK_DIR}"
    exit 1
}
echo
echo "Installing LSST stack"
echo "====================="
echo

if [ -n ${LOCAL_DISTSERVER_ROOT} ]; then
    echo
    echo "Offline mode"
    echo "============"
    echo
    export EUPS_PKGROOT="${LOCAL_DISTSERVER_ROOT}/production"
    NEWINSTALL_URL="file://${EUPS_PKGROOT}/newinstall.sh"
    export EUPS_TARURL=file://${LOCAL_DISTSERVER_ROOT}/1.5.0.tar.gz
    export EUPS_GIT_REPO=${LOCAL_DISTSERVER_ROOT}/eups.git
else
    export EUPS_PKGROOT="http://sw.lsstcorp.org/eupspkg|http://lsst-web.ncsa.illinois.edu/~fjammes/qserv"
    NEWINSTALL_URL="http://sw.lsstcorp.org/eupspkg/newinstall.sh"
fi

curl -O ${NEWINSTALL_URL} ||
{
    echo "Unable to download from ${NEWINSTALL_URL}"
    exit 1
}

time bash newinstall.sh ||
{
    echo "ERROR : newinstall.sh failed"
    exit 1
}
echo "XXXXXXXXXXXXXXXx"
source loadLSST.sh

echo
echo "Installing Qserv"
echo "================"
echo
time eups distrib install qserv ${VERSION} &&
setup qserv ||
{
    echo "Unable to install Qserv"
    exit 1
}
echo
echo "Installing Qserv integration tests datasets"
echo "==========================================="
echo
time eups distrib install qserv_testdata &&
setup qserv_testdata ||
{
    echo "Unable to install Qserv test datasets"
    exit 1
}


rm ${GIT_CONFIG}

echo
echo "Configuring Qserv"
echo "================="
echo
cd $QSERV_DIR/admin &&
qserv-configure --all ||
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
