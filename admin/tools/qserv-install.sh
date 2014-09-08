#!/bin/bash

# Standard LSST install procedure
set -e
DIR=$(cd "$(dirname "$0")"; pwd -P)
# . $DIR/../etc/settings.cfg.sh

usage()
{
cat << EOF
Usage: $0 [[-r <path/to/local/distserver>]] [[-i <path/to/install/dir>]] [[-v <version>]] 
This script install Qserv according to LSST packaging standards.

OPTIONS:
   -h      Show this message and exit
   -r      Local distribution server root directory, 
           used in internet-free mode
   -i      Install directory : MANDATORY
   -v      Qserv version to install 
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
                exit 1
                ;;
        esac
done

if [[ -z "${STACK_DIR}" ]]
then
     usage
     exit 1
fi

if [[ -d ${STACK_DIR} ]]; then
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
if [[ -z "${LOCAL_DISTSERVER_ROOT}" ]]; then
    echo
    echo "Online mode"
    echo "==========="
    echo
    export EUPS_PKGROOT="http://sw.lsstcorp.org/eupspkg|http://lsst-web.ncsa.illinois.edu/~fjammes/qserv"
    NEWINSTALL_URL="http://sw.lsstcorp.org/eupspkg/newinstall.sh"
else
    echo
    echo "Offline mode"
    echo "============"
    echo
    export EUPS_PKGROOT="${LOCAL_DISTSERVER_ROOT}/production"
    NEWINSTALL_URL="file://${EUPS_PKGROOT}/newinstall.sh"
    export EUPS_TARURL=file://${LOCAL_DISTSERVER_ROOT}/1.5.0.tar.gz
    export EUPS_GIT_REPO=${LOCAL_DISTSERVER_ROOT}/eups.git
fi

curl -O ${NEWINSTALL_URL} ||
{
    echo "Unable to download from ${NEWINSTALL_URL}"
    exit 2
}

time bash newinstall.sh ||
{
    echo "ERROR : newinstall.sh failed"
    exit 1
}


EUPS_PKGROOT_QSERV=${EUPS_PKGROOT}

# TODO : warn loadLSST.sh append http://sw.lsstcorp.org/eupspkg to
# EUPS_PKGROOT, this isn't compliant with internet-free mode
# TODO : if first url in EUPS_PKGROOT isn't available eups fails without
# trying next ones
. ${STACK_DIR}/loadLSST.sh ||
{
    echo "ERROR : unable to load LSST stack environment"
    exit 1
}

echo
echo "Installing Qserv"
echo "================"
echo
time eups distrib install qserv ${VERSION} -r ${EUPS_PKGROOT_QSERV} &&
setup qserv ${VERSION} ||
{
    echo "Unable to install Qserv"
    exit 1
}

echo
echo "Installing Qserv integration tests datasets"
echo "==========================================="
echo
time eups distrib install qserv_testdata -r ${EUPS_PKGROOT_QSERV} &&
setup qserv_testdata ||
{
    echo "Unable to install Qserv test datasets"
    exit 1
}

echo
echo "Configuring Qserv"
echo "================="
echo
cd $QSERV_DIR/admin &&
qserv-configure.py --all ||
{
    echo "Unable to configure Qserv as a mono-node instance"
    exit 1
}

echo
echo "Starting Qserv"
echo "=============="
echo
CFG_VERSION=`qserv-version.sh`
${HOME}/qserv-run/${CFG_VERSION}/bin/qserv-start.sh ||
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
${HOME}/qserv-run/${CFG_VERSION}/bin/qserv-stop.sh ||
{
    echo "Unable to stop Qserv"
    exit 1
}
