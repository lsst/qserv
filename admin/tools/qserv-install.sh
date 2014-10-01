#!/bin/bash

# Standard LSST install procedure
set -e
DIR=$(cd "$(dirname "$0")"; pwd -P)
# . $DIR/../etc/settings.cfg.sh

# Default values below may be overidden by cmd-line options
MODE="internet mode"
DEV_DISTSERVER_ROOT="http://lsst-web.ncsa.illinois.edu/~fjammes/qserv-dev"
EUPS_PKGROOT="http://sw.lsstcorp.org/eupspkg"
NEWINSTALL_URL="http://sw.lsstcorp.org/eupspkg/newinstall.sh"
VERSION="-t qserv"     

underline() { echo $1; echo "${1//?/${2:-=}}";}

usage()
{
cat << EOF
Usage: $0 [[-r <path/to/local/distserver>]] [[-i <path/to/install/dir>]] [[-v <version>]] 
This script install Qserv according to LSST packaging standards.

OPTIONS:
   -h      Show this message and exit
   -d      Use development distribution server: ${DEV_DISTSERVER_ROOT}  
   -r      Local distribution server root directory, 
           used in internet-free mode
   -i      Install directory : MANDATORY
   -v      Qserv version to install, default to the one with the 'qserv' tag
EOF
}

while getopts "dr:i:v:h" o; do
        case "$o" in
        d)
                DEV_OPTION=1
                MODE="development/internet mode"
                VERSION="-t qserv-dev"
                EUPS_PKGROOT="${EUPS_PKGROOT}|${DEV_DISTSERVER_ROOT}"
                ;;
        r)
                LOCAL_OPTION=1
                MODE="internet-free mode"
                LOCAL_DISTSERVER_ROOT="${OPTARG}"
                if [[ ! -d ${LOCAL_DISTSERVER_ROOT} ]]; then
                    >&2 echo "ERROR : $MODE require a local distribution server"
                    usage
                    exit 1
                fi 
                EUPS_PKGROOT="${LOCAL_DISTSERVER_ROOT}/production"
                NEWINSTALL_URL="file://${EUPS_PKGROOT}/newinstall.sh"
                export EUPS_TARURL=file://${LOCAL_DISTSERVER_ROOT}/1.5.0.tar.gz
                export EUPS_GIT_REPO=${LOCAL_DISTSERVER_ROOT}/eups.git
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
     >&2 echo "ERROR : install directory required, use -i option."
     usage
     exit 1
fi

if [[ -n ${DEV_OPTION} && -n ${LOCAL_OPTION} ]]; then
    >&2 echo "ERROR : -r and -d options are not compatible"
    usage
    exit 1
fi 

if [[ -d ${STACK_DIR} ]]; then
    chmod -R 755 $STACK_DIR &&
    rm -rf $STACK_DIR ||
    {
        >&2 echo "Unable to remove install directory previous content : ${STACK_DIR}"
        exit 1
    }
fi
mkdir $STACK_DIR &&
cd $STACK_DIR ||
{
    >&2 echo "Unable to go to install directory : ${STACK_DIR}"
    exit 1
}

echo
underline "Installing LSST stack : $MODE, version : $VERSION" 
echo
export EUPS_PKGROOT
curl -O ${NEWINSTALL_URL} ||
{
    >&2 echo "Unable to download from ${NEWINSTALL_URL}"
    exit 2
}

time bash newinstall.sh ||
{
    >&2 echo "ERROR : newinstall.sh failed"
    exit 1
}

# TODO : warn loadLSST.sh append http://sw.lsstcorp.org/eupspkg to
# EUPS_PKGROOT, this isn't compliant with internet-free mode
# TODO : if first url in EUPS_PKGROOT isn't available eups fails without
# trying next ones
. ${STACK_DIR}/loadLSST.sh ||
{
    >&2 echo "ERROR : unable to load LSST stack environment"
    exit 1
}

echo
underline "Installing Qserv distribution (version : $VERSION)"
echo
time eups distrib install qserv_distrib ${VERSION} -r ${EUPS_PKGROOT} &&
setup qserv_distrib ${VERSION} ||
{
    >&2 echo "Unable to install Qserv"
    exit 1
}

echo
underline "Configuring Qserv"
echo
cd $QSERV_DIR/admin &&
qserv-configure.py --all ||
{
    >&2 echo "Unable to configure Qserv as a mono-node instance"
    exit 1
}

echo
underline "Starting Qserv"
echo
CFG_VERSION=`qserv-version.sh`
${HOME}/qserv-run/${CFG_VERSION}/bin/qserv-start.sh ||
{
    >&2 echo "Unable to start Qserv"
    exit 1
}

echo
underline "Running Qserv integration tests"
echo
qserv-test-integration.py ||
{
    >&2 echo "Integration tests failed"
    exit 1
}

echo
underline "Stopping Qserv"
echo
${HOME}/qserv-run/${CFG_VERSION}/bin/qserv-stop.sh ||
{
    >&2 echo "Unable to stop Qserv"
    exit 1
}
