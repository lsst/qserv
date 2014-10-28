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
   -R      Qserv execution directory (configuration and data) 
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
                export EUPS_VERSION="1.5.0"
                export EUPS_TARURL=file://${LOCAL_DISTSERVER_ROOT}/${EUPS_VERSION}.tar.gz
                export EUPS_GIT_REPO=${LOCAL_DISTSERVER_ROOT}/eups.git
                ;;
        i)
                # Remove trailing slashes
                STACK_DIR=`echo "${OPTARG}" | sed 's#/*$##'`
                ;;
        R)
                QSERV_RUN_DIR=`echo "${OPTARG}" | sed 's#/*$##'`
                QSERV_RUN_DIR_OPT="-R ${QSERV_RUN_DIR}"
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

if [[ -d ${STACK_DIR} || -L ${STACK_DIR} ]]; then
    [ "$(ls -A ${STACK_DIR})" ] &&
    {
        echo "Cleaning install directory: ${STACK_DIR}"
        chmod -R 755 $STACK_DIR/* &&
        # / below is required if ${STACK_DIR} is a symlink
        find ${STACK_DIR}/ -mindepth 1 -delete ||
        {
            >&2 echo "Unable to remove install directory previous content : ${STACK_DIR}"
            exit 1
        }
    }
else
    mkdir $STACK_DIR ||
    {
        >&2 echo "Unable to create install directory ${STACK_DIR}"
        exit 1
    }

fi

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
if [[ -n ${LOCAL_OPTION} ]]; then
    EUPS_PKG_ROOT_BACKUP=${EUPS_PKGROOT}
fi
. ${STACK_DIR}/loadLSST.bash ||
{
    >&2 echo "ERROR : unable to load LSST stack environment"
    exit 1
}
if [[ -n ${LOCAL_OPTION} ]]; then
    export EUPS_PKGROOT=${EUPS_PKG_ROOT_BACKUP}
fi

echo
underline "Installing Qserv distribution (version: $VERSION, distserver: ${EUPS_PKGROOT})"
echo
time eups distrib install qserv_distrib ${VERSION} &&
setup qserv_distrib ${VERSION} ||
{
    >&2 echo "Unable to install Qserv"
    exit 1
}

echo
underline "Configuring Qserv"
echo
qserv-configure.py --all ${QSERV_RUN_DIR_OPT} ||
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
