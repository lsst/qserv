#!/bin/bash

# Standard LSST install procedure
set -e
DIR=$(cd "$(dirname "$0")"; pwd -P)
# . $DIR/../etc/settings.cfg.sh

# Default values below may be overidden by cmd-line options
MODE="internet mode"
DEV_DISTSERVER_ROOT="http://lsst-web.ncsa.illinois.edu/~fjammes/qserv-dev"
EUPS_PKGROOT="http://sw.lsstcorp.org/eupspkg"
NEWINSTALL_URL="https://sw.lsstcorp.org/eupspkg/newinstall.sh"
VERSION="-t qserv"

underline() {
    printf "\n%s\n" "${1}"
    printf "%s" "${1}" | sed "s/./=/g"
    printf "\n\n";
}

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
   -i      Install directory : MANDATORY, MUST BE AN ABSOLUTE PATH
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
                if [ ! -d "${LOCAL_DISTSERVER_ROOT}" ]; then
                    >&2 printf "ERROR : %s require a local distribution\
 server\n" "$MODE"
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
                STACK_DIR=`printf "%s" "${OPTARG}" | sed 's#/*$##'`
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


if [ -z "${STACK_DIR}" ]
then
     >&2 printf "ERROR : install directory required, use -i option.\n"
     usage
     exit 1
fi

if [ -n "${DEV_OPTION}" -a -n "${LOCAL_OPTION}" ]; then
    >&2 printf "ERROR : -r and -d options are not compatible\n"
    usage
    exit 1
fi

if [ -d "${STACK_DIR}" -o -L "${STACK_DIR}" ]; then
    [ "$(ls -A ${STACK_DIR})" ] &&
    {
        printf "Cleaning install directory: ${STACK_DIR}\n"
        chmod -R 755 $STACK_DIR/* &&
        # / below is required if ${STACK_DIR} is a symlink
        find ${STACK_DIR}/ -mindepth 1 -delete ||
        {
            >&2 printf "Unable to remove install directory previous content:\
 ${STACK_DIR}\n"
            exit 1
        }
    }
else
    mkdir $STACK_DIR ||
    {
        >&2 printf "Unable to create install directory ${STACK_DIR}\n"
        exit 1
    }

fi

cd $STACK_DIR ||
{
    >&2 printf "Unable to go to install directory : ${STACK_DIR}\n"
    exit 1
}

underline "Installing LSST stack : $MODE, version : $VERSION"
export EUPS_PKGROOT
curl -O ${NEWINSTALL_URL} ||
{
    >&2 printf "Unable to download from ${NEWINSTALL_URL}\n"
    exit 2
}

time bash newinstall.sh ||
{
    >&2 printf "ERROR : newinstall.sh failed\n"
    exit 1
}

# TODO : warn loadLSST.bash append http://sw.lsstcorp.org/eupspkg to
# EUPS_PKGROOT, this isn't compliant with internet-free mode
# TODO : if first url in EUPS_PKGROOT isn't available eups fails without
# trying next ones
if [ -n "${LOCAL_OPTION}" ]; then
    EUPS_PKG_ROOT_BACKUP=${EUPS_PKGROOT}
fi
. ${STACK_DIR}/loadLSST.bash ||
{
    >&2 printf "ERROR : unable to load LSST stack environment\n"
    exit 1
}
if [ -n "${LOCAL_OPTION}" ]; then
    export EUPS_PKGROOT=${EUPS_PKG_ROOT_BACKUP}
fi

underline "Installing Qserv distribution (version: $VERSION, distserver: ${EUPS_PKGROOT})"
time eups distrib install qserv_distrib ${VERSION} &&
setup qserv_distrib ${VERSION} ||
{
    >&2 printf "Unable to install Qserv\n"
    exit 1
}

underline "Configuring Qserv"
qserv-configure.py --all ||
{
    >&2 printf "Unable to configure Qserv as a mono-node instance\n"
    exit 1
}

underline "Starting Qserv"
CFG_VERSION=`qserv-version.sh`
${HOME}/qserv-run/${CFG_VERSION}/bin/qserv-start.sh ||
{
    >&2 printf "Unable to start Qserv\n"
    exit 1
}

underline "Running Qserv integration tests"
qserv-test-integration.py ||
{
    >&2 printf "Integration tests failed\n"
    exit 1
}

underline "Stopping Qserv"
${HOME}/qserv-run/${CFG_VERSION}/bin/qserv-stop.sh ||
{
    >&2 printf "Unable to stop Qserv\n"
    exit 1
}
