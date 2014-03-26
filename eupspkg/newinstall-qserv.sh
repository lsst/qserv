#!/usr/bin/env bash
#
# Usage: sh newinstall.sh [package ...]
#
#  Set up some initial environment variables
#
# set -x
SHELL=/bin/bash
INSTALL_DIR=$PWD
export EUPS_PKGROOT="http://datasky.in2p3.fr/qserv/distserver"

QSERV_REPO=git://dev.lsstcorp.org/LSST/DMS/qserv
QSERV_BRANCH=tickets/3100

while [ $# -gt 0 ]; do
    case "$1" in 
        -H) INSTALL_DIR="$2"; shift;;
        -r) EUPS_PKGROOT="$2"; shift;;
        *)  break;;
    esac
    shift
done
cd $INSTALL_DIR

export PREFIX=.qserv_install_scripts
git archive --remote=${QSERV_REPO} --format=tar --prefix=${PREFIX}/ ${QSERV_BRANCH} eupspkg | tar xf - || {
    echo "Failed to download Qserv install scripts"
    exit 2
}

INSTALLSCRIPT_DIR=${INSTALL_DIR}/${PREFIX}

CFG_FILE="${INSTALLSCRIPT_DIR}/eupspkg/env.sh"
/bin/cat <<EOM >$CFG_FILE
export INSTALL_DIR=${INSTALL_DIR}
export EUPS_PKGROOT=${EUPS_PKGROOT}
export EUPS_GIT_CLONE_CMD="git clone https://github.com/RobertLuptonTheGood/eups.git"
export EUPS_GIT_CHECKOUT_CMD="git checkout 1.3.0"
EOM

# TODO rename QSERV_SRC_DIR ?
export QSERV_SRC_DIR=${INSTALLSCRIPT_DIR}
${INSTALLSCRIPT_DIR}/eupspkg/install.sh || {
    echo "Failed to install Qserv using eups"
    exit 2
}
 
