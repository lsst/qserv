#!/bin/sh

set -e

# First, with lsstsw tools : 
# rebuild lsst qserv qserv_testdata
# publish -t qserv -b bXXX git lsst qserv_distrib
# publish with current tag will add newinstall.sh to distserver
# publish -t current -b bXXX git lsst qserv_distrib

############################
# CUSTOMIZE NEXT PARAMETER :
############################

# above directory must be published via a webserver
PUBLIC_HTML=${HOME}/qserv-www

#############################

if [[ -z ${LSSTSW} ]]; then
    echo "ERROR : Please setup lsstsw tools before running this script"
    exit 1
fi

source ${LSSTSW}/etc/settings.cfg.sh
DISTSERVER_ROOT=${LSSTSW}/distserver

EUPS_VERSION=1.5.0
EUPS_TARBALL="$EUPS_VERSION.tar.gz"
EUPS_TARURL="https://github.com/RobertLuptonTheGood/eups/archive/$EUPS_TARBALL"
EUPS_GITREPO="git://github.com/RobertLuptonTheGood/eups.git"

git_update_bare() {
    if [ -z "$1" ]; then
        echo "git_update_bare() requires one arguments"
        exit 1
    fi
    local giturl=$1
    local product=$(basename ${giturl})
    local retval=1

    if [ ! -d ${product} ]; then
        echo "Cloning ${giturl}"
        git clone --bare ${giturl} ||
        git remote add origin  ${giturl} && retval=0
    else
        echo "Updating ${giturl}"
        cd ${product}
        git fetch origin +refs/heads/*:refs/heads/* && retval=0
        cd .. 
    fi

    if [ ! retval ]; then 
        echo "ERROR : git update failed"
    fi  
    return ${retval}
}

if [ ! -d ${DISTSERVER_ROOT} ]; then
    mkdir ${DISTSERVER_ROOT} ||
    {
        echo "Unable to create local distribution directory : ${DISTSERVER_ROOT}"
        exit 1
    }

fi
cd ${DISTSERVER_ROOT} ||
{
    echo "Unable to go to local distribution directory directory : ${DISTSERVER_ROOT}"
    exit 1
}

echo
echo "Downloading eups tarball"
echo "========================"
echo
if [ ! -s ${EUPS_TARBALL} ]; then
    curl -L ${EUPS_TARURL} > ${EUPS_TARBALL} ||
    {
        echo "Unable to download eups tarball from ${EUPS_TARURL}"
        exit 1
    }
fi

echo
echo "Checking for distribution server data existence"
echo "==============================================="
echo
if [ ! -d ${EUPS_PKGROOT} ]; then
    echo "Directory for distribution server data doesn't exist."
    echo "Please create it using lsstsw with package mode"
    exit 1
fi

if ! git_update_bare ${EUPS_GITREPO}; then
    echo "Unable to synchronize with next git repository : ${EUPS_GITREPO}"
    exit 1
fi

EUPS_TARURL="file://${DISTSERVER_ROOT}/$EUPS_TARBALL"
EUPS_GITREPO="${DISTSERVER_ROOT}/eups.git"

echo
echo "Adding Qserv install script"
echo "==========================="
echo
cp ${BUILDDIR}/qserv/admin/tools/qserv-install.sh ${DISTSERVER_ROOT}

echo
echo "Creating Qserv internet-free distserver tarball"
echo "========================================="
echo
TOP_DIR=`basename ${DISTSERVER_ROOT}`
TARBALL=${PUBLIC_HTML}/qserv-internet-free-distserver.tar.gz
mkdir -p ${PUBLIC_HTML}
tar zcvf ${TARBALL} -C ${DISTSERVER_ROOT}/.. ${TOP_DIR} ||
{
    echo "Unable to create ${TARBALL}"
    exit 1
}

echo "Offline distribution server archive creation SUCCESSFUL"
echo "DISTSERVER_ROOT=${DISTSERVER_ROOT}"
echo "TARBALL=${TARBALL}"
