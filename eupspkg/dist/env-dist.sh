# use ../env.sh to set INSTALL_DIR

# used for package distribution only :
export REPOSITORY_BASE_CONTRIB=git://git.lsstcorp.org/contrib/eupspkg
export REPOSITORY_BASE_DMS=git://git.lsstcorp.org/LSST/DMS
export EUPSPKG_REPOSITORY_PATH='git://git.lsstcorp.org/contrib/eupspkg/$PRODUCT|git://git.lsstcorp.org/LSST/DMS/$PRODUCT'
export EUPSPKG_SOURCE=git
export LOCAL_PKGROOT=${INSTALL_DIR}/${DISTSERVERNAME}
export DEPS_DIR=${QSERV_SRC_DIR}/eupspkg/dist/dependencies

export QSERV_REPO=git://dev.lsstcorp.org/LSST/DMS/qserv

export DATA_REPO=git://dev.lsstcorp.org/LSST/DMS/testdata/qservdata.git
export DATA_BRANCH=master
