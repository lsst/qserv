# use ../env.sh to set INSTALL_DIR

# used for package distribution only :
export REPOSITORY_BASE=git://git.lsstcorp.org/contrib/eupspkg
export EUPSPKG_REPOSITORY_PATH='git://git.lsstcorp.org/contrib/eupspkg/$PRODUCT'
export EUPSPKG_SOURCE=git
export VERSION=6.0.0rc1
export LOCAL_PKGROOT=$INSTALL_DIR/distserver
export DEPS_DIR=${QSERV_SRC_DIR}/eupspkg/dist/dependencies
