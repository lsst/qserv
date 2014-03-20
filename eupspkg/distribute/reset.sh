source env.sh

if [ -n ${INSTALL_DIR} ]; then
    rm -rf ${INSTALL_DIR}/*
fi
rm -rf ~/.eups/ups_db ~/.eups/_caches_

cd ${INSTALL_DIR}
mkdir distserver
mkdir tmp 

cd ${INSTALL_DIR}
mkdir sources
cd sources
# install eups latest version
# git clone git@github.com:RobertLuptonTheGood/eups.git
git clone ${EUPS_GIT_URL} ${EUPS_GIT_OPTIONS} 

cd eups/
./configure --prefix="${INSTALL_DIR}/eups" --with-eups="${INSTALL_DIR}/stack"
make
make install

# install git latest version
source "${INSTALL_DIR}/eups/bin/setups.sh"
export EUPS_PKGROOT="http://lsst-web.ncsa.illinois.edu/~mjuric/pkgs"

# If you don't have git > v1.8.4, do:
eups distrib install git
