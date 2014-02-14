mkdir -p ${HOME}/.lsst/
cp eups/env.sh ${HOME}/.lsst/
source ${HOME}/.lsst/env.sh


if [ ! -w ${INSTALL_DIR} ]
then   
    echo "cannot write in ${INSTALL_DIR}: Permission denied" >&2
    exit 1
fi

mkdir -p ${INSTALL_DIR}/.eups/
cp eups/functions.sh ${INSTALL_DIR}/.eups/
source ${INSTALL_DIR}/.eups/functions.sh

if [ -e "${INSTALL_DIR}/eups/bin/setups.sh" ]
then   
    echo
    echo "REMOVING PREVIOUS PACKAGES"
    echo
    source "${INSTALL_DIR}/eups/bin/setups.sh"
    eups_unsetup_all
    eups_remove_all
fi

cd ${INSTALL_DIR}
rm -rf sources &&
mkdir sources &&
cd sources &&
# install eups latest version
# git clone git@github.com:RobertLuptonTheGood/eups.git
git clone ${EUPS_GIT_URL} &&
cd eups/ &&
./configure --prefix="${INSTALL_DIR}/eups" \
--with-eups="${INSTALL_DIR}/stack"&&
make &&
make install ||
{
    echo "Failed to install eups" >&2
    exit 1
}

# install git latest version
source "${INSTALL_DIR}/eups/bin/setups.sh"
export EUPS_PKGROOT="http://lsst-web.ncsa.illinois.edu/~mjuric/pkgs"

# If you don't have git > v1.8.4, do:
eups distrib install git_scm

setup git_scm

echo
echo "INSTALLING PACKAGES : $PWD"
echo
export EUPS_PKGROOT=${QSERV_EUPS_PKGROOT}
eups distrib install virtualenv_python
setup virtualenv_python

time eups distrib install qserv 
