export INSTALL_DIR=/opt/qserv-dev
export EUPS_PKGROOT="http://datasky.in2p3.fr/qserv/distserver/"
export EUPS_GIT_CLONE_CMD="git clone https://github.com/RobertLuptonTheGood/eups.git"

if [ ! -w ${INSTALL_DIR} ]
then   
    echo "cannot write in ${INSTALL_DIR}: Permission denied" >&2
    exit 1
fi

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
${EUPS_GIT_CLONE_CMD} &&
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

# If you don't have git > v1.8.4, do:
eups distrib install git --repository="http://lsst-web.ncsa.illinois.edu/~mjuric/pkgs"

setup git

echo
echo "INSTALLING PACKAGES : $PWD"
echo
eups declare python system -r none -m none

time eups distrib install qserv 
