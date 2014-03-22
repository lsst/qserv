BASEDIR=$(dirname $0)
source ${BASEDIR}/setup.sh

if [ ! -w ${INSTALL_DIR} ]
then   
    echo "cannot write in ${INSTALL_DIR}: Permission denied" >&2
    exit 1
fi

if [ -n "${INSTALL_DIR}" ]; then
    chmod -R u+rwx ${INSTALL_DIR}/*
    rm -rf ${INSTALL_DIR}/*
fi

eups_install

# install git latest version
source "${INSTALL_DIR}/eups/bin/setups.sh"

# If you don't have git > v1.8.4, do:
eups distrib install git --repository="http://lsst-web.ncsa.illinois.edu/~mjuric/pkgs"

setup git

echo "Installing Qserv in $PWD"
eups declare python system -r none -m none

time eups distrib install qserv 
