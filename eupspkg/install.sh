BASEDIR=$(dirname $0)
source ${BASEDIR}/setup.sh
if [ -n "${SETUP}" ]
then
    exit 1
fi


if [ ! -w ${INSTALL_DIR} ]
then   
    echo "cannot write in ${INSTALL_DIR}: Permission denied" >&2
    exit 1
fi

if [ -n "${INSTALL_DIR}" ]; then
    echo "Erasing existing Qserv install"
    chmod -R u+rwx ${INSTALL_DIR}/*
    find ${INSTALL_DIR}/* -not -name "newinstall-qserv-*.sh" -delete
fi

eups_install

# install git latest version
source "${INSTALL_DIR}/eups/bin/setups.sh"

# If you don't have git > v1.8.4, do:
eups distrib install git --repository="http://lsst-web.ncsa.illinois.edu/~mjuric/pkgs"
setup git

echo "Installing Qserv in ${INSTALL_DIR}"
# Try to use system python, if a compatible version is available
CHECK_SYSTEM_PYTHON='import sys; exit(1) if sys.version_info < (2, 4) or sys.version_info > (2, 8) else exit(0)'
python -c "$CHECK_SYSTEM_PYTHON"
retcode=$?
if [ $retcode == 0 ]
then 
    echo "Detected Qserv-compatible sytem python version; will use it."
    EUPS_PYTHON=$EUPS_PATH/$(eups flavor)/python/system
    mkdir -p $EUPS_PYTHON/ups
    eups declare python system -r $EUPS_PYTHON -m none
    cat > $EUPS_PATH/site/manifest.remap <<-EOF
python  system
EOF
else
    echo "Qserv depends on system python 2.6 or 2.7"
    exit 2  
fi    

# Try to use system numpy, if a compatible version is available
CHECK_NUMPY='import numpy'
python -c "$CHECK_NUMPY" && {
    if [ $(eups list python --version) == 'system' ]
    then    
        SYSPATH=`python -c "import inspect,sys, numpy; modulepath=inspect.getfile(numpy); paths=[path for path in sys.path if modulepath.startswith(path)]; print sorted(paths)[-1]"`
        [ -d ${SYSPATH} ] || {
            echo "Unable to detect system numpy PYTHONPATH. Aborting installation."
        }
        echo "Detected Qserv-compatible sytem numpy version in ${SYSPATH}; will use it."
        EUPS_NUMPY=$EUPS_PATH/$(eups flavor)/numpy/system
        mkdir -p $EUPS_NUMPY/ups
        EUPS_NUMPY_TABLE=${EUPS_NUMPY}/ups/numpy.table
        cat > ${EUPS_NUMPY_TABLE} <<-EOF
setupRequired(python)
envPrepend(PYTHONPATH, $SYSPATH)
EOF
        eups declare numpy system -r ${EUPS_NUMPY} -m ${EUPS_NUMPY_TABLE}
        cat > $EUPS_PATH/site/manifest.remap <<-EOF
numpy  system
EOF
    else        
        echo "Unable to detect system numpy library. Aborting installation."
        exit 1
    fi
}


time eups distrib install qserv || {
    echo "Failed to install Qserv"
    exit 2
}

setup qserv

SETUP_SCRIPT=${INSTALL_DIR}/setup-qserv.sh
cat > ${SETUP_SCRIPT} <<-EOF
source ${INSTALL_DIR}/eups/bin/setups.sh
setup qserv
source ${QSERV_DIR}/qserv-env.sh
EOF

echo "Installation complete"
echo "Now type "
echo 
echo "  source ${INSTALL_DIR}/setup-qserv.sh"
echo 
echo "to enable Qserv and its dependencies"
echo "and"
echo 
echo "  cd $QSERV_DIR/admin"
echo "  scons"
echo 
echo "to configure a Qserv mono-node instance"
echo "and"
echo 
echo "  qserv-start"
echo "  qserv-testdata.py"
echo 
echo "to launch integration tests"
