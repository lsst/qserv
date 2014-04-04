# Enable current Qserv version in eups :
# --------------------------------------

# Once Qserv is installed (cf. README.txt), in order to install your Qserv development 
# version please use next commands,
PREFIX=/path/where/you/install/your/Qserv/dev/version/
VERSION=dev-version-name
QSERV_SRC_DIR=${HOME}/src/qserv/

# declare your Qserv dev version in eups
# Copy and modify table file if you want to modify Qserv dependencies
eups declare qserv $VERSION -r $PREFIX -m $QSERV_DIR/ups/qserv.table 

# unsetup Qserv version issued from distribution server
unsetup qserv

# Enable your Qserv version, and dependencies, in eups
setup qserv $VERSION

# Build and install your Qserv version
cd $QSERV_SRC_DIR
scons install prefix=$PREFIX

# and then configure Qserv following instructions in README.txt
# and run integration tests if you want to check your install

# Test cases :
# ------------ 

# In order to download data for test case, please run next commands 
# in current directory :

git submodule init
git submodule update 

# If you want to modify tests data, please clone Qserv test data repository :
cd ~/src/
git clone ssh://git@dev.lsstcorp.org/LSST/DMS/testdata/qservdata.git
cd ~/src/qserv/
git submodule update --remote
