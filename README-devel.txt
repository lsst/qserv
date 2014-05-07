# Enable current Qserv version in eups :
# --------------------------------------

# Once Qserv is installed (cf. README.txt), in order to install your Qserv development 
# version please use next commands,

# path to your Qserv git repository
QSERV_SRC_DIR=${HOME}/src/qserv/
#
# # Build and install your Qserv version
cd $QSERV_SRC_DIR
setup -r .
eupspkg -er build               # build
eupspkg -er install             # install to EUPS stack directory
eupspkg -er decl                # declare it to EUPS

# Enable your Qserv version, and dependencies, in eups
setup qserv $VERSION

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
