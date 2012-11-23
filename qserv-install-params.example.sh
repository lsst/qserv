# WARNING : these variables mustn't be changed once the install process is started

QSERV_USER=$USER

# Default log path is $QSERV_BASE/var/log/
# export QSERV_LOG=/var/log/$QSERV_VERSION

# Where to download LSST data
# Example: PT1.1 data should be in ${QSERV_IMPORT_DATA}/pt11/ 
QSERV_IMPORT_DATA=/space/data/lsst

# Default mysql data path is $QSERV_MYSQL_DATA
QSERV_MYSQL_DATA=/space/data/${QSERV_VERSION}/mysql

QSERV_MYSQL_PASS='changeme'

# Qserv rpc service port is 7080 but is hard-coded
QSERV_MYSQL_PORT=3306
QSERV_MYSQL_PROXY_PORT=4040
CMSD_MANAGER_PORT=2131
XROOTD_PORT=1094

# In mono-node configuration :
# - cmsd won't be started
# - xrootd configuration file (lsp.cf) will disable xrootd manager 
MONO_NODE=1

# Geometry file will be downloaded by default
# but a source directory may be specified 
# it could be retrieved for exemple with : git clone git://dev.lsstcorp.org/LSST/DMS/geom
# QSERV_GEOM=/home/user/geom

DATE=`date +%Y%m%d`
