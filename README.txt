# Quick install guide :
# ---------------------

# WARN : The install procedure described in README.txt doesn't install Qserv 
# from current source code, but from Qserv distribution server, see README-devel.txt 
# to install Qserv from current source code.

# Install system dependencies :
#   for Scientific Linux 6
sudo admin/bootstrap/qserv-install-deps-sl6.sh
#   for Debian 
sudo admin/bootstrap/qserv-install-debian-wheezy.sh
#   for Ubuntu
sudo admin/bootstrap/qserv-install-ubuntu-13.10.sh

# Then run  :
INSTALL_DIR=/opt/example-qserv-install/
eupspkg/newinstall-qserv.sh -H ${INSTALL_DIR} 
# and follow instructions displayed at the end of install process.
# i.e.

source ${INSTALL_DIR}/setup-qserv.sh

# Configuration :
# ---------------

cd $QSERV_DIR/admin
# edit qserv.conf, which is the "mother" configuration file from which
# configuration parameters will be deployed in all qserv services
# configuration files/db
# for a minimalist single node install just leave default 
scons

# Integration tests :
# -------------------

# launch integration for dataset 01
qserv-start.sh
qserv-testdata.py
