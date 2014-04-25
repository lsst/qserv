# Quick install guide :
# ---------------------

# WARN : The install procedure described in README.txt doesn't install Qserv 
# from current source code, but a previously packaged Qserv uploaded on Qserv distribution server 
# (Here master branch version issued from commit 3c8a2a4e5f3674e504ab3f86e99317f0173d80b1)
# IMPORTANT FOR DEVELOPPERS : see README-devel.txt to install Qserv from current source code.

# Install system dependencies :
#   for Scientific Linux 6
sudo admin/bootstrap/qserv-install-deps-sl6.sh
#   for Debian 
sudo admin/bootstrap/qserv-install-debian-wheezy.sh
#   for Ubuntu
sudo admin/bootstrap/qserv-install-ubuntu-13.10.sh

# Then run  :
wget http://datasky.in2p3.fr/qserv/distserver-master/newinstall-qserv-6.0.0rc1-master.sh
chmod u+x newinstall-qserv-6.0.0rc1-master.sh
./newinstall-qserv-6.0.0rc1-master.sh
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

# Integration tests (may not work since tickets/DM-282 merge) :
# -------------------------------------------------------------

# launch integration for dataset 01
qserv-start.sh
qserv-testdata.py
