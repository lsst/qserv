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

# $INSTALL_DIR must be empty
cd $INSTALL_DIR
wget http://sw.lsstcorp.org/eupspkg/newinstall.sh
# script below will ask some questions, answer 'yes' everywhere
bash newinstall.sh
source loadLSST.sh
eups distrib install qserv -r http://lsst-web.ncsa.illinois.edu/~fjammes/qserv
setup qserv

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
