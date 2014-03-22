Quick install guide :
---------------------

# Install system dependencies :
#   for Scientific Linux 6
$ admin/bootstrap/qserv-install-deps-sl6.sh
#   for Debian 
$ admin/bootstrap/qserv-install-debian-wheezy.sh
#   for Ubuntu
$ admin/bootstrap/qserv-install-ubuntu-13.10.sh

$ export ${QSERV_SRC_DIR}=${PWD}
# WARN : edit INSTALL_DIR variable in eupspkg/env.sh
# WARN : qserv core install may fails, deps install should always work 
$ eupspkg/install.sh

# setup qserv and its deps in eups :
$ export ${QSERV_SRC_DIR}=${PWD}
$ source eupspkg/setup.sh
$ setup qserv
$ eups list

Configuration :
---------------

$ cd $QSERV_DIR/admin
# edit qserv.conf, which is the "mother" configuration file from which
# configuration parameters will be deployed in all qserv services
# configuration files/db
# for a minimalist single node install just set 
# master=127.0.0.1
$ scons

Integration tests :
-------------------

# Warning : unstable
# launch integration for dataset 01
$ source $QSERV_DIR/qserv-env.sh
$ qserv-start
$ qserv-benchmark -l --case=01
