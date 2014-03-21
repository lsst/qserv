Quick install guide :
---------------------

# edit INSTALL_DIR variable in eupspkg/install.sh
# run :
$ eupspkg/install.sh

# enable qserv in eups :
# Warning : qserv core install may fails, deps install should always work 
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
