Quick install guide :
---------------------

- edit INSTALL_DIR variable in eupspkg/install.sh
- run :
$ eupspkg/install.sh

After installation :
--------------------

- run :

# enable qserv in eups :
# Warning : qserv core install may fails, deps install should always work 
$ source eupspkg/setup.sh
$ setup qserv
$ eups list

# Warning : unstable
# launch integration for dataset 01
$ source $QSERV_DIR/qserv-env.sh
$ qserv-start
$ qserv-benchmark -l --case=01
