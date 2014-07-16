Quick start guide :
===================

**NOTE FOR DEVELOPERS** : The install procedure described in README.txt doesn't install Qserv from your current git repository version, but from a previously packaged Qserv uploaded on Qserv distribution server. See **README-devel.txt** in order to install Qserv from your current git repository version.

Installing dependencies
-----------------------

  | # Install system dependencies :
  | # for Scientific Linux 6
  | sudo admin/bootstrap/qserv-install-deps-sl6.sh
  | # for Debian
  | sudo admin/bootstrap/qserv-install-debian-wheezy.sh
  | # for Ubuntu
  | sudo admin/bootstrap/qserv-install-ubuntu-13.10.sh


Installation :
--------------

  | # $INSTALL_DIR must be empty
  | cd $INSTALL_DIR
  | curl -O http://sw.lsstcorp.org/eupspkg/newinstall.sh
  | # script below will ask some questions, answer 'yes' everywhere
  | bash newinstall.sh
  | source loadLSST.sh
  | eups distrib install qserv -r http://lsst-web.ncsa.illinois.edu/~fjammes/qserv
  | setup qserv
  | # only if you want to run integration tests on a mono-node instance :
  | eups distrib install qserv_testdata -r http://lsst-web.ncsa.illinois.edu/~fjammes/qserv
  | setup qserv_testdata

Configuration :
---------------

Configuration is installed apart from Qserv software.

  | # qserv-configure.py --help give additional informations
  | # configuration parameters will be deployed in all
  | # qserv services configuration files/db
  | # for a minimalist single node install just leave default
  | qserv-configure.py --all

Integration tests :
-------------------

For a mono-node instance.

.. note::

  Default value for $QSERV_RUN_DIR is $HOME/qserv-run/$QSERV_VERSION,
  with QSERV_VERSION provided by qserv-version.sh command.

  | $QSERV_RUN_DIR/bin/qserv-start.sh
  | # launch integration tests for all datasets
  | qserv-test-integration.py
  | # launch integration tests only for dataset n°01
  | qserv-benchmark.py --case=01 --load
