.. _quick-start:

#################
Quick start guide
#################

.. note::

   *FOR DEVELOPERS* : The procedure below install a Qserv release published on Qserv distribution server. 
   Please see :ref:`quick-start-devel` in order to install Qserv from your current git repository version.

 .. _quick-start_pre-requisites:
**************
Pre-requisites
**************

.. code-block:: bash

   # Install system dependencies :
   # for Scientific Linux 6
   sudo admin/bootstrap/qserv-install-deps-sl6.sh
   # for Debian
   sudo admin/bootstrap/qserv-install-debian-wheezy.sh
   # for Ubuntu
   sudo admin/bootstrap/qserv-install-ubuntu-14.04.sh


************
Installation
************

.. code-block:: bash

   # $INSTALL_DIR must be empty
   cd $INSTALL_DIR
   curl -O http://sw.lsstcorp.org/eupspkg/newinstall.sh
   # script below will ask some questions, answer 'yes' everywhere
   bash newinstall.sh
   source loadLSST.sh
   eups distrib install qserv -r http://lsst-web.ncsa.illinois.edu/~fjammes/qserv
   setup qserv
   # only if you want to run integration tests on a mono-node instance :
   eups distrib install qserv_testdata -r http://lsst-web.ncsa.illinois.edu/~fjammes/qserv
   setup qserv_testdata

*************
Configuration
*************

Configuration data are installed apart from Qserv software.

.. warning::
   The -all option below will remove any previous configuration for the same
   Qserv version.

.. code-block:: bash

   # qserv-configure.py --help give additional informations
   # configuration parameters will be deployed in all
   # qserv services configuration files/db
   # for a minimalist single node install just leave default
   qserv-configure.py --all

*****************
Integration tests
*****************

For a mono-node instance.

.. note::

  Default value for $QSERV_RUN_DIR is $HOME/qserv-run/$QSERV_VERSION,
  with QSERV_VERSION provided by qserv-version.sh command.

.. code-block:: bash

   $QSERV_RUN_DIR/bin/qserv-start.sh
   # launch integration tests for all datasets
   qserv-test-integration.py
   # launch integration tests only for dataset n°01
   qserv-benchmark.py --case=01 --load
