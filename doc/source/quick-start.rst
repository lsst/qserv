.. _quick-start:

#################
Quick start guide
#################

.. note::

   *FOR DEVELOPERS*: The procedure below installs a Qserv release published on Qserv distribution server. 
   Please see :ref:`quick-start-devel` in order to install Qserv from your current git repository version.

.. _quick-start-pre-requisites:

**************
Pre-requisites
**************

Install system dependencies
===========================

* For Fedora 19: :download:`qserv-install-deps-fedora19.sh <../../admin/bootstrap/qserv-install-deps-fedora19.sh>`.
* For Scientific Linux 6: :download:`qserv-install-deps-sl6.sh <../../admin/bootstrap/qserv-install-deps-sl6.sh>`.
* For Debian Wheezy: :download:`qserv-install-deps-debian-wheezy.sh <../../admin/bootstrap/qserv-install-deps-debian-wheezy.sh>`.
* For Ubuntu 13.10: :download:`qserv-install-deps-ubuntu-13.10.sh <../../admin/bootstrap/qserv-install-deps-ubuntu-13.10.sh>`.
* For Ubuntu 14.04: :download:`qserv-install-deps-ubuntu-14.04.sh <../../admin/bootstrap/qserv-install-deps-ubuntu-14.04.sh>`.

************
Installation
************

.. code-block:: bash

   INSTALL_DIR = root/directory/where/qserv/stack/will/be/installed
   # e.g. ~qserv, please note that $INSTALL_DIR must be empty
   cd $INSTALL_DIR
   curl -O http://sw.lsstcorp.org/eupspkg/newinstall.sh
   # script below will ask some questionsr. Unless you know what you're doing,
   # and you need a fine tuned setup, please answer 'yes' everywhere.
   bash newinstall.sh
   source loadLSST.sh
   eups distrib install qserv -r http://lsst-web.ncsa.illinois.edu/~fjammes/qserv
   setup qserv
   # only if you want to run integration tests on a mono-node instance :
   eups distrib install qserv_testdata -r http://lsst-web.ncsa.illinois.edu/~fjammes/qserv
   setup qserv_testdata

.. _quick-start-configuration:

*************
Configuration
*************

.. note::

   Qserv commands listed below provides advanced options, use --help option to
   discover how to use it. 

Configuration data is installed apart from Qserv software.

.. warning::
   The -all option below will remove any previous configuration for the same
   Qserv version.

.. code-block:: bash

   # qserv-configure.py --help provides additional informations
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
   # launch only a subset of integration tests, here dataset n°01.
   # fine-tuning is available (see --help)
   qserv-check-integration.py --case=01 --load
