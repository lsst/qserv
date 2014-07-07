Quick start guide
=================

.. note::
  **For developers**, the install procedure described in README.txt doesn't install Qserv from your current git repository version,
  but from a previously packaged Qserv uploaded on Qserv distribution server.
  See :doc:`README-devel` in order to install Qserv from your current git repository version.

.. _qserv_install_pre_requisites:

Pre-requisites
--------------

Install system dependencies
***************************

.. code-block:: bash

  # Install system dependencies :
  # for Scientific Linux 6
  sudo admin/bootstrap/qserv-install-deps-sl6.sh
  # for Debian
  sudo admin/bootstrap/qserv-install-debian-wheezy.sh
  # for Ubuntu
  sudo admin/bootstrap/qserv-install-ubuntu-13.10.sh

Install eups
************

See :ref:`qserv_install_pre_requisites`

.. code-block:: bash

  # $INSTALL_DIR must be empty
  cd $INSTALL_DIR
  curl -O http://sw.lsstcorp.org/eupspkg/newinstall.sh
  # script below will ask some questions, answer 'yes' everywhere
  bash newinstall.sh

Load LSST stack environment
***************************

.. code-block:: bash

  cd $INSTALL_DIR
  source loadLSST.sh

.. _qserv_install:

Qserv Installation
------------------

.. code-block:: bash

  eups distrib install qserv -r http://lsst-web.ncsa.illinois.edu/~fjammes/qserv
  setup qserv
  # only if you want to run integration tests on a mono-node instance :
  eups distrib install qserv_testdata -r http://lsst-web.ncsa.illinois.edu/~fjammes/qserv
  setup qserv_testdata

.. _qserv_config:

Configuration
-------------

Configuration is installed apart from Qserv software.

.. warning::
  Above --all option will remove previous configuration if it exists.

.. code-block:: bash

  # qserv-configure.py --help give additional informations
  # configuration parameters will be deployed in all
  # qserv services configuration files/db
  # for a minimalist single node install just leave default
  qserv-configure.py --all


.. _qserv_integration_tests:

Integration tests
-----------------

For a mono-node instance.

.. code-block:: bash

  qserv-start.sh
  # launch integration tests for all datasets
  qserv-testdata.py
  # launch integration tests only for dataset n°01
  qserv-benchmark.py --case=01 --load
