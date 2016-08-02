.. _quick-start:

#################
Quick start guide
#################

.. note::

   This procedure is for RELEASED and PUBLISHED Qserv software.
   Developers interested in modifying code or running unreleased versions should see :ref:`quick-start-devel`.

.. _quick-start-pre-requisites:

**************
Pre-requisites
**************

.. _quick-start-pre-requisites-system-deps:

Install system dependencies
===========================

Please run next script under **root account**:

* For Fedora 19: :download:`qserv-install-deps-fedora19.sh <../../../admin/bootstrap/qserv-install-deps-fedora19.sh>`.
* For Scientific Linux 6: :download:`qserv-install-deps-sl6.sh <../../../admin/bootstrap/qserv-install-deps-sl6.sh>`.
* For RedHat7-based distributions: :download:`qserv-install-deps-rhel7.sh <../../../admin/bootstrap/qserv-install-deps-rhel7.sh>`.
* For Debian8-based distribution: :download:`qserv-install-deps-debian8.sh <../../../admin/bootstrap/qserv-install-deps-debian8.sh>`.
* For Ubuntu 12.04: :download:`qserv-install-deps-ubuntu-12.04.sh <../../../admin/bootstrap/qserv-install-deps-ubuntu-12.04.sh>`.
* For Ubuntu 14.04: :download:`qserv-install-deps-ubuntu-14.04.sh <../../../admin/bootstrap/qserv-install-deps-ubuntu-14.04.sh>`.

************
Installation
************

.. _quick-start-install-lsst-stack:

Install LSST stack
==================

First, log in with a **non-root user account**.

.. code-block:: bash

   NEWINSTALL_URL=https://raw.githubusercontent.com/lsst/lsst/master/scripts/newinstall.sh
   # create a new directory stack and initialize INSTALL_DIR as an absolute path to stack
   mkdir stack
   INSTALL_DIR=/path/to/lsst/stack/
   # e.g. ~qserv, please note that $INSTALL_DIR must be empty
   cd $INSTALL_DIR
   curl -OL ${NEWINSTALL_URL}
   # script below will ask some questions. Unless you know what you're doing,
   # and you need a fine tuned setup, please answer 'yes' everywhere.
   bash newinstall.sh
   . loadLSST.bash

.. _quick-start-install-qserv:

Install Qserv distribution
==========================

Then below, please set ``RELEASE`` to "|release|" to install explicitly this release, or replace it with ``qserv_latest`` to install current Qserv release.

.. code-block:: bash

   # First, define RELEASE, here we install current Qserv release
   RELEASE="qserv_latest"
   # loadLSST.bash must have been sourced before running this (see above)
   eups distrib install --tag $RELEASE qserv_distrib
   setup qserv_distrib --tag $RELEASE

.. _quick-start-configuration:

*************
Configuration
*************

Overview
========

Configuration data is installed apart from Qserv software binaries, in a directory thereafter named *QSERV_RUN_DIR*.

The :program:`qserv-configure.py` script builds a Qserv configuration be deploying configuration parameters in all qserv services configuration files/db. It is called like this:

.. code-block:: bash

   qserv-configure.py [-h] [-a] [-p] [-d] [-e] [-m] [-x] [-q] [-s] [-c]
                      [-v {DEBUG,INFO,WARNING,FATAL,ERROR}] [-f]
                      [-R QSERV_RUN_DIR]

The :program:`qserv-configure.py` script has several important options:

.. program:: qserv-configure.py

.. option:: -h, --help

   Display all availables options.

.. option:: -a, --all

   Clean ``QSERV_RUN_DIR`` and fill it with mono-node configuration data.

.. option:: -R <directory>, --qserv-run-dir <directory>

   Set configuration data location (i.e. ``QSERV_RUN_DIR``), Default value for
   ``QSERV_RUN_DIR`` is ``$HOME/qserv-run/$QSERV_VERSION``, ``QSERV_VERSION`` being provided by qserv-version.sh command.

Mono-node instance
==================

.. warning::
   The -all option below will remove any previous configuration for the same
   Qserv version.

.. code-block:: bash

   # for a minimalist single node install :
   qserv-configure.py --all

*******
Testing
*******

For a mono-node instance.

.. code-block:: bash

   # default value for QSERV_RUN_DIR
   QSERV_RUN_DIR="$HOME/qserv-run/$(qserv-version.sh)"
   # start qserv
   $QSERV_RUN_DIR/bin/qserv-start.sh
   # launch integration tests for all datasets
   qserv-test-integration.py
   # launch only a subset of integration tests, here dataset n°01.
   # fine-tuning is available (see --help)
   qserv-check-integration.py --case=01 --load
   # to avoid conflict you can stop qserv
   $QSERV_RUN_DIR/bin/qserv-stop.sh

********************
For more information
********************

https://confluence.lsstcorp.org/display/LSWUG/Building+the+LSST+Stack+from+Source

