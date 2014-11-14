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
* For Debian Wheezy: :download:`qserv-install-deps-debian-wheezy.sh <../../../admin/bootstrap/qserv-install-deps-debian-wheezy.sh>`.
* For Ubuntu 12.04: :download:`qserv-install-deps-ubuntu-12.04.sh <../../../admin/bootstrap/qserv-install-deps-ubuntu-12.04.sh>`.
* For Ubuntu 13.10: :download:`qserv-install-deps-ubuntu-13.10.sh <../../../admin/bootstrap/qserv-install-deps-ubuntu-13.10.sh>`.
* For Ubuntu 14.04: :download:`qserv-install-deps-ubuntu-14.04.sh <../../../admin/bootstrap/qserv-install-deps-ubuntu-14.04.sh>`.

************
Installation
************

.. _quick-start-install-lsst-stack:

Install LSST stack
==================

First, log in with a **non-root user account**.

.. code-block:: bash

   NEWINSTALL_URL=https://sw.lsstcorp.org/pkgs/newinstall.sh
   export EUPS_PKGROOT=${NEWINSTALL_URL}
   INSTALL_DIR=root/directory/where/qserv/stack/will/be/installed
   # e.g. ~qserv, please note that $INSTALL_DIR must be empty
   cd $INSTALL_DIR
   curl -O ${NEWINSTALL_URL}
   # script below will ask some questions. Unless you know what you're doing,
   # and you need a fine tuned setup, please answer 'yes' everywhere.
   bash newinstall.sh
   . loadLSST.bash

.. _quick-start-install-qserv:

Install Qserv distribution 
==========================

Then below, please set ``RELEASE`` to "|release|" to install explicitly this release, or replace it with ``-t qserv`` to install current Qserv release.

.. code-block:: bash

   # First, define RELEASE, here we install current Qserv release
   RELEASE="-t qserv"
   # loadLSST.bash must have been sourced before running this (see above)
   eups distrib install qserv_distrib $RELEASE
   setup qserv_distrib $RELEASE

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

   $QSERV_RUN_DIR/bin/qserv-start.sh
   # launch integration tests for all datasets
   qserv-test-integration.py
   # launch only a subset of integration tests, here dataset n°01.
   # fine-tuning is available (see --help)
   qserv-check-integration.py --case=01 --load
