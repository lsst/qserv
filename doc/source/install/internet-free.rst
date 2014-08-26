##########################
Internet-free installation
##########################

Cluster often doesn't provide internet access for security reasons, that's why
it is possible to install Qserv on machines without internet access.
Qserv distribution offers an archive containing distribution server data which can copied on a distributed file system and used by internet-free servers in order to rebuild Qserv from source.

**************
Pre-requisites
**************

Install system dependencies
===========================

You will need one machine with Internet access. It will need write access to a shared file system visiblefrom the offline cluster.

All system dependencies need to be installed on all nodes in the cluster. See :ref:`quick-start-pre-requisites-system-deps`

Download distribution server data
=================================

Run the following scripts as **non-root user** on the machine **with Internet access**.

.. literalinclude:: ../../../admin/tools/prepare-install-from-internet-free-distserver.example.sh
   :language: bash
   :emphasize-lines: 15-
   :linenos:

************
Installation
************

Run the following commands on each node in the cluster, use the **non-root user account** which will run Qserv.

.. code-block:: bash

   # OPTIONAL : if python 2.7 isn't available, install Anaconda : 
   ${INTERNET_FREE_DISTSERVER_DIR}/Anaconda-1.8.0-Linux-x86_64.sh 

You can then **apply customizations below** to standard Qserv install procedure (start here : :ref:`quick-start-install-lsst-stack`) :

- before installing LSST stack (see :ref:`quick-start-install-lsst-stack`), replace `NEWINSTALL_URL` value with :

.. code-block:: bash

   NEWINSTALL_URL=${INTERNET_FREE_DISTSERVER_DIR}

- `newinstall.sh` script will ask you two questions, answer "no" to the first one (i.e. install git with eups), and and "no" to the second (i.e. don't install anaconda with eups).

- before installing Qserv (see :ref:`quick-start-install-qserv`), replace `EUPS_PKGROOT` value with :

.. code-block:: bash

   # `. loadLSST.sh` instruction updates EUPS_PKGROOT value, that's why you need to correct it 
   # before installing Qserv
   export EUPS_PKGROOT=${INTERNET_FREE_DISTSERVER_DIR}

