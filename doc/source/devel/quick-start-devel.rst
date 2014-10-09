.. _quick-start-devel:

################################
Quick start guide for developers
################################

Using Qserv with your own custom code or arbitrary versions can be done by
connecting your local git clone with an eups software stack containing Qserv
dependencies.

**************
Pre-requisites
**************

Follow classical install :ref:`quick-start-pre-requisites`, and then build and install all
source dependencies for the current Qserv release:

.. code-block:: bash

   # use Qserv official distribution server
   eups distrib install --onlydepend --repository http://lsst-web.ncsa.illinois.edu/~fjammes/qserv qserv
   # only if you want to launch integration tests with your Qserv code
   eups distrib install qserv_testdata
   setup qserv_testdata

.. note::

   Above 'eups distrib' command will install dependencies for the current Qserv release. If you want to develop with an other set of dependencies, you may
   have to install them one by one, or specify a given Qserv version or tag (using -t). See :ref:`build-qserv-with-specific-dependencies` for additional informations.

.. _quick-start-devel-setup-qserv:

************************************
Setup your own Qserv version in eups
************************************

These commands explain how to connect your local Qserv git repository to an eups software stack containing Qserv dependencies.
Once Qserv dependencies are installed in eups stack, please use next commands in order to install your Qserv development version:

.. code-block:: bash

   # clone Qserv repository
   SRC_DIR=${HOME}/src
   mkdir ${SRC_DIR}
   cd ${SRC_DIR}
   # anonymous access :
   git clone git://git.lsstcorp.org/LSST/DMS/qserv
   # or authenticated access (require a ssh key) :
   git clone ssh://git@git.lsstcorp.org/LSST/DMS/qserv
   # build and install your Qserv version
   cd qserv
   # if following "setup" command fails due to missing packages one has to
   # manually install those packages with regular "eups distrib install ..."
   setup -k -r .
   # build Qserv. Optional, covered by next command (i.e. install)
   scons build
   # install Qserv in-place (i.e. in ${SRC_DIR}/qserv/)
   scons install prefix=${PWD}

   # Each time you want to test your code, run :
   scons install prefix=${PWD}


You can also replace previous ``scons`` commands with next ``eupspkg``
commands, please note that these commands are usable with any git repository
whose code is eups-compliant, and which support in-place build:

.. code-block:: bash

   # build Qserv. Optional, covered by next command (i.e. install)
   eupspkg -e build
   # install Qserv in-place (i.e. in ${SRC_DIR}/qserv/)
   eupspkg -e PREFIX=$PWD install

   # Each time you want to test your code, run :
   eupspkg -e PREFIX=$PWD install

Once the qserv eups stack is integrated with your local Qserv repository, you
will need to configure and (if desired) test it (see :ref:`quick-start-configuration`).

*******************
Updating test cases
*******************

If you want to modify tests datasets, please clone Qserv test data repository :

.. code-block:: bash

   cd ~/src/
   # authenticated access (require a ssh key) :
   git clone ssh://git@git.lsstcorp.org/LSST/DMS/testdata/qserv_testdata.git

In order to test it with your Qserv version :

.. code-block:: bash

   QSERV_TESTDATA_SRC_DIR=${HOME}/src/qserv_testdata/
   cd $QSERV_TESTDATA_SRC_DIR
   setup -k -r .
   scons build                # build
   scons install prefix=dist  # install (qserv_testdata doesn't support
                              # in-place install)
   cd dist
   setup -k -r .

   # Each time you want to test your code, run :
   cd ..
   scons install prefix=dist

