.. _quick-start-devel:

################################
Quick start guide for developers
################################

Using Qserv with your own custom code or arbitrary versions can be done by
connecting your local git clone with an eups software stack containing Qserv
dependencies.

.. _quick-start-devel-pre-requisites:

**************
Pre-requisites
**************

Follow classical install :ref:`quick-start-pre-requisites`, and then build and install all
source dependencies for the current Qserv release:

.. code-block:: bash

   # use Qserv official distribution server
   eups distrib install --onlydepend --tag qserv-dev qserv
   # only if you want to launch integration tests with your Qserv code
   eups distrib install --tag qserv-dev qserv_testdata
   setup qserv_testdata --tag qserv-dev
 
.. note::

   Above 'eups distrib' command will install dependencies for the current Qserv release. If you want to develop with an other set of dependencies, you may
   have to install them one by one, or specify a given Qserv version or tag (instead of ``-t qserv``). See :ref:`build-qserv-with-specific-dependencies` for additional informations.

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
   git clone git://github.com/LSST/qserv
   # or authenticated access (require a ssh key) by tapping :
   # git clone ssh://git@github.com/LSST/qserv
   # build and install your Qserv version
   cd qserv
   # if following "setup" command fails due to missing packages one has to
   # manually install those packages with regular "eups distrib install ..."
   setup --root . --tag qserv-dev
   # build Qserv. Optional, covered by next command (i.e. install)
   scons build
   # install Qserv in-place (i.e. in ${SRC_DIR}/qserv/)
   scons install

   # Each time you want to test your code, run :
   scons install

.. note::
   In above commands ``scons`` can be replaced with 
   ``eupspkg -e PREFIX=$PWD`` which will compute the number of processor on your system
   and launch a parallel build using all of them (see `scons -j` option).

For complete list of supported scons targets see :ref:`scons-targets`.

Once the qserv eups stack is integrated with your local Qserv repository, you
will need to configure and (if desired) test it (see :ref:`quick-start-configuration`).

*******************
Updating test cases
*******************

If you want to modify tests datasets, please clone Qserv test data repository :

.. code-block:: bash

   cd ~/src/
   # authenticated access (require a ssh key) :
   git clone ssh://git@github.com/LSST/qserv_testdata

In order to test it with your Qserv version :

.. code-block:: bash

   QSERV_TESTDATA_SRC_DIR=${HOME}/src/qserv_testdata/
   cd $QSERV_TESTDATA_SRC_DIR
   # Assuming you have already installed in eups stack
   # qserv_testdata dependencies
   setup -k -r .
   scons                    # Build for in-place use


*********************************
Updating other Qserv dependencies
*********************************

``eupspkg`` provide an abstraction layer which allow you to easily develop
with any eups-distributed package. Please note that commands below are usable with any git repository
whose code is eups-compliant, and which supports in-place install:

.. code-block:: bash

   # clone Qserv repository
   SRC_DIR=${HOME}/src
   cd ${SRC_DIR}
   # authenticated access (require a ssh key) :
   git clone ssh://git@git.lsstcorp.org/LSST/DMS/dependency
   # build and install your version of this Qserv dependency
   cd dependency
   # if following "setup" command fails due to missing packages one has to
   # manually install those packages with regular "eups distrib install ..."
   setup -k -r .
   eupspkg -e build
   # install dependency in-place (if possible)
   eupspkg -e PREFIX=$PWD install

   # Each time you want to test your code, run :
   eupspkg -e PREFIX=$PWD install

