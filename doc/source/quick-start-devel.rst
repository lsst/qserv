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
 
   Above command will install dependencies for the current Qserv release. If you want to develop with an other set of dependencies, you may
   have to install them one by one, or specify a given Qserv version.

***********************************
Setup current Qserv version in eups
***********************************

These commands explain how to connect your local Qserv git repository to an eups software stack containing Qserv dependencies.
Once Qserv dependencies are installed in eups stack, please use next commands in order to install your Qserv development version:

.. code-block:: bash

   # clone Qserv repository
   SRC_DIR=${HOME}/src
   mkdir ${SRC_DIR}
   cd ${SRC_DIR}
   # anonymous access : 
   git clone git://dev.lsstcorp.org/LSST/DMS/qserv 
   # or authenticated access (require a ssh key) :
   git clone ssh://git@dev.lsstcorp.org/LSST/DMS/qserv
   # build and install your Qserv version
   cd qserv
   # if following "setup" command fails due to missing packages one has to
   # manually install those packages with regular "eups distrib install ..."
   setup -r .
   eupspkg -er build               # build
   eupspkg -er install             # install to EUPS stack directory
   eupspkg -er decl                # declare it to EUPS
   # enable your Qserv version, and dependencies, in eups
   # $VERSION is available by using :
   eups list qserv
   setup qserv $VERSION

Once the qserv eups stack is integrated with your local Qserv repository, you
will need to configure and (if desired) test it (see :ref:`quick-start-configuration`).

 .. warning::
 
   Be advised that eupspkg may generate different version numbers depending on
   whether the code has changed after checkout. For example it may generate
   version which looks like "master-g86a30ec72a" for freshly checked-out code but
   if you change anything in your repository it will generate new version
   "master-g86a30ec72a-dirty". You may end up with two versions of qserv
   installed, be very careful and remember to run "setup qserv" with the correct
   version number.


*******************
Updating test cases
*******************

If you want to modify tests datasets, please clone Qserv test data repository :

.. code-block:: bash

   cd ~/src/
   # authenticated access (require a ssh key) :
   git clone ssh://git@dev.lsstcorp.org/LSST/DMS/testdata/qserv_testdata.git

In order to test it with your Qserv version :

.. code-block:: bash

   QSERV_TESTDATA_SRC_DIR=${HOME}/src/qserv_testdata/
   cd $QSERV_TESTDATA_SRC_DIR
   setup -r .
   eupspkg -er build               # build
   eupspkg -er install             # install to EUPS stack directory
   eupspkg -er decl                # declare it to EUPS
   # Enable your Qserv version, and dependencies, in eups
   # $VERSION is available by using :
   eups list
   setup qserv_testdata $VERSION
