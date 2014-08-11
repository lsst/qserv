Quick start guide for developpers
=================================

Pre-requisites
--------------

Follow classical install pre-requisites (see
:ref:`qserv_install_pre_requisites`), and then install all Qserv eups
dependencies by installing last Qserv release (see :ref:`qserv_install`), or manually :

.. code-block:: bash

  eups distrib install boost
  eups distrib install swig
  eups distrib install geom
  eups distrib install mysqlpython
  eups distrib install numpy
  # use Qserv developement distribution server
  export EUPS_PKGROOT=http://lsst-web.ncsa.illinois.edu/~fjammes/qserv
  eups distrib install antlr
  eups distrib install db
  eups distrib install kazoo
  eups distrib install luaxmlrpc
  eups distrib install mysqlproxy
  eups distrib install protobuf
  eups distrib install twisted
  eups distrib install xrootd
  eups distrib install zookeeper
  # only if you want to launch integration tests with your Qserv code
  eups distrib install qserv_testdata
  setup qserv_testdata
  # revert to LSST standard distribution server
  export EUPS_PKGROOT=http://sw.lsstcorp.org/eupspkg

.. note::

  This is last Qserv release eups-dependencies list. You may
  have to update this list if your own Qserv version doesn't rely on exactly the
  same dependencies.

Setup current Qserv version in eups
-----------------------------------

In order to install your Qserv development version please use next commands,

.. code-block:: bash

  # path to your Qserv git repository
  QSERV_SRC_DIR=${HOME}/src/qserv/
  # Build and install your Qserv version
  cd $QSERV_SRC_DIR
  # if following "setup" command fails due to missing packages one has to
  # manually install those packages with regular "eups distrib install ..."
  setup -r .
  eupspkg -er build               # build
  eupspkg -er install             # install to EUPS stack directory
  eupspkg -er decl                # declare it to EUPS
  # Enable your Qserv version, and dependencies, in eups
  # $VERSION is available by using :
  eups list qserv
  setup qserv $VERSION

Then re-run configuration and test process (see :ref:`qserv_config`).

.. warning::

  Be advised that eupspkg may generate different version numbers depending on
  whether the code has changed after checkout. For example it may generate
  version which looks like "master-g86a30ec72a" for freshly checked-out code but
  if you change anything in your repository it will generate new version
  "master-g86a30ec72a-dirty". You may end up with two versions of qserv
  installed, be very careful and remember to run "setup qserv" with the correct
  version number.

Updating test cases
-------------------

If you want to modify tests datasets, please clone Qserv test data repository :

.. code-block:: bash

  cd ~/src/
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
