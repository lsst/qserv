********************************************************
Build and install a custom version of Qserv on a cluster
********************************************************

Build on a dedicated machine
============================

Pre-requisites
++++++++++++++

* Install LSST stack and Qserv dependencies as described here :ref:`quick-start-devel-pre-requisites`.
* Open write access for each developer to Qserv install path:

  .. code-block:: bash

   # Update acl
   setfacl --recursive --modify g::rwx /path/to/lsst/stack/Linux64/qserv

Build and tag
+++++++++++++

Each developer can than build and install its own Qserv version in shared LSST stack, beware race-conditions:

.. code-block::  bash

   # Path to stack must be the same on all nodes
   source /path/to/lsst/stack/loadLSST.bash

   # Enable a recent git version
   setup git

   # Clone Qserv repository
   SRC_DIR=${HOME}/src
   mkdir ${SRC_DIR}
   cd ${SRC_DIR}
   # authenticated access (require a ssh key) :
   git clone ssh://git@github.com/LSST/qserv

   # Update dependencies to latest Qserv release
   eups distrib install qserv -t qserv --onlydepend

   # Install Qserv in the stack
   cd qserv/
   setup -r . -t qserv
   eupspkg -erd install

   # 'latestbuild' is a global eups tag used to tag latest Qserv version available on the cluster
   # Declare latestbuild tag in $EUPS_PATH/ups_db/global.tags, if not exists
   grep -q -F 'latestbuild' $EUPS_PATH/ups_db/global.tags || echo 'latestbuild' >> $EUPS_PATH/ups_db/global.tags

   # Tag 'latestbuild' this Qserv version
   eupspkg -erd decl -t latestbuild

Deploy on the cluster
=====================

You can use and adapt CC-IN2P3 example (see ${SRC_DIR}/qserv/admin/tools/cluster/cc-in2p3/shmux).

