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

   # Clone Qserv repository
   SRC_DIR=${HOME}/src
   mkdir ${SRC_DIR}
   cd ${SRC_DIR}
   # authenticated access (require a ssh key) :
   git clone ssh://git@github.com/LSST/qserv

   # Install Qserv in the stack
   cd qserv/
   setup -r .
   eupspkg -erd install

   # 'latestbuild' is a global eups tag used to tag latest Qserv version available on the cluster
   # Declare latestbuild tag in $EUPS_PATH/ups_db/global.tags, if not exists
   grep -q -F 'latestbuild' $EUPS_PATH/ups_db/global.tags || echo 'latestbuild' >> $EUPS_PATH/ups_db/global.tags

   # Tag 'latestbuild' this Qserv version
   eupspkg -erd decl -t latestbuild

Deploy on the cluster
=====================

Adapt this example:

* Script for installing a cluster node :download:`install-node.sh <../../../admin/tools/cluster/cc-in2p3/install-node.sh>`. In this example, a shared file system is used to retrieve binaries on all nodes.
* Aliases for parallel management via ssh :download:`parallel-ssh-command.aliases <../../../admin/tools/cluster/cc-in2p3/parallel-ssh-command.aliases>`.

  .. code-block:: bash

   source ~/src/qserv/admin/tools/cluster/cc-in2p3/parallel-ssh-command.aliases

   # Command below are launched on parallel on each node

   # Stop Qserv
   qserv-stop

   # Install Qserv
   qserv-install

   # Start Qserv
   qserv-start

   # Check Qserv status
   qserv-status
