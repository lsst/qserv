******************************************************************
Deploy and configure a custom version of Qserv on CC-IN2P3 cluster
******************************************************************

Pre-requisites
==============

* shmux (http://web.taranis.org/shmux/)
* a shared filesystem containing a pre-built Qserv stack

Deployment
==========

* Set install and configuration parameters in scripts/params.sh
* Deploy management scripts to all nodes:

  .. code-block:: bash

   rsync-scripts.sh

* Run install and configuration scripts on all nodes:

  .. code-block:: bash

   parallel-script.sh <scriptname>

  Available <scriptname> are:

  * remove-stack.sh:    remove existing local stack
  * rsync-stack.sh:     synchronize local stack with the one on the shared-filesystem
  * configure.sh:       configure each node
  * start.sh:           start Qserv on all nodes
  * status.sh:          stop Qserv on all nodes
  * stop.sh:            get Qserv status on all nodes
