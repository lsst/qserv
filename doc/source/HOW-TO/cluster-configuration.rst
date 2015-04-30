******************************
How to configure Qserv cluster
******************************

Pre-requisite
=============

Build/install Qserv on each machine of the cluster, using eups.

Install master and worker nodes
===============================

On each node:

- setup qserv
- make fresh run directory
- edit meta-configuration file
- fill run directory.

Example for master node:

.. code-block:: bash

  . /path/to/stack/loadLSST.bash
  setup qserv $VERSION
  qserv-configure.py -i -R /path/to/run/dir -D /path/to/data/dir
  vi /path/to/run/dir/qserv-meta.conf
  # change values of next: 
  #   - "node_type" is set to "master"
  #   - "master" is set to master dns name

  # next command will not impact /path/to/data/dir if not empty
  qserv-configure.py -r /path/to/run/dir
  /path/to/run/dir/bin/qserv-start.sh

And then repeat the very same operations on the worker nodes,
except that "node_type" field value is set to "worker".

