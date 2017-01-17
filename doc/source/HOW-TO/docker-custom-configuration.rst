################################################
Customize container-embedded Qserv configuration
################################################

This tutorial explains how to customize container configuration
without having to rebuild its image. This is convenient for debugging
and optimizing.

.. note::

   This procedure was tested with Docker 1.19

**************
Pre-requisites
**************

* Have a working Qserv cluster, distributed on several physical or virtual hosts.

* Create `/qserv/custom` directory on all hosts, any template for configuration files
  found in this directory will be used by the container.

***********************
Customize configuration
***********************

The container generates Qserv configuration files from templates, located in Qserv
installation directory, at each startup. Each file located in `/qserv/custom`, on
host, will overwrite its corresponding Qserv template at container restart and create
a customized configuration file.

  .. code-block:: bash

   # Connect to host
   ssh <qserv-host>

   # Original configuration files are located inside container
   # Use them as example and customize them
   docker exec -it <container-name> bash
   . /qserv/stack/loadLSST.bash
   setup qserv -t qserv-dev
   ls $QSERV_DIR/share/qserv/configuration/templates/
   echo $QSERV_DIR
   exit

   # Copy a given file from the container to the host
   XROOTD_CFG=<QSERV_DIR>/share/qserv/configuration/templates/etc/lsp.cf
   docker cp "$XROOTD_CFG" /tmp/

   # Copy custom configuration files templates to host directory
   cp <my-template-file> /qserv/custom/etc
   
   # Restart the container using the orchestration tool

