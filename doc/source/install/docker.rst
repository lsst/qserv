#################
Docker containers 
#################

Qserv is also available as a Docker image (see https://www.docker.com/).

.. note::

   This procedure was tested with Docker 1.6 and 1.8.

***********************
Build main Qserv images
***********************

Create two images:

- qserv:latest: Qserv latest release
- qserv:dev: Allow to build/configure and run Qserv from sources

.. code-block:: bash

   . /path/to/lsst/stack/loadLSST.bash
   cd ${SRC_DIR}/qserv/admin/tools/docker
   ./build-images.sh
   
*********
Use cases
*********

Test latest release
===================

Run mono-node integration test against latest Qserv release:

.. code-block:: bash

   docker run -it --rm \
   -h $(hostname)-docker -u qserv \
   fjammes/qserv:latest \
   /qserv/scripts/mono-node-test.sh

Develop and test 
================

Set uid in qserv:dev w.r.t your host machine user account. This creates a qserv:dev-uid image which can mount host source code in a container with correct permissions. 
Host might be a development machine or a continuous integration server.

.. code-block:: bash

   ./change-uid.sh
 
Build, configure and run Qserv from source in qserv:dev-uid container using
source and run directory located on a development machine. 

.. code-block:: bash

   docker run -it --rm \
   -h $(hostname)-docker \
   -v /home/qserv/src/qserv:/home/dev/src/qserv \
   -v /home/qserv/qserv-run/:/home/dev/qserv-run \
   -u dev \
   fjammes/qserv:dev-uid \
   /bin/sh -c "/home/dev/scripts/build.sh && /qserv/scripts/mono-node-test.sh"

Create images for cluster deploiment 
====================================

Create two Qserv images

- qserv:laster-master: Instance of Qserv latest release configured as master
- qserv:laster-worker: Instance of Qserv latest release configured as worker

.. code-block:: bash

   cd ${SRC_DIR}/qserv/admin/tools/docker
   ./build-configured-images.sh
 
***************
Useful commands
***************

.. literalinclude:: ../../../admin/tools/docker/useful-cmd.sh 
   :language: bash
   :linenos:

