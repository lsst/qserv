#################
Docker containers
#################

Qserv is also available as a Docker image (see https://www.docker.com/).

.. note::

   This procedure was tested with Docker 1.8.

.. _docker-github:

**************************************
Build Qserv image from a Github branch
**************************************

Create Qserv latest release image, with 3 names:

- qserv:latest
- qserv:dev
- qserv:YYY_MM

.. code-block:: bash

   . /path/to/lsst/stack/loadLSST.bash
   cd ${SRC_DIR}/qserv/admin/tools/docker
   1_build-image.sh -C

Create Qserv cutting-edge dependencies image, named qserv:dev:

.. code-block:: bash

   # cutting-edge dependencies needs to be tagged eups-dev
   # on distribution server.
   cd ${SRC_DIR}/qserv/admin/tools/docker
   1_build-image.sh -CD

Create Qserv image for a given git tag/branch:

In order to push produced Docker images to Docker Hub, prefix
``<docker-image-name>`` with ``qserv/qserv:`` in instructions below.

.. code-block:: bash

   # Code need to be pushed on github
   cd ${SRC_DIR}/qserv/admin/tools/docker
   3_build-git-image.sh -R <git-tag/branch> -T <docker-image-name>
   # Current Qserv version will have eups tag named qserv-dev

Create Qserv master and worker images from a given Qserv version:

.. code-block:: bash

   # Code need to be pushed on github
   # eups tag named qserv-dev will be used to setup Qserv version
   cd ${SRC_DIR}/qserv/admin/tools/docker
   4_build-configured-image.sh -i <docker-image-name>

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

Create Qserv image for a developer workstation:

.. code-block:: bash

   cd ${SRC_DIR}/qserv/admin/tools/docker
   build-work-image.sh
   # change uid in image so that it match host user id
   # in order to mount rw user source code in container
   change-uid.sh


Set uid in qserv:dev w.r.t your host machine user account. This creates a qserv:dev-uid image which can mount host source code in a container with correct permissions.
Host might be a development machine or a continuous integration server.

.. code-block:: bash

   ./change-uid.sh

Build, configure and run Qserv from source in qserv:work-uid container using
source and run directory located on a development machine.

.. code-block:: bash

   docker run -it --rm \
   -h $(hostname)-docker \
   -v /home/qserv/src/qserv:/home/dev/src/qserv \
   -v /home/qserv/qserv-run/:/home/dev/qserv-run \
   -u dev \
   fjammes/qserv:work-uid \
   /bin/sh -c "/home/dev/scripts/build.sh && /qserv/scripts/mono-node-test.sh"

***************
Useful commands
***************

.. literalinclude:: ../../../admin/tools/docker/useful-cmd.sh
   :language: bash
   :linenos:

