######################
Docker virtual machine
######################

Qserv is also available as a Docker image (see https://www.docker.com/).

***************
Basic use cases
***************

Create a docker image containing a Qserv mono-node instance and run integration tests:

.. code-block:: bash

   . /path/to/lsst/stack/loadLSST.bash
   cd ${SRC_DIR}/qserv/admin/tools/docker
   ./build-docker-img.sh centos_7

Dockerfile example for centos7:
.. literalinclude:: ../../../admin/tools/docker/centos_7/Dockerfile
   :language: bash
   :linenos:

Useful Docker commands:
.. literalinclude:: ../../../admin/tools/docker/useful-cmd.sh
   :language: bash
   :linenos:

