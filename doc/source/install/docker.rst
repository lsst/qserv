######################
Docker virtual machine
######################

Qserv is also available as a Docker image (see https://www.docker.com/).

***************
Basic use cases
***************

Once you have run Qserv image in Docker container, see
:ref:`quick-start-configuration` in order to configure Qserv as a mono-node
instance.

.. literalinclude:: ../../../admin/tools/docker/cmd.example.sh
   :language: bash
   :linenos:

File below allows to create a Qserv image for Docker from scratch.

.. literalinclude:: ../../../admin/tools/docker/Dockerfile
   :language: bash
   :linenos:
