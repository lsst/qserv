###########################################
Run multinode test inside Docker containers
###########################################

.. note::

   This procedure was tested with Docker 1.12

**************
Pre-requisites
**************

.. note::

   All pre-requisites below can be handled automatically by using a
   Cloud-Computing infrastructure. We provide Openstack support in pre-alpha
   version, please contact us for additional information.

.. _vagrant-openstack-example: https://github.com/fjammes/vagrant-openstack-example
.. _packer-openstack-example: https://github.com/fjammes/packer-openstack-example

- A user account on a handful of Linux machines, for example: account *myuser* on a workstation  named *myhost.domain.org*, and on cluster nodes named *qserv00.domain.org* to *qserv03.domain.org*
- *ssh* access from *myhost.domain.org* to all *qservXX.domain.org* for *myuser* account, with no password prompt, for example using ssh keys authentication mechanism.
- A parallel *ssh* client installed on *myhost.domain.org*, for example
  *shmux* (http://web.taranis.org/shmux/)
- Internet access available for all *qservXX.domain.org*
- Docker running on all *qservXX.domain.org*
- *myuser* belonging to  *docker* group on all *qservXX.domain.org*

  .. code-block:: bash

     sudo usermod -a -G docker myuser

.. note::

   It is possible to run a development version of Qserv by generating Qserv master and a worker images from a given github branch/tag, see :ref:`docker-github`

******************
Run multinode test
******************

On the workstation *myhost.domain.org*, clone Qserv code and go to directory containing example for deployment scripts.

.. code-block:: bash

   git clone git@github.com:lsst/qserv.git
   cd ${SRC_DIR}/qserv/admin/tools/docker/deployment/parallel

create and adapt next example scripts to prepare multinode test execution:

In :file:`env.sh`, prepare your host list and set the name of your images:

.. literalinclude:: ../../../admin/tools/docker/deployment/parallel/env.example.sh
   :language: bash
   :linenos:

Then, install Qserv and launch multinodes integration tests.

.. code-block:: bash

   # download latest docker image on each node:
   ./pull.sh
   # Start Qserv and run multinode test:
   ./run-multinode-tests.sh
   # Check Qserv status
   ./status.sh
   # Stop Qserv
   ./stop.sh

