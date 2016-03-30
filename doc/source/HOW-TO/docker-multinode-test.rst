###########################################
Run multinode test inside Docker containers
###########################################

.. note::

   This procedure was tested with Docker 1.7 and 1.8.

**************
Pre-requisites
**************

- A Qserv master and a worker image made from the github branch/tag which will be tested, see :ref:`docker-github`

.. note::

   All pre-requisites below can be handled automatically by using Vagrant and a
   Cloud-Computing infrastructure. An example for NCSA Openstack platform is
   available here: vagrant-openstack-example_.
   You can also fully automate VM images creation by using Packer, see example:
   packer-openstack-example_.

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

******************
Run multinode test
******************

On the workstation *myhost.domain.org*, clone Qserv code and go to directory containing example for deployment scripts.

.. code-block:: bash

   git clone git@github.com:lsst/qserv.git
   cd ${SRC_DIR}/qserv/admin/tools/docker/deployment

create and adapt next example scripts to prepare multinode test execution:

In :file:`env.sh`, prepare your host list and set the name of your images:

.. literalinclude:: ../../../admin/tools/docker/deployment/parallel/env.example.sh
   :language: bash
   :linenos:

In :file:`pull.sh`, download latest container version on each node:

.. literalinclude:: ../../../admin/tools/docker/deployment/parallel/pull.sh
   :language: bash
   :linenos:

In :file:`run-multinode-tests.sh`, run multinode test:

.. literalinclude:: ../../../admin/tools/docker/deployment/parallel/run-multinode-tests.sh
   :language: bash
   :linenos:

Then, launch multinode test:

.. code-block:: bash

   ./run-multinode-tests.sh

