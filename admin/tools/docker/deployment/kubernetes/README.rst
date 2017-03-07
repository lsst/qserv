#######################
Run Qserv on Kubernetes
#######################

.. note::

   This procedure was tested with Docker 1.12 and Kubernetes 1.5 (Client v1.5.1, Server v1.5.3)

**************
Pre-requisites
**************

.. note::

   All pre-requisites below can be handled automatically by using a
   Cloud-Computing infrastructure. We provide Openstack support in pre-alpha
   version, please contact us for additional information.

- Required packages installed: `yum install docker-engine ebtables kubeadm kubectl kubelet kubernetes-cni util-linux`
- A user account on a handful of Linux machines, for example: account *myuser* on a workstation  named *myhost.domain.org*, and on cluster nodes named *qserv00.domain.org* to *qserv03.domain.org*
- *ssh* access from *myhost.domain.org* to all *qservXX.domain.org* for *myuser* account, with no password prompt, for example using ssh keys authentication mechanism.
- A parallel *ssh* client installed on *myhost.domain.org*, for example
  *gnu parallel* (http://web.taranis.org/shmux/)
- Internet or Docker registry access available for all *qservXX.domain.org*
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
   cd ${SRC_DIR}/qserv/admin/tools/docker/deployment/kubernetes
   # admin/ contains script which require ssh access to node
   # orchestration/ is copied to k8s orchestration node and contains scripts
   # which manage pods

Create and adapt next example scripts to prepare multinode test execution:
In :file:`env.sh`, set your volume and the tag of your qserv images:

.. literalinclude:: ../../../admin/tools/docker/deployment/kubernetes/env.example.sh
   :language: bash
   :linenos:

You also need an environment file with all node hostnames and a ssh
client configuration file. Run Openstack provisioning script and check
`~/.lsst/qserv-cluster` directory to see examples.

Then, install Kubernetes, Qserv and launch multinodes integration tests.

.. code-block:: bash

   # Create kubernetes cluster
   ./kube-create.sh
   # Start Qserv (pods and unix services)
   ./start.sh
   # Check Qserv status
   ./status.sh
   # Run multinode tests
   ./run-multinode-tests.sh
   # Stop Qserv
   ./stop.sh

***********
Useful tips
***********

Do not start Qserv services in pod, for debugging purpose:

.. code-block:: bash

   # on host machine
   mkdir -p /qserv/custom/bin
   touch /qserv/custom/bin/qserv-start.sh
   chmod u+x /qserv/custom/bin/qserv-start.sh


