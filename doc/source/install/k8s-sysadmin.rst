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

These tasks require sudo access and should be performed by system administrators.

- Required packages installed: `yum install docker-engine ebtables kubeadm kubectl kubelet kubernetes-cni util-linux`
- A sudo-enabled user account on a handful of Linux machines, for example: account *myuser* on a workstation  named *myhost.domain.org*, and on cluster nodes named *qserv00.domain.org* to *qserv03.domain.org*
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

On the workstation *myhost.domain.org*, get and prepare deployment scripts.

Set up straightforward ssh access, using:
  - optionally, an ssh client configuration file
  - optionally, a parallel  configuration file (see example below)

.. literalinclude:: ../../../admin/tools/docker/deployment/kubernetes/sshloginfile.example.in2p3
   :language: bash
   :linenos:

.. code-block:: bash

   git clone git@github.com:lsst/qserv.git
   cd ${SRC_DIR}/qserv/admin/tools/docker/deployment/kubernetes/sysadmin
   # Destroy kubernetes cluster if it exists
   ./kube-destroy.sh
   # Create kubernetes cluster
   ./kube-create.sh


On each machine running kubectl client, create a directory readable by all kubectl users,
for example `/qserv/kubernetes`.

Then create files below inside it:
  - an environment file named `env-infrastructure.sh` with all node hostnames (see example below)

.. literalinclude:: ../../../admin/tools/docker/deployment/kubernetes/env-infrastructure.example.in2p3.sh
   :language: bash
   :linenos:

- an environment file named `env.sh` with all kubernetes configuration (qserv images, attached volumes, ...)

.. literalinclude:: ../../../admin/tools/docker/deployment/kubernetes/env.example.sh
   :language: bash
   :linenos:

- an file named `env.sh` with all kubernetes secrets

.. code-block:: bash

   # Helper to get secret fiel from k8s master
   # Check it is readable by all kubectl users
   ./export-kubeconfig.sh -K /qserv/kubernetes/kubeconfig

.. note::

   An alternative solution is to run Openstack provisioning script and check `~/.lsst/qserv-cluster` directory to see examples.
