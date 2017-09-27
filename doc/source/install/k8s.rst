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

On the workstation *myhost.domain.org*, get and prepare deployment scripts.

.. code-block:: bash

   git clone git@github.com:lsst/qserv.git
   cd ${SRC_DIR}/qserv/admin/tools/docker/deployment/kubernetes
   # admin/ contains script which require ssh access to node
   # orchestration/ is copied to k8s orchestration node and contains scripts
   # which manage pods
   # Create configuration file
   cp env.example.sh env.sh

In :file:`env.sh`, set your container configuration (qserv images, attached volumes, ...):

.. literalinclude:: ../../../admin/tools/docker/deployment/kubernetes/env.example.sh
   :language: bash
   :linenos:

You also need to create file below in `~/.lsst/qserv-cluster`:
  - an environment file named `env-infrastructure.sh` with all node hostnames (see example below)
  - optionally, an ssh client configuration file
  - optionally, a parallel  configuration file (see 2nd example below)

.. literalinclude:: ../../../admin/tools/docker/deployment/kubernetes/env-infrastructure.example.in2p3.sh
   :language: bash
   :linenos:


.. literalinclude:: ../../../admin/tools/docker/deployment/kubernetes/sshloginfile.example.in2p3
   :language: bash
   :linenos:

.. note::

   An alternative solution is to run Openstack provisioning script and check `~/.lsst/qserv-cluster` directory to see examples.

Then, install Kubernetes, Qserv and launch multinodes integration tests.

.. code-block:: bash

   # Start Qserv (pods and unix services)
   ./start.sh
   # Check Qserv status
   ./status.sh
   # Run multinode tests
   ./run-multinode-tests.sh
   # Stop Qserv
   ./stop.sh

*********************
Administer Kubernetes
*********************

This tasks require sudo access and should be performed by system administrators.

.. code-block:: bash

   # Destroy kubernetes cluster
   ./kube-destroy.sh
   # Create kubernetes cluster
   ./kube-create.sh

***********
Cheat sheet
***********

Debug
-----

For debugging purpose, it might be useful not to start Qserv services in pods:

.. code-block:: bash

   # on host machine
   mkdir -p /qserv/custom/bin
   touch /qserv/custom/bin/qserv-start.sh
   chmod u+x /qserv/custom/bin/qserv-start.sh
   # Qserv startup script will be here replaced by an empty file

Interacting with running Pods
-----------------------------

.. code-block:: bash

    kubectl logs my-pod                               # dump pod logs (stdout)
$ kubectl logs my-pod -c my-container                 # dump pod container logs (stdout, multi-container case)
$ kubectl logs -f my-pod                              # stream pod logs (stdout)
$ kubectl logs -f my-pod -c my-container              # stream pod container logs (stdout, multi-container case)
$ kubectl run -i --tty busybox --image=busybox -- sh  # Run pod as interactive shell
$ kubectl attach my-pod -i                            # Attach to Running Container
$ kubectl port-forward my-pod 5000:6000               # Forward port 6000 of Pod to your to 5000 on your local machine
$ kubectl exec my-pod -- ls /                         # Run command in existing pod (1 container case)
$ kubectl exec my-pod -c my-container -- ls /         # Run command in existing pod (multi-container case)
$ kubectl top pod POD_NAME --containers               # Show metrics for a given pod and its containers
