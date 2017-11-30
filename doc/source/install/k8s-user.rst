#######################
Run Qserv on Kubernetes
#######################

.. note::

   This procedure was tested with Docker 1.12 and Kubernetes 1.5 (Client v1.5.1, Server v1.5.3)

**************
Pre-requisites
**************

- A user account on a Linux machine, named *myhost.domain.org* being able to open a connection to a kubernetes master
- Internet or Docker registry access available
- Docker running
- *myuser* belonging to  *docker* group

  .. code-block:: bash

     sudo usermod -a -G docker myuser

******************
Run multinode test
******************

On the workstation *myhost.domain.org*, get and prepare deployment scripts.

.. code-block:: bash

   git clone git@github.com:lsst/qserv.git
   cd ${SRC_DIR}/qserv/admin/tools/docker/deployment/kubernetes
   # Open a shell in a container providing kubernetes client
   ./run-kubectl.sh

In :file:`~/.kube/env.sh`, set your container configuration (qserv images, attached volumes, ...):

.. literalinclude:: ../../../admin/tools/docker/deployment/kubernetes/env.example.sh
   :language: bash
   :linenos:

.. code-block:: bash

   # Start Qserv (pods and unix services)
   ./admin/start.sh
   # Check Qserv status
   ./admin/status.sh
   # Run multinode tests
   ./run-multinode-tests.sh
   # Stop Qserv
   ./admin/stop.sh

***********
Cheat sheet
***********

See https://kubernetes.io/docs/user-guide/kubectl-cheatsheet/

Interacting with running Pods
-----------------------------

.. code-block:: bash

    kubectl get nodes                            # Get kubernetes nodes 
    kubectl get pods                             # Get kubernetes pods for namespace "default"
    kubectl exec master -it -- bash              # Run command in existing pod (1 container case)
    kubectl exec worker-1 -c worker -- bash      # Run command in existing pod (multi-container case)
    kubectl exec master -- tail -f /var/log      # Display latest logs on Qserv master

    # Log management, not standardized yet
    kubectl logs worker-1 -c mariadb             # dump pod container logs (stdout, multi-container case)
    kubectl logs -f master                       # stream pod logs (stdout)
    kubectl logs -f worker-1 -c mariadb          # stream pod container logs (stdout, multi-container case)

Debug
-----

For debugging purpose, it might be useful not to start Qserv services in pods:

.. code-block:: bash

   # on host machine
   mkdir -p /qserv/custom/bin
   touch /qserv/custom/bin/qserv-start.sh
   chmod u+x /qserv/custom/bin/qserv-start.sh
   # Qserv startup script will be replaced by an empty file

