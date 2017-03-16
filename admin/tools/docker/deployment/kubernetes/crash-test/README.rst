#########################
Use Kubernetes crash test
#########################

To use crash tests:

* Install and start Qserv on k8s
* Stop Qserv 
* Extract data from container to host disk
* Edit ../env.sh so that container mount /qserv/data on host
* Restart k8s cluster
* Run crash tests

.. code-block:: bash

   # once Qserv is running
   ../admin/extract-data.sh
   # WARN edit ../env.sh
   ../stop.sh
   ../start.sh
   # Load data
   ../run-multinode-tests.sh
   ./crash-test.sh
