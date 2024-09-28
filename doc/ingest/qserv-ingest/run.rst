
########################################
Run Qserv ingest on a Kubernetes cluster
########################################

Prerequisites
=============

- An up and running Qserv instance, managed by `qserv-operator <https://qserv-operator.lsst.io>`_ inside a k8s cluster:

.. code:: sh

    $ kubectl get qserv
    NAME    AGE
    qserv   10d

    $ kubectl get pods
    NAME                              READY   STATUS    RESTARTS   AGE
    qserv-czar-0                      3/3     Running   6          10d
    qserv-ingest-db-0                 1/1     Running   2          10d
    qserv-repl-ctl-0                  1/1     Running   6          10d
    qserv-repl-db-0                   1/1     Running   2          10d
    qserv-worker-0                    5/5     Running   0          10d
    qserv-worker-1                    5/5     Running   0          10d
    ...
    qserv-worker-7                    5/5     Running   0          10d
    qserv-worker-8                    5/5     Running   0          10d
    qserv-worker-9                    5/5     Running   0          10d
    qserv-xrootd-redirector-0         2/2     Running   0          10d
    qserv-xrootd-redirector-1         2/2     Running   0          10d

The namespace containing this instance will be called ``<QSERV_NAMESPACE>``.

-  Save ``<QSERV_NAMESPACE>`` as the namespace  for all subsequent ``kubectl`` commands:

.. code:: sh

    kubectl config set-context --current --namespace=<QSERV_NAMESPACE>

For additional informations, check official documentation for `setting the namespace preference <https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/#setting-the-namespace-preference>`_

- Privileges to create pods and persistent volumes inside ``<QSERV_NAMESPACE>``.

- An HTTP(s) server providing access to input data and metadata. All pods inside ``<QSERV_NAMESPACE>`` must be able to access this HTTP server.

- An instance of Argo Workflow running in ``<QSERV_NAMESPACE>``, and the ``argo`` client.
  The example script ``prereq-install.sh`` located at the top-level of the `<https://github.com/lsst-dm/qserv-ingest>`_ repository allows to install a version which is supported by the ingest process.
  It will also install ``helm``, a package manager for Kubernetes.

.. code:: sh

    $ kubectl get pod -l app.kubernetes.io/part-of=argo-workflows
    NAME                                                  READY   STATUS    RESTARTS   AGE
    argo-workflows-server-9f9bd4cc4-wkm5z                 1/1     Running   0          1m
    argo-workflows-workflow-controller-57779f5f96-2r9jf   1/1     Running   1          1m

- `git-lfs <https://git-lfs.com>`__ is required in order to retrieve integration test datasets.

Prepare and configure Qserv ingest
==================================

Get the project
---------------

.. code:: sh

    RELEASE="2022.1.1-rc1"
    git clone --single-branch -b "$RELEASE" --depth 1 https://github.com/lsst-dm/qserv-ingest
    cd qserv-ingest

.. _Configuration:

Configuration
-------------

A directory with ingest workflow configuration and an ``env.sh`` file have to be created:

.. code:: sh

    cp -r manifests/in2p3-cosmo manifests/<CUSTOM_INGEST>
    cp env.example.sh env.sh

#. In ``env.sh``, set ``OVERLAY`` to ``<CUSTOM_INGEST>``, and eventually ``INSTANCE`` to the name of current Qserv instance.
#. Configure ingest workflow configuration file (named ``manifests/<CUSTOM_INGEST>/configmap/ingest.yaml``):

..
    Copy example configuration file in current directory in order to update documentation
    cp ~/src/qserv-ingest/manifests/base/configmap/ingest.yaml ~/src/qserv/doc/admin/qserv-ingest/

Inline documentation for this configuration file is avalaible at ``manifests/base/configmap/ingest.yaml``:

.. literalinclude:: ingest.yaml
   :language: yaml
   :linenos:


Launch Qserv ingest
===================

Launch the workflow using `Argo <https://argoproj.github.io/>`_

.. code:: sh

    ./argo-submit.sh
    # monitor the workflow execution
    argo get @latest

Then adapt ``example/query.sh`` to launch a few queries against freshly ingested data.


Delete an existing database
===========================

Please refer to `Qserv Replication Service documentation <https://confluence.lsstcorp.org/display/DM/6.+Deleting+databases+and+tables>`_,
and then adapt example script ``example/delete_database.sh``.


Run interactively a workflow step
=================================


.. code:: sh

    ./argo-submit.sh -s
    # Retrive the pod name for the 'interactive' step
    argo get @latest
    # Open a shell inside it
    kubectl exec -it qserv-ingest-2hrcf-595146013 -c main bash
    # All binaries for launching benchmark steps are located here:
    ls /ingest/bin/
