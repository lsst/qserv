##############################
Run Qserv on top of Kubernetes
##############################

Deployment
==========

The offical way to install Qserv is to run it on top of Kubernetes using
the `qserv-operator <https://qserv-operator.lsst.io>`_.

Administration
==============

Access to Qserv dashboard
-------------------------

This access is only available for users having access to the cluster with ``kubectl``.

.. code:: bash

    # Open a tunnel between local machine and Qserv dashboard
    kubectl port-forward qserv-repl-ctl-0 8080 &

    # Access to the dashboard
    firefox http://localhost:8080