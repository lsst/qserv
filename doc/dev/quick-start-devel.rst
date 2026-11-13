.. _quick-start-devel:

################################
Quick start guide for developers
################################

Using Qserv with your own custom code or arbitrary versions can be done using Qserv build containers.
Full documentation is available here:
https://rubinobs.atlassian.net/wiki/x/MB3pAg

***********************************
Bootstrap a development environment
***********************************

.. code:: sh

    # Clone qserv and its submodules
    git clone --recurse-submodules https://github.com/lsst/qserv
    cd qserv
    # Build Qserv base images
    ./bin/qserv build-images     # (add `--pull-image` to pull images from ghcr if available)
    # Build Qserv user image
    ./bin/qserv build-user-build-image
    # Configure, compile, and run unit tests
    ./bin/qserv build -j8
    # Open a shell in the build container for incremental work
    ./bin/qserv run-build
    # Rebuild inside the container
    make -j8 install


***********************************************
Building an image and running integration tests
***********************************************

After making your changes, use the following steps to package them up as a Qserv image and run the
integration test suite:

.. code:: sh

    # NOTE: Remove build/ directory to restart the build from scratch
    # (the next build reruns cmake).
    rm -rf build/
    ./bin/qserv build -j8

Integration tests run against a docker-compose cluster that uses the Qserv image created by
``./bin/qserv build``:

.. code:: sh

    ./bin/qserv up                   # start the test cluster
    ./bin/qserv itest                # load test data and run the integration suite
    ./bin/qserv down -v              # stop the cluster and remove its volumes
    ./bin/qserv itest-rm             # remove the reference database volumes

.. note::

   The image name that is run is tagged from ``git describe --dirty``, so it changes with each commit
   or with local uncommitted changes. Rerun ``./bin/qserv build`` before ``up`` to ensure the image
   exists before running the test cluster.
