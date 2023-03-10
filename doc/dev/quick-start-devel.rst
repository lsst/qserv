.. _quick-start-devel:

################################
Quick start guide for developers
################################

Using Qserv with your own custom code or arbitrary versions can be done using Qserv build containers.
Full documentation is available here:
https://confluence.lsstcorp.org/display/DM/Qserv-Lite+Builds+in+Containers

***********************************
Bootstrap a development environment
***********************************

.. code:: sh

    # Clone qserv and its submodules
    git clone --recursive https://github.com/lsst/qserv
    cd qserv
    # Build Qserv base images
    qserv build-images
    # Build Qserv user image
    qserv build-user-build-image
    # Open a shell in a development container
    # NOTE: Add '--user=qserv' option to command below if user id equals 1000
    # NOTE: Use --user-build-image in order to avoid rebuilding a Qserv user image after each commit
    qserv run-build --user-build-image docker.io/qserv/lite-build-fjammes:2023.2.1-rc2-10-g6624d8b28
    # Build host code inside Qserv container
    make


*****************************
Build an image for production
*****************************

.. code:: sh

    # NOTE: Remove build/ directory to restart the build from scratch.
    # This can fix build errors.
    rm -rf build/
    # NOTE: Add '--user=qserv' option to command below if user id equals 1000
    qserv build -j8