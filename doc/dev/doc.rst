######################
Generate documentation
######################

Documentation is automatically built and generated on each Github Action build. This may also be done locally by manually running the following script:

.. code:: sh

    # Set this variable w.r.t your qserv source directory path
    QSERV_SRC_DIR=$HOME/src/qserv
    qserv build-images --pull-image
    # Require '--user=qserv' if uid=1000 on host machine
    rm -rf $QSERV_SRC_DIR/build/doc
    # Run this only command to regenerate documentation
    qserv build-docs --cmake --linkcheck --user=qserv

Documentation can then be accessed locally with a web browser:

.. code:: sh

    # Access documentation
    firefox $QSERV_SRC_DIR/build/doc/html/index.html