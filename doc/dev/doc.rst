######################
Generate documentation
######################

Documentation is automatically built and generated on each Github Action build. This can also be performed manually by launching script below:

.. code:: sh

    # Set this variable w.r.t your qserv source directory path
    QSERV_SRC_DIR=$HOME/src/qserv
    qserv build-images --pull-image
    # Require '--user=qserv' if uid=1000 on host machine
    rm -rf $QSERV_SRC_DIR/build/doc
    qserv build-docs --cmake --linkcheck --user=qserv