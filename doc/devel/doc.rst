######################
Generate documentation
######################

Documentation is automatically built and generated on each Travis-CI build. This can also be performed manually by launching script below:
    
.. code:: sh

    curl -fsSL https://raw.githubusercontent.com/lsst/doc-container/master/run.sh | bash -s -- -p <LTD_PASSWORD> ~/src/qserv
