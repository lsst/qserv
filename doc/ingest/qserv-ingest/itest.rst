############################
About integration test cases
############################

Integration test datasets
=========================

Chunk contribution files and replication service configuration based on Qserv integration test cases ``case01`` and ``case03`` are manually generated using script ``qserv/itest/get-testdata.sh`` on a developememt workstation.
They are located in directories ``itest/datasets/case01`` and ``itest/datasets/case03``.

**WARNING**: Qserv integration test cases ``case02`` and ``case04`` are not yet used for qserv-ingest integration tests

How to add an integration test case to qserv-ingest
===================================================

Pre-requisites
--------------

- A k8s-hosted Qserv instance embedding a test case dataset.
- `telepresence <https://www.telepresence.io/docs/latest/quick-start/>`_ client installed on workstation

Procedure
---------

.. code:: bash

    # Launch qserv-ingest development container and open an interactive shell inside it
    cd ~/src/qserv-ingest
    ./dev_local.sh
    CASE_ID="case03" && rm -rf /tmp/dbbench && mkdir /tmp/dbbench
    dbbench --url mysql://qsmaster:@qserv-czar:4040 --database q"$CASE_ID" ./itest/datasets/"$CASE_ID"/dbbench.ini

    # Switch to development workstation
    CASE_ID="case03"
    rm -rf /tmp/dbbench-expected
    docker cp qserv-ingest:/tmp/dbbench/ /tmp/dbbench-expected
    tar -C /tmp -zcvf ./itest/datasets/"$CASE_ID"/dbbench-expected.tgz dbbench-expected

