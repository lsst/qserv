#####################################
How to use replication service client
#####################################

Pre-requisites
--------------

- A k8s-hosted Qserv instance embedding a test case dataset.
- `telepresence <https://www.telepresence.io/docs/quick-start/>`_ client installed on workstation

Procedure
---------

.. code:: bash

    # Launch qserv-ingest development container and open an interactive shell inside it
    cd ~/src/qserv-ingest
    ./dev_local.sh

    # Get the current version of the Ingest API
    repcli http://qserv-repl-ctl-0.qserv-repl-ctl:8080/meta/version get

    # Obtaining descriptions of existing databases and database families
    # See https://confluence.lsstcorp.org/display/DM/1.+Obtaining+descriptions+of+existing+databases+and+database+families
    repcli http://qserv-repl-ctl-0.qserv-repl-ctl:8080/replication/config get

    # Get info on transaction for a given database
    # See https://confluence.lsstcorp.org/display/DM/3.+Get+info+on+transactions
    VERSION=17
    repcli -vvvv --json '{"database":"dc2_run2_1i_dr1b","version":'$VERSION'}'  http://qserv-repl-ctl-0.qserv-repl-ctl:8080/ingest/trans post
