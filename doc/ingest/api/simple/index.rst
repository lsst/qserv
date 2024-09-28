
.. _ingest-api-simple:

=======================
Simple Workflow Example
=======================

This section provides a practical example of a simple workflow, demonstrating the core API interactions without relying on any automation or
wrapper scripts. Ensure you read the overview before proceeding to the example:

- :ref:`ingest-api-concepts-overview`

Test data
---------

The data used in this example is a small collection of ready-to-use ``CSV`` files and ``JSON`` configurations
located in the directory ``test101``. The directory is available in the Qserv repository at the following subfolder:

- `doc_datasets/test101 <https://github.com/lsst/qserv/tree/main/doc_datasets/test101>`_

The following configuration and data files are available in the directory:

..  code-block::

    database_test101.json

    table_Object.json
    table_Source.json
    table_Filter.json

    data/
        Filter.csv
        Object/
            chunk_7310_overlap.txt
            chunk_7480_overlap.txt
            chunk_7480.txt
        Source/
            chunk_7480.txt

The JSON files provide definitions for the database and tables:

``database_test101.json``
  The database ``test101`` to be created

``table_Object.json``
  The *director* table ``Object``

``table_Source.json``
  The simple *dependent* table ``Source``

``table_Filter.json``
  The *regular* (fully-replicated) table ``Filter``

Data files for these 3 tables are found within the subfolder ``data/``.

Note that the data is already partitioned and ready to be ingested into the Qserv instance. There are two chunks in
the dataset ``7480`` and ``7310``, where the chunk ``7310`` containes only the chunk *overlap*. A table ``Source`` has
no overlaps since it is a dependent table. Only the *director* tables in Qserv can have overlaps.

Qserv setup
-----------

There are a few options for setting up the Qserv instance and the database. This example will be using an existing Kubernetes-based Qserv
instance. For the sake of simplicity, all interactins with the Ingest API will be done using the ``curl`` command from inside
the Replication Controller pod.

.. code-block: bash

    kubectl exec -it qserv-repl-ctl-0 -- /bin/bash

Ater logging into the pod, one can pull the files form the repository into the container:

.. code-block: bash

    cd /home/qserv
    mkdir test101
    cd test101
    for file in database_test101.json table_Object.json table_Source.json table_Filter.json; do
        curl -O https://raw.githubusercontent.com/lsst/qserv/tickets/DM-46111/doc_datasets/test101/data/$file
    done
    mkdir data
    curl -O https://raw.githubusercontent.com/lsst/qserv/tickets/DM-46111/doc_datasets/test101/data/Filter.csv
    mkdir Object
    cd Object
    for file in chunk_7310_overlap.txt chunk_7480_overlap.txt chunk_7480.txt; do
        curl -O https://raw.githubusercontent.com/lsst/qserv/tickets/DM-46111/doc_datasets/test101/data/Object/$file
    done
    cd ..
    mkdir Sourcee
    cd Source
    curl -O https://raw.githubusercontent.com/lsst/qserv/tickets/DM-46111/doc_datasets/test101/data/Source/chunk_7480.txt
    cd ..
    cd ..

The next test is to ensure that the Replication Controller server is running. The server should respond to requests send to the following
service:

.. code-block: bash

    curl http://localhost:8080/meta/version

The response should be a JSON object explained in:

- :ref:`ingest-general-versioning`

Register the database and tables
--------------------------------

Register the database:

.. code-block: bash

    curl http://localhost:8080/ingest/database -X POST -H "Content-Type: application/json" -d @database_test101.json

Register the tables:

.. code-block:: bash

    curl http://localhost:8080/ingest/table -X POST -H "Content-Type: application/json" -d @table_Object.json
    curl http://localhost:8080/ingest/table -X POST -H "Content-Type: application/json" -d @table_Source.json
    curl http://localhost:8080/ingest/table -X POST -H "Content-Type: application/json" -d @table_Filter.json

Start the transaction
---------------------

Start the transaction:

.. code-block:: bash

    curl http://localhost:8080/ingest/trans \
        -X POST -H "Content-Type: application/json" \
        -d '{"database": "test101","auth_key": ""}'

The response should be a JSON object with the transaction ID. The transaction ID is needed for all subsequent
requests (insignificant parts of the response are omitted):

.. code-block:: json

    {  "success" : 1,
        "databases" : {
            "test101" : {
                "transactions" : [
                    {
                        "id" : 84,
                        "state" : "STARTED"
                        "begin_time" : 1730139963298,
                        "start_time" : 1730139963367,
                        "end_time" : 0,
                    }
                ]
            }
        }
    }

Get locations of the workers for ingesting the regular table Filter
-------------------------------------------------------------------

.. code-block:: bash

    curl http://localhost:8080/ingest/regular \
        -X GET -H "Content-Type: application/json" -d '{"transaction_id":84}'

The service returns a JSON object with the locations of the workers for ingesting the regular tables:

.. code-block:: json

    {   "locations" : [

            {   "worker" : "qserv-worker-0",
                "host" : "10.141.0.44",
                "host_name" : "qserv-worker-0.qserv-worker.default.svc.cluster.local",
                "port" : 25002,
                "http_host_name" : "qserv-worker-0.qserv-worker.default.svc.cluster.local",
                "http_host" : "10.141.0.44",
                "http_port" : 25004
            },
            {   "worker" : "qserv-worker-1",
                "host" : "10.141.7.33",
                "host_name" : "qserv-worker-1.qserv-worker.default.svc.cluster.local",
                "port" : 25002,
                "http_host" : "10.141.7.33",
                "http_host_name" : "qserv-worker-1.qserv-worker.default.svc.cluster.local",
                "http_port" : 25004
            },
            {   "worker" : "qserv-worker-2",
                "host" : "10.141.2.45",
                "host_name" : "qserv-worker-2.qserv-worker.default.svc.cluster.local",
                "port" : 25002,
                "http_host" : "10.141.2.45",
                "http_host_name" : "qserv-worker-2.qserv-worker.default.svc.cluster.local",
                "http_port" : 25004
            },
            {   "worker" : "qserv-worker-3",
                "host" : "10.141.4.37",
                "host_name" : "qserv-worker-3.qserv-worker.default.svc.cluster.local",
                "port" : 25002,
                "http_host" : "10.141.4.37",
                "http_host_name" : "qserv-worker-3.qserv-worker.default.svc.cluster.local",
                "http_port" : 25004
            },
            {   "worker" : "qserv-worker-4",
                "host" : "10.141.6.37",
                "host_name" : "qserv-worker-4.qserv-worker.default.svc.cluster.local",
                "port" : 25002,
                "http_host" : "10.141.6.37",
                "http_host_name" : "qserv-worker-4.qserv-worker.default.svc.cluster.local",
                "http_port" : 25004
            }
        ]
    }

According to the response, the data of the *regular* table ``Filter`` have to be pushed to the folowing worker hosts (using
the FQDNs of the hosts):

..  code-block:

    qserv-worker-0.qserv-worker.default.svc.cluster.local
    qserv-worker-1.qserv-worker.default.svc.cluster.local
    qserv-worker-2.qserv-worker.default.svc.cluster.local
    qserv-worker-3.qserv-worker.default.svc.cluster.local
    qserv-worker-4.qserv-worker.default.svc.cluster.local

Where the port numbers are:

- ``25002`` for the binary protocol
- ``25004`` for the HTTP protocol

The next section will be presenting examples for ingesting the data using both protocols.

Get locations of the chunks 7310 and 7480
-----------------------------------------

For chunk ``7310``:

.. code-block:: bash

    curl http://localhost:8080/ingest/chunk \
        -X POST -H "Content-Type: application/json" -d '{"transaction_id":84,"chunk":7310,"auth_key":""}'

.. code-block:: json

    {   "location" : {
            "worker" : "qserv-worker-2",
            "host" : "10.141.2.45",
            "host_name" : "qserv-worker-2.qserv-worker.default.svc.cluster.local",
            "port" : 25002,
            "http_host" : "10.141.2.45",
            "http_host_name" : "qserv-worker-2.qserv-worker.default.svc.cluster.local",
            "http_port" : 25004
        }
    }

For chunk ``7480``:

.. code-block:: bash

    curl http://localhost:8080/ingest/chunk \
        -X POST -H "Content-Type: application/json" -d '{"transaction_id":84,"chunk":7480,"auth_key":""}'

.. code-block:: json

    {   "location" : {
            "worker" : "qserv-worker-3",
            "host" : "10.141.4.37",
            "host_name" : "qserv-worker-3.qserv-worker.default.svc.cluster.local",
            "port" : 25002,
            "http_host" : "10.141.4.37",
            "http_host_name" : "qserv-worker-3.qserv-worker.default.svc.cluster.local",
            "http_port" : 25004
        }
    }

The following map shows the endpoints for ingesting the chunks over the proprietary binary protocol:

- ``7310``: ``qserv-worker-2.qserv-worker.default.svc.cluster.local:25002``
- ``7480``: ``qserv-worker-3.qserv-worker.default.svc.cluster.local:25002``

The endpoints for the HTTP-based protocol are presented below:

- ``7310``: ``qserv-worker-2.qserv-worker.default.svc.cluster.local:25004``
- ``7480``: ``qserv-worker-3.qserv-worker.default.svc.cluster.local:25004``


..  hint:

    Both ``host`` (``http_host``) abd ``host_name`` (``http_host_name``) addresses are provided in the response.
    The former is the IP address. The latter is the fully qualified domain name (FQDN) of the worker host.
    It's recommended to use the FQDN in the Kubernetes-based Qserv deployment where IP addresses of the worker
    pods are not stable. This suggestion applies to both the binary and HTTP protocols, and to any table type.

Ingest the data
---------------

Two alternative options for ingesting the data are presented in this section. The first option is to ingest the data
via the proprietary binary protocol using the following tool:

- :ref:`ingest-tools-qserv-replica-file` (TOOLS)

The second technique is to push the data via the HTTP protocol using the following service:

- :ref:`ingest-worker-contrib-by-val` (REST)

Ingest the data using the binary protocol
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following command will ingest the data of the *regular* table ``Filter`` using the binary protocol:

.. code-block:: bash

    PORT=25002
    TRANS=84
    TABLE_TYPE="R"

    mkdir -p logs

    for idx in $(seq 0 4); do
        WORKER_HOST="qserv-worker-${idx}.qserv-worker.default.svc.cluster.local";
        qserv-replica-file INGEST FILE \
            ${WORKER_HOST} \
            ${PORT} \
            ${TRANS} \
            "Filter" \
            ${TABLE_TYPE} \
            data/Filter.csv \
            --verbose >& logs/Filter_${WORKER_HOST_IDX}.log;
    done

Note the flag ``--verbose`` which will print the summary of the ingestion request. The logs will be saved in
the directory ``logs/``. Here is an example of the output found in ``logs/Filter_0.log``:

..  code-block::

                         Id: 1728535
    Ingest service location: qserv-worker-0.qserv-worker.default.svc.cluster.local:25002
     Transaction identifier: 84
          Destination table: Filter
                      Chunk: 0
           Is chunk overlap: 0
            Input file name: data/Filter.csv
                Start  time: 2024-10-28 20:14:55.922
                Finish time: 2024-10-28 20:14:55.945
               Elapsed time: 0 sec
                 Bytes sent: 75
                  MByte/sec: -nan
         Number of warnings: 0
      Number of rows parsed: 9
      Number of rows loaded: 9

Now ingest the data of the *partitioned* tables ``Object`` and ``Source``:

.. code-block:: bash

    PORT=25002
    TRANS=84
    TABLE_TYPE="P"

    qserv-replica-file INGEST FILE \
        "qserv-worker-2.qserv-worker.default.svc.cluster.local" \
        ${PORT} \
        ${TRANS} \
        "Object" \
        ${TABLE_TYPE} \
        data/Object/chunk_7310_overlap.txt \
        --fields-terminated-by=',' \
        --verbose >& logs/Object_chunk_7310_overlap.log

    qserv-replica-file INGEST FILE \
        "qserv-worker-3.qserv-worker.default.svc.cluster.local" \
        ${PORT} \
        ${TRANS} \
        "Object" \
        ${TABLE_TYPE} \
        data/Object/chunk_7480_overlap.txt \
        --fields-terminated-by=',' \
        --verbose >& logs/Object_chunk_7480_overlap.log

    qserv-replica-file INGEST FILE \
        "qserv-worker-3.qserv-worker.default.svc.cluster.local" \
        ${PORT} \
        ${TRANS} \
        "Object" \
        ${TABLE_TYPE} \
        data/Object/chunk_7480.txt \
        --fields-terminated-by=',' \
        --verbose >& logs/Object_chunk_7480.log

    qserv-replica-file INGEST FILE \
        "qserv-worker-3.qserv-worker.default.svc.cluster.local" \
        ${PORT} \
        ${TRANS} \
        "Source" \
        ${TABLE_TYPE} \
        data/Source/chunk_7480.txt \
        --fields-terminated-by=',' \
        --verbose >& logs/Source_chunk_7480.log

Push the data to workers via the HTTP protocol
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

..  hint::

    The ``curl`` command is used to push the data to the workers. The worker services return responses in JSON format.
    In the examples presented below, the response objects are stored in files with the extension ``.json``.
    The corresponding option is ``-o logs/<file>.json``. Always evaluate the response object to ensure the operation
    was successful.


The following command will ingest the data of the *regular* table ``Filter`` using the HTTP protocol:

.. code-block:: bash

    mkdir -p logs

    for idx in $(seq 0 4); do
        WORKER_HOST="qserv-worker-${idx}.qserv-worker.default.svc.cluster.local";

        curl http://${WORKER_HOST}:25004/ingest/csv \
            -X POST -H 'Content-Type: multipart/form-data' \
            -F 'transaction_id=84'\
            -F 'table=Filter' \
            -F 'file=@data/Filter.csv' \
            -o logs/Filter_${WORKER_HOST_IDX}.json \
            >& logs/Filter_${WORKER_HOST_IDX}.log;
    done

Now ingest the data of the *partitioned* tables ``Object`` and ``Source``:

.. code-block:: bash

    curl http://qserv-worker-2.qserv-worker.default.svc.cluster.local:25004/ingest/csv \
        -X POST -H 'Content-Type: multipart/form-data' \
        -F 'transaction_id=84'\
        -F 'table=Object' \
        -F 'chunk=7310' \
        -F 'overlap=1' \
        -F 'fields_terminated_by=,' \
        -F 'file=@data/Object/chunk_7310_overlap.txt' \
        -o logs/logs/Object_chunk_7310_overlap.json \
        >& logs/logs/Object_chunk_7310_overlap.log

    curl http://qserv-worker-3.qserv-worker.default.svc.cluster.local:25004/ingest/csv \
        -X POST -H 'Content-Type: multipart/form-data' \
        -F 'transaction_id=84'\
        -F 'table=Object' \
        -F 'chunk=7480' \
        -F 'overlap=1' \
        -F 'fields_terminated_by=,' \
        -F 'file=@data/Object/chunk_7480_overlap.txt' \
        -o logs/logs/Object_chunk_7480_overlap.json \
        >& logs/logs/Object_chunk_7480_overlap.log

    curl http://qserv-worker-3.qserv-worker.default.svc.cluster.local:25004/ingest/csv \
        -X POST -H 'Content-Type: multipart/form-data' \
        -F 'transaction_id=84'\
        -F 'table=Object' \
        -F 'chunk=7480' \
        -F 'overlap=0' \
        -F 'fields_terminated_by=,' \
        -F 'file=@data/Object/chunk_7480.txt' \
        -o logs/logs/Object_chunk_7480.json \
        >& logs/logs/Object_chunk_7480.log

    curl http://qserv-worker-3.qserv-worker.default.svc.cluster.local:25004/ingest/csv \
        -X POST -H 'Content-Type: multipart/form-data' \
        -F 'transaction_id=84'\
        -F 'table=Source' \
        -F 'chunk=7480' \
        -F 'overlap=0' \
        -F 'fields_terminated_by=,' \
        -F 'file=@data/Source/chunk_7480.txt' \
        -o logs/logs/Source_chunk_7480.json \
        >& logs/logs/Source_chunk_7480.log

Note that the last 4 commands are overridoimg the default field terminator ``\t`` with the comma ``','``.

Commit the transaction
----------------------

..  code-block:: bash

    curl 'http://localhost:8080/ingest/trans/84?abort=0' \
        -X PUT -H "Content-Type: application/json" \
        -d '{"auth_key": ""}'

This is a synchronous operation. The response will be a JSON object with the status of the operation. If the response object
contains the key ``success`` with the value ``1``, the operation was successful. The workflow may also check the status of
the transaction by making the following request:

..  code-block:: bash

    curl 'http://localhost:8080/ingest/database/test101' \
        -X GET -H "Content-Type: application/json"

The response object will contain the status of the transaction. If the transaction is in the ``FINISHED`` state, the
transaction was successful:

..  code-block:: json

    {   "databases" : {
            "test101" : {
                "is_published" : 0,
                "num_chunks" : 2,
                "transactions" : [
                    {
                        "id" : 84,
                        "database" : "test101",
                        "state" : "FINISHED",
                        "begin_time"      : 1730139963298,
                        "start_time"      : 1730139963367,
                        "end_time"        : 1730156228946,
                        "transition_time" : 1730156228374,
                        "context" : {},
                        "log" : []
                    }
                ]
            }
        },
        "success" : 1,
        "error" : "",
        "error_ext" : {},
        "warning" : "No version number was provided in the request's query.",
    }

Publish the database
---------------------

..  code-block:: bash

    curl 'http://localhost:8080/ingest/database/test101' \
        -X PUT -H "Content-Type: application/json" \
        -d '{"auth_key": ""}'

This is a synchronous operation. The response will be a JSON object with the status of the operation (truncated for brevity
to the key ``success``):

..  code-block:: json

    {   "success" : 1
    }

The database is now published and ready for queries.

Test the catalog
----------------

This can be done by running a few simple queries via the ``mysql`` client:

..  code-block:: bash

    kubectl exec -it qserv-czar-0 -c proxy -- \
        mysql --protocol=tcp -hlocalhost -P4040 -uqsmaster test101

This will open a MySQL client connected to the database ``test101``. The following queries can be run to test the catalog:

..  code-block:: sql

    SELECT * FROM Filter;
    +----------------+----------+------------+----------+--------+
    | qserv_trans_id | filterId | filterName | photClam | photBW |
    +----------------+----------+------------+----------+--------+
    |             84 |        0 | u          |        0 |      0 |
    |             84 |        1 | g          |        0 |      0 |
    |             84 |        2 | r          |        0 |      0 |
    |             84 |        3 | i          |        0 |      0 |
    |             84 |        4 | z          |        0 |      0 |
    |             84 |        5 | y          |        0 |      0 |
    |             84 |        6 | w          |        0 |      0 |
    |             84 |        7 | V          |        0 |      0 |
    +----------------+----------+------------+----------+--------+

..  code-block:: sql

    SELECT COUNT(*) FROM Object;
    +----------+
    | COUNT(*) |
    +----------+
    |     1000 |
    +----------+

..  code-block:: sql

    SELECT COUNT(*) FROM Source;
    +----------+
    | COUNT(*) |
    +----------+
    |     4583 |
    +----------+

..  code-block::

    SELECT * FROM Object LIMIT 1\G
    *************************** 1. row ***************************
          qserv_trans_id: 84
                objectId: 433327840428745
                   iauId: NULL
                   ra_PS: 1.30450574307
             ra_PS_Sigma: 0.0000153903
                 decl_PS: 3.34239540723
           decl_PS_Sigma: 0.0000166903
           radecl_PS_Cov: 0.00000000162187
                   ra_SG: 1.30451383451
             ra_SG_Sigma: 0.000135688
                 decl_SG: 3.34239574427
           decl_SG_Sigma: 0.000145373
           radecl_SG_Cov: -0.00000000107427
                 raRange: NULL
               declRange: NULL
               ...

**Note** the MySQL-specific syntax for the query ``\G``. The ``\G`` is a MySQL-specific command that formats the output
of the query in a more readable way. The output is presented in a vertical format, where each row is presented on a separate
line. The columns are presented in the format ``column_name: value``.

..  code-block:: sql

    SELECT objectId FROM Object LIMIT 10;
    +-----------------+
    | objectId        |
    +-----------------+
    | 433327840428745 |
    | 433327840428744 |
    | 433327840428743 |
    | 433327840428742 |
    | 433327840428741 |
    | 433327840428740 |
    | 433327840428739 |
    | 433327840428746 |
    | 433327840428747 |
    | 433327840428748 |
    +-----------------+
    10 rows in set (0.07 sec)

..  code-block:: sql

    SELECT objectId,decl_PS,ra_PS FROM Object WHERE objectId=433327840428739;
    +-----------------+---------------+---------------+
    | objectId        | decl_PS       | ra_PS         |
    +-----------------+---------------+---------------+
    | 433327840428739 | 3.33619102281 | 1.29801680549 |
    +-----------------+---------------+---------------+

Post-ingest operations
----------------------

The database is now ready for queries. However, the following operations can be performed:

- :ref:`ingest-api-post-ingest`

