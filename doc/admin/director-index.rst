.. _admin-director-index:

Director Index
==============

The *director* indexes in Qserv are optional metadata tables associated with the *director* tables, which are explained in:

- :ref:`ingest-api-concepts-table-types` (CONCEPTS)

Each row in the index table refers to the corresponding row in the related *director* table. The association is done via
the unique identifier of rows in the *director* table. In additon to the unique identifier, the index table also contains
the number of a chunk (column ``chunkId``) which contains the row in the *director* table. The index table is used to speed up the queries that
use the primary keys of *director* tables to reference rows.

Here is an example of the index table schema and the schema of the corresponding *director* table ``Object``:

.. code-block:: sql

    CREATE TABLE qservMeta.test101__Object (
        objectId BIGINT NOT NULL,
        chunkId INT NOT NULL,
        subChunkId INT NOT NULL,
        PRIMARY KEY (objectId)
    );

    CREATE TABLE test101.Object (
        objectId BIGINT NOT NULL,
        ..
    );  

The index allows to speed up the following types of queries:

- point queries (when an identifier is known)
- ``JOIN`` queries (when the *director* table is used as a reference table by the dependent tables)

Point queries can be executed without scanning all chunk tables of the *director* table. Once the chunk number is known,
the query will be sent to the corresponding chunk table at a worker node where the table resides. For example,
the following query can be several orders of magnitude faster with the index:

.. code-block:: sql

    SELECT * FROM test101.Object WHERE objectId = 123456;

The index is optional. If the index table is not found in the Qserv Czar's database, queries will be executed
by scanning all chunk tables of the *director* table.

The index table can be built in two ways:

- Automatically by the Qserv Replication/Ingest system during transaction commit time if the corresponding flag
  was set as ``auto_build_secondary_index=1`` when calling the database registration service:

  - :ref:`ingest-db-table-management-register-db` (REST)

  .. note::

    The index tables that are built automatically will be MySQL-partitioned. The partitioning is done
    to speed up the index construction process and to benefit from using the distributed transactions
    mechanism implemented in the Qserv Ingest system:

    - :ref:`ingest-api-concepts-transactions` (CONCEPTS)

    Having too many partitions in the index table can slow down user queries that use the index. Another side
    effect of the partitions is an increased size of the table. The partitions can be consolidated at the database
    *publishing* stage as described in the following section:

    - :ref:`ingest-api-concepts-publishing-data` (CONCEPTS)

- Manually, on the *published* databases using the following service:

  - :ref:`ingest-director-index-build` (REST)

  Note that the index tables built by this service will not be partitioned.

The following example illustrates rebuilding the index of the *director* table ``Object`` that resides in
the *published* database ``test101``:

.. code-block:: bash

    curl localhost:25081/ingest/index/secondary \
        -X POST -H "Content-Type: application/json" \
        -d '{"database":"test101", "director_table":"Object","rebuild":1,"local":1}'

.. warning::

  The index rebuilding process can be time-consuming and potentially affect the performance of user query processing
  in Qserv. Depending on the size of the *director* table, the process can take from several minutes to several hours.
  For *director* tables exceeding 1 billion rows, the process can be particularly lengthy.
  It's recommended to perform the index rebuilding during a maintenance window or when the system load is low.

Notes on the MySQL table engine configuration for the index
-----------------------------------------------------------

The current implementation of the Replication/Ingest system offers the following options for the implementation
of index table:

- ``innodb``: https://mariadb.com/kb/en/innodb/
- ``myisam``: https://mariadb.com/kb/en/myisam-storage-engine/

Each engine has its own pros and cons.

The ``innodb`` engine is the default choice. The option is controlled by the following configuration parameter of the Master
Replication Controller:

- ``(controller,director-index-engine)``

The parameter can be set via the command line when starting the controller:

- ``--controller-director-index-engine=<engine>``
