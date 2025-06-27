
.. _admin-row-counters:

=========================
Row counters optimization
=========================

.. _admin-row-counters-intro:

Introduction
------------

Soon after the initial public deployment of Qserv, it was noticed that numerous users were executing the following queries
to determine the number of rows in a table:

.. code-block:: sql

    SELECT COUNT(*) FROM <database>.<table>
    SELECT COUNT(*) FROM <database>.<table> LIMIT <N>

Typically, Qserv handles this query by distributing it to all workers, which then count the rows in each chunk table and aggregate the results
at the Czar. This process is akin to the one used for *shared scan* (or simply *scan*) queries. The performance of these *scan* queries can
fluctuate based on several factors:

- The number of chunks in the target table
- The number of workers available
- The presence of other concurrent queries (particularly slower ones)

In the best-case scenario, such a scan would take seconds; in the worst case, it could take many minutes or even hours.
This has led to frustration among users, as this query appears to be (and indeed is) a very trivial non-scan query.

To address this situation, Qserv includes a built-in optimization specifically for this type of query.
Here's how it works: Qserv Czar maintains an optional metadata table for each data table, which stores the row count for each
chunk. This metadata table is populated and managed by the Qserv Replication system. If the table is found, the query
optimizer will use it to determine the number of rows in the table without the need to scan all the chunks.
 
Note that this optimization is currently optional for the following reasons:

- Collecting counters requires scanning all chunk tables, which can be time-consuming. Performing this during
  the catalog *publishing* phase would extend the ingest time and increase the likelihood of workflow instabilities
  (generally, the longer an operation takes, the higher the probability of encountering infrastructure-related failures).
- The counters are not necessary for the data ingest process itself. They are merely optimizations for query performance.
- Building the counters before the ingested data have been quality assured (Q&A-ed) may not be advisable.
- The counters may need to be rebuilt if the data are modified (e.g., after making corrections to the ingested catalogs).

The following sections provide detailed instructions on building, managing, and utilizing the row counters, along with formal
descriptions of the corresponding REST services.

.. note::

    In the future, the per-chunk counters will be used for optimizing another class of unconditional queries
    presented below:

    .. code-block:: sql

        SELECT * FROM <database>.<table> LIMIT <N>
        SELECT `col`,`col2` FROM <database>.<table> LIMIT <N>

    For these "indiscriminate" data probes, Qserv would dispatch chunk queries to a subset of random chunks that have enough
    rows to satisfy the requirements specified in ``LIMIT <N>``.

.. _admin-row-counters-build:

Building and deploying
----------------------

.. warning::

    Depending on the scale of a catalog (data size of the affected table), it may take a while before this operation
    will be complete.

.. note::

    Please, be advised that the very same operation could be performed at the catalog publishing time as explained in:

    - :ref:`ingest-db-table-management-publish-db` (REST)

    The decision to perform this operation during catalog publishing or as a separate step, as described in this document,
    is left to the discretion of Qserv administrators or developers of the ingest workflows. It is generally recommended
    to make it a separate stage in the ingest workflow. This approach can expedite the overall transition time of a catalog
    to its final published state. Ultimately, row counters optimization is optional and does not impact the core functionality
    of Qserv or the query results presented to users.

To build and deploy the counters, use the following REST service:

- :ref:`ingest-row-counters-deploy` (REST)

The service needs to be invoked for every table in the ingested catalog. Here is a typical example of using this service,
which will work even if the same operation was performed previously:

.. code-block:: bash

    curl http://localhost:25080/ingest/table-stats \
      -X POST -H "Content-Type: application/json" \
      -d '{"database":"test101",
           "table":"Object",
           "overlap_selector":"CHUNK_AND_OVERLAP",
           "force_rescan":1,
           "row_counters_state_update_policy":"ENABLED",
           "row_counters_deploy_at_qserv":1,
           "auth_key":""}'

This method is applicable to all table types: *director*, *dependent*, *ref-match*, or *regular* (fully replicated).
If the counters already exist in the Replication system's database, they will be rescanned and redeployed.

It is advisable to compare Qserv's performance for executing the aforementioned queries before and after running this operation.
Typically, if the table statistics are available in Qserv, the result should be returned in a small fraction of
a second (approximately 10 milliseconds) on a lightly loaded Qserv.

.. _admin-row-counters-delete:

Deleting
--------

In certain situations, such as when there is suspicion that the row counters were inaccurately scanned or during the quality
assurance (Q&A) process of the ingested catalog, a data administrator might need to remove the counters and allow Qserv
to perform a full table scan. This can be achieved using the following REST service:

- :ref:`ingest-row-counters-delete` (REST)

Similarly to the previously mentioned service, this one should also be invoked for each table requiring attention. Here is
an example:

.. code-block:: bash

    curl http://localhost:25080/ingest/table-stats/test101/Object \
      -X DELETE -H "Content-Type: application/json" \
      -d '{"overlap_selector":"CHUNK_AND_OVERLAP","qserv_only":1,"auth_key":""}'

Note that with the parameters shown above, the statistics will be removed from Qserv only.
This means the system would not need to rescan the tables again if the statistics need to be rebuilt. The counters could simply
be redeployed later at Qserv. To remove the counters from the Replication system's persistent state as well,
the request should have ``qserv_only=0``.

An alternative approach, detailed in the next section, is to instruct Qserv to bypass the counters for query optimization.


.. _admin-row-counters-disable:

Disabling the optimization at run-time
---------------------------------------

.. warning::

    This is a global setting that affects all users of Qserv. All new queries will be run without the optimization.
    It should be used with caution. Typically, it is intended for use by the Qserv data administrator to investigate
    suspected issues with Qserv or the catalogs it serves.

To complement the previously explained methods for scanning, deploying, or deleting row counters for query optimization,
Qserv also supports a run-time switch. This switch can be turned on or off by submitting the following statements via
the Qserv front-ends:

.. code-block:: sql

    SET GLOBAL QSERV_ROW_COUNTER_OPTIMIZATION = 1
    SET GLOBAL QSERV_ROW_COUNTER_OPTIMIZATION = 0

The default behavior of Qserv, when the variable is not set, is to enable the optimization for tables where the counters
are available.

.. _admin-row-counters-retrieve:

Inspecting
----------

It's also possible to retrieve the counters from the Replication system's state using the following REST service:

.. code-block:: bash

    curl http://localhost:25080/ingest/table-stats/test101/Object \
      -X GET -H "Content-Type: application/json" \
      -d '{"auth_key":""}'

- :ref:`ingest-row-counters-inspect` (REST)

The retrieved information can be utilized for multiple purposes, including investigating potential issues with the counters,
monitoring data distribution across chunks, or creating visual representations of chunk density maps. Refer to the REST service
documentation for more details on this topic.
