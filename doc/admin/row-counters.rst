
.. _admin-row-counters:

=========================
Row counters optimization
=========================

.. _admin-row-counters-intro:

Introduction
------------

Shortly after the first public instances of Qserv were made available to the users, it was observed that many users
were launching the following query:

.. code-block:: sql

    SELECT COUNT(*) FROM <database>.<table>

Normally, Qserv would process the query by broadcasting the query to all workers to count rows at each chunk
table and aggregate results into a single number at Czar. This is basically the very same mechanism that is found
behind the *shared scan* (or just *scan*) queries. The performnce of the *scan* queries is known to vary depending on
the following factors:

- the number of chunks in the table of interest
- the number of workers
- and the presence of other competing queries (especially the slow ones)

In the best case scenario such scan would take seconds, in the worst one - many minutes or even hours.
This could quickly cause (and has caused) frustration among users since this query looks like (and in reality is)
the very trivial non-scan query.

To address this situation, Qserv has a built-in optimization that is targeting exactly this class of queries.
Here is how it works. For each data table Qserv Czar would have an optional metadata table to store the number
of rows for each chunk. The table is populated and managed by the Qserv Replication system.
 
Note that this optimization is presently an option. And these are the reasons:

-  Counter collection requires scanning all chunk tables, which would take time. Doing this during
   the catalog *publishing* time would prolong the ingest time and increase the chances of instabilities
   for the workflows (in general, the longer some operation is going - the higher the probability of runing into
   the infrastructure-related faulures).
- The counters are not needed for the purposes of the data ingest *per se*. These are just optimizations for the queries.
- Building the counters before the ingested data have been Q&A-ed may not be a good idea.
- The counters may need to be rebuilt if the data have been changed (after fix ups to the ingested catalogs)

The rest of this section along with the formal description of the corresponding REST services explains how to build
and manage the counters.

.. note::

    In the future, the per-chunk counters will be used for optimizing another class of the unconditional queries
    presented below:

    .. code-block:: sql

        SELECT * FROM <database>.<table> LIMIT <N>
        SELECT `col`,`col2` FROM <database>.<table> LIMIT <N>

    For these "indiscriminate" data probes Qserv would dispatch chunk queries to a subset of random chunks that have enough
    rows to satisfy the requirements specified in ``LIMIT <N>``.


.. _admin-row-counters-build:

Building and deploying
----------------------

.. warning::

    Depending on a scale of a catalog (data size of the affected table), it may take a while before this operation
    will be complete.

.. note::

    Please, be advised that the very same operation could be performed at the catalog publishing time as explained in:

    - :ref:`ingest-db-table-management-publish-db` (REST)

    The choice of doing this at the catalog publishing time, or doing this as a separate operation explained in this document
    is left to the Qserv administrators or developers of the ingest workflows. The general recommendation is to make it
    a separate stage of the ingest workflow. In this case, the overall transition time of a catalog to the final published
    state would be faster. In the end, the row counters optimization is optional, and it doesn't affect the overall
    functionality of Qserv or query results seen by users.

To build and deploy the counters one would need to use the following REST service:

- :ref:`ingest-row-counters-deploy` (REST)

The service needs to be invoked for every table of the ingested catalog. This is the typical example of using this service
that would work regardless if the very same operation was already done before:

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

This would work for tables of any type: *director*, *dependent*, *ref-match*, or *regular* (fully replicated). If the counters
already existed in the Replication system's database, they would still be rescanned and redeployed in there.

It may be a good idea to compare the performance of Qserv for executing the above-mentioned queries before and after running
this operation. Normally, if the table statistics are available at Qserv, it should take a small fraction of
a second (about 10 milliseconds) to see the result on the lightly loaded Qserv.

.. _admin-row-counters-delete:

Deleting
--------

Sometimes, if there is doubt that the row counters were incorrectly scanned, or when Q&A-in the ingested catalog,
a data administrator may want to remove the counters and let Qserv do the full scan of the table instead. This can be done
by using the following REST service:


- :ref:`ingest-row-counters-delete` (REST)

Likewise, the previously explained service, this one should also be invoked for each table needing attention. Here is
an example:

.. code-block:: bash

    curl http://localhost:25080/ingest/table-stats/test101/Object \
      -X DELETE -H "Content-Type: application/json" \
      -d '{"overlap_selector":"CHUNK_AND_OVERLAP","qserv_only":1,"auth_key":""}'

Note that with a combination of the parameters shown above, the statistics will be removed from Qserv only.
So, the system would not need to rescan the tables again should the statistics need to be rebuilt. The counters could be simply
redeployed later at Qserv. To remove the counters from the Replication system's persistent state as well
the request should have ``qserv_only=0``.

An alternative technique explained in the next section is to tell Qserv not to use the counters for optimizing queries.


.. _admin-row-counters-disable:

Disabling the optimization at run-time
---------------------------------------

.. warning::

    This is a global setting that affects all users of Qserv. All new quries will be ru w/o the optimization.
    It should be used with caution. Normally, it is meant to be used by the Qserv data administrator to investigate
    suspected issues with Qserv or the catalogs it serves.

To complement the previously explained methods for scanning, deploying, or deleting row counters for query optimization,
Qserv also supports the run-time switch. The switch is turned on or off by the following statement to be submitted via
the front-ends of Qserv:

.. code-block:: sql

    SET GLOBAL QSERV_ROW_COUNTER_OPTIMIZATION = 1
    SET GLOBAL QSERV_ROW_COUNTER_OPTIMIZATION = 0


The default behavior of Qserv when the variable is not set is to enable the optimization for tables where the counters
are available.

.. _admin-row-counters-retrieve:

Inspecting
----------

It's also possible to retrieve the counters from the Replication system's state using the following REST service:

- :ref:`ingest-row-counters-inspect` (REST)

The information obtained in this way could be used for various purposes, such as investigating suspected issues with
the counters, monitoring data placement in the chunks, or making visual representations of the chunk density maps.
See the description of the REST service for further details on this subject.
