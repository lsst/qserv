.. _query-scan-params:

Scheduling parameters for queries
=================================

Qserv processes queries in a distributed manner. The queries are executed by the Qserv workers in response to requests
from the Qserv Czar. Queries may have different levels of complexity and may require different resources to be executed.
In order to optimize the execution of queries, avoid overloading the system, and provide rapid response for interactive
queries, Qserv workers have a set of schedulers tuned accordingly. During the initial query analysis, the Qserv Czar
will analyze the query and assign each query a priority number based on the query complexity and special configuration
parameters known to the Czar. The number will be sent to the worker where it will be used for placing the query
into the most appropriate scheduler's queue.

The following factors influence the scheduling of queries in Qserv:

- the number of tables in the query
- the kinds of tables in the query

  - some tables may be significantly bigger than others
  - tables may have different types in Qserv

- the presence of JOINs in the query

  - JOINs may require more resources to be executed
  - JOINs may require more time to be executed

- the presence of the RefMatch table in the query
- the spatial coverage of the query

  - queries which cover a large area of the sky are known in Qserv as the *scan* queries
  - queries that involve a small number of chunks are known as the *interactive* queries

- the configured ``scanRate`` parameter of each table in the query. This parameter represents
  the "weight" of the table in the query scheduling. The higher the value, the more
  resources the table will consume during the query execution.

This document explains how to obtain and configure the ``scanRate`` parameter for tables in Qserv.
These operations are performed via the Qserv Master Replication Controller's REST API.

General Notes
-------------

Basic information about the Qserv Replication Controller's REST API can be found in the following document:

- :ref:`ingest-general` (API)

Retreiving Parameters
---------------------

To retrieve the ``scanRate`` parameter for a table, use the following API call:

- :ref:`ingest-shared-scan-get` (API)

Updating Parameters
-------------------

To update the ``scanRate`` parameter for a table, use the following API call:

- :ref:`ingest-shared-scan-set` (API)
