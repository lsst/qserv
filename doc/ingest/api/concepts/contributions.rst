
.. _ingest-api-concepts-contributions:

Table contributions
===================

The API defines a *contribution* as a set of rows ingested into a table via a separate request.
These are the most important characteristics of contributions:

- Each contribution request is always made within the scope of a transaction. This association
  is crucial for data provenance and data tagging purposes.
- Information on contributions is preserved in the persistent state of the Ingest system.
- Contributions have unique identifiers assigned by the Ingest system.

.. _ingest-api-concepts-contributions-atomicity:

Request atomicity and retries
-----------------------------

The contribution ingest requests are considered as atomic operations with a few important caveats:

- The contributions are not committed to the table until the transaction is committed even
  if the contribution request was successful.

- Failed contribution requests must be evaluated by the workflow to determine if the target table
  remains in a consistent state. This is indicated by the ``retry-allowed`` attribute returned
  in the response object. Based on the value of this flag, the workflow should proceed as follows:

  - ``retry-allowed=0``: The workflow must roll back the transaction and initiate a new
    contribution request within the scope of a new transaction. For more details, refer to:

    - :ref:`ingest-api-advanced-transactions-abort` (ADVANCED)
    - :ref:`ingest-trans-management-end` (REST)

  - ``retry-allowed=1``: The workflow can retry the contribution within the scope of the same
    transaction using:

    - :ref:`ingest-worker-contrib-retry` (REST)

Note that for contributions submitted by reference, there is an option to configure a request
to automatically retry failed contributions. The maximum number of such retries is controlled
by the ``num_retries`` attribute of the request:

- :ref:`ingest-worker-contrib-by-ref` (REST)

Contributions pushed to the service by value can not be automatically retried. The workflow
would have to decide on the retrying the failed contributions explicitly.

Multiple contributions
----------------------

When ingesting rows into a table (whether *partitioned* or *regular*), the workflow does not need to complete
this in a single step from one input file. The Ingest system supports building tables from multiple contributions.
Contributions can be made within different transactions or multiple contributions can be ingested within the same transaction.
It is the responsibility of the workflow to keep track of what has been ingested into each table, within which transaction,
and to handle any failed transactions appropriately.

Ingest methods
--------------

Data (rows) of the contributions are typically stored in the ``CSV``-formatted files. In this
case the files would be either directly pushed to the worker Ingest server or uploaded by
the Ingest service from a location that is accessible to the worker:

- :ref:`ingest-worker-contrib-by-val` (REST)
- :ref:`ingest-worker-contrib-by-ref` (REST)

The first option (ingesting by value) also allows pushing data of contributions
directly from the memory of the client process (worklow) w/o the need to store the data in the files.

.. _ingest-api-concepts-contributions-status:

Status of the contribution requests
-----------------------------------

The system allows to pull the information on the contributions given their identifiers:

- :ref:`ingest-info-contrib-requests` (REST)

An alternative option is to query the information on contributions submitted in a scope of
a transaction:

- :ref:`ingest-trans-management-status-one` (REST)

The schema of the contribution descriptor objects is covered by:

- :ref:`ingest-worker-contrib-descriptor`
