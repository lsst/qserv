
.. _ingest-api-concepts-contributions:

Table contributions
===================

The API defines the *contribution* as a set of rows that is being ingested into a table via
a separate request. The contribution ingest requests are considered as atomic operations with
a few important caveats:

- The contributions are not committed to the table until the transaction is committed even
  if the contribution request was successful.

- Failed contribution requests have to be evaluated by the workflow to see if the target table
  was left in a consistent state. This is indicated by the ``retry-allowed`` attributed returned
  in the response object. There are two scenarios for what the workflow would do based on The
  value of the flag:

  - ``retry-allowed=0`` - the workflow has to roll back the transaction and make another
    contribution request in a scope of a new transaction. See more on this subject in:

    - :ref:`ingest-api-advanced-transaction-abort` (ADVANCED)
    - :ref:`ingest-trans-management-end` (REST)

  - ``retry-allowed=1`` - the workflow can re-try the contribution in a scope of the same
    transaction using:

    - :ref:`ingest-worker-contrib-retry` (REST)

Note that for contributions submitted by reference, there is an option to configure a request
to automatically retry failed contributions. The maximum number of such retries is controlled
by the ``num_retries`` attribute of the request:

- :ref:`ingest-worker-contrib-by-ref` (REST)

Contributions pushed to the service by value can not be automatically retried. The workflow
would have to decide on the retrying the failed contributions explicitly.

Data (rows) of the contributions are typically stored in the ``CSV``-formatted files. In this
case the files would be either directly pushed to the worker Ingest server or uploaded by
the Ingest service from a location that is accessible to the worker:

- :ref:`ingest-worker-contrib-by-val` (REST)
- :ref:`ingest-worker-contrib-by-ref` (REST)

The first option (ingesting by value) also allows pushing data of contributions
directly from the memory of the client process (worklow) w/o the need to store the data in the files.

Contributions have other important characteristic, such as:

- Each contribution request is always made in a scope of a transaction. This association is
  important for the data provenance and the data tagging purposes.
- The information on contributions is preserved in the persistent state of the Ingest system.
- Contributions have unique identifiers that are assigned by the Ingest system.

The system allows to pull the information on the contributions given their identifiers:

- :ref:`ingest-info-contrib-requests` (REST)

An alternative option is to query the information on contributions submitted in a scope of
a transaction:

- :ref:`ingest-trans-management-status-one` (REST)

The schema of the contribution descriptor objects is covered by:

- :ref:`ingest-worker-contrib-descriptor`
