

.. _ingest-api-advanced-unpublishing-databases:

Ingesting tables into the published catalogs
--------------------------------------------

.. warning::

  Currently, the ingest system only supports adding new tables to databases. It does not permit adding rows to previously
  ingested tables. Any attempts to modify existing tables will be blocked by the system.

In some cases, especially when ingesting large-scale catalogs, the input data for all tables may not be available at the start
of the ingest campaign. Some tables may be ready for ingestion earlier than others. Another scenario is when a previously
ingested table needs to be re-ingested with corrected data. In these situations, the catalog must be built incrementally
while allowing Qserv users to access the previously published tables. The previously described workflow of ingesting all
tables at once and then publishing the catalog as a whole would not work here. To address these scenarios, the system allows
temporarily *un-publishing* the catalog to add new or replace existing tables. The following REST service should be used for
this:

- :ref:`ingest-db-table-management-unpublish-db` (REST)

Key points to note:

- This operation is very quick.
- The database state transition is largely transparent to Qserv users, except when replacing an existing table with a newer
  version. The un-published database, including all previously ingested tables, will still be visible and queryable by Qserv
  users.
- The operation requires the ingest workflow to use an administrator-level authorization key. This will be demonstrated in
  the example below.


The modified workflow sequence expected in this case is as follows:

#. Unpublish the existing catalog.
#. Delete an existing table if it needs to be replaced.
#. Register a new table (or a new version of the removed table) or multiple tables as needed.
#. Start transactions.
#. Load contributions for the new tables.
#. Commit transactions.
#. Publish the catalog again.

This sequence can be repeated as needed to modify the catalog. Note that starting from step **3**, this sequence
is no different from the simple scenario of ingesting a catalog from scratch. The last step of the sequence
will only affect the newly added tables. Hence, the performance of that stage will depend only on the scale and the amount
of data ingested into the new tables.

Here is an example of how to unpublish a catalog:

.. code-block:: bash

  curl http://qserv-master01:25081/replication/config/database/test101 \
    -X PUT -H 'Content-Type: application/json' \
    -d'{"admin_auth_key":""}'
