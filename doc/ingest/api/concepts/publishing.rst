
.. _ingest-api-concepts-publishing-data:

Publishing databases and tables
===============================

Databases
---------

Databases in Qserv can be in one of two states: *published* or *unpublished*. Databases in the *published* state
are visible to Qserv users and can be queried. Generally, databases in this state are considered static and cannot be modified.
However, certain operations are still allowed on the tables of published databases. These operations are documented in
the following section:

- :ref:`ingest-api-post-ingest`

Databases in the *published* state are also subject to routine replica management operations performed by
the Qserv Replication system.

Databases that are in the opposite (*unpublished*) state are not visible to the Qserv users. This state is reserved for making
significant changes to the table data, the table schema, ingesting new tables, etc. The replica management operations are not
performed on the databases in this state in order to avoid conflicts with the ongoing changes.

Newly created databases are always in the *unpublished* state. When all desired tables are ingested into th edatabase and
the database is ready for querying, it should be published. The database can be published using the following service:

- :ref:`ingest-db-table-management-publish-db` (REST)

Databases can also be unpublished to allow adding new tables, or for performaging significant changes to the table data or schema
using the following service:

- :ref:`ingest-db-table-management-unpublish-db` (REST)

Tables
------

Tables in Qserv can be in one of two states: *published* or *unpublished*. This separation exists only for the tables of
databases that are in the *unpublished* state. Unlike databases, this state plays a different role for tables. It's meant to
indicate that the table is fully ingested and should not be modified while the database is in the *unpublished* state.

Newly created tables are always in the *unpublished* state. When all desired data is ingested into the table and the table is
ready for querying, it should be published. This is done indirectly when the database is published. After that, the table is marked
as *published* and is visible to Qserv users.

During the table publishing stage the Replication/Ingest system:

- removes MySQL partitions from the data tables at workers
- (optionally) removes MySQL partitions from the *director* index table

The last step is optional. It only applies to the *director* tables, and only if the database was registered with
the optional attribute set as ``auto_build_secondary_index=1`` when calling the service:

- :ref:`ingest-db-table-management-register-db` (REST)`

