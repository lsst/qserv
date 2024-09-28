
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

The *unpublished* state is reserved for making significant changes to the table data, the table schema, ingesting new tables, etc.
The replica management operations are not performed on the databases in this state in order to avoid conflicts with the ongoing changes.

Note that newly created databases are always in the *unpublished* state, and they are not visible to the Qserv users. When all desired
tables are ingested into the database and the database is ready for querying, it should be *published* using the following service:

- :ref:`ingest-db-table-management-publish-db` (REST)

..  note::

    Before a database can be published, all transactions open within the context of the database must be either committed or rolled back
    using the following service:

    - :ref:`ingest-trans-management-end` (REST)

    If this condition is not met, the database publishing service will reject the request. It is the responsibility of the workflow
    to manage these transactions.

Databases can also be unpublished to allow adding new tables, or for performaging significant changes to the table data or schema
using the following service:

- :ref:`ingest-db-table-management-unpublish-db` (REST)

..  note::

    The database unpublishing service does not affect the visibility of the database to Qserv users. All tables that existed
    in the database before unpublishing can still be queried by Qserv users. The unpublishing operation is transparent to the users.

Tables
------

Tables in Qserv can be in one of two states: *published* or *unpublished*. This distinction is relevant only for tables within
databases that are in the *unpublished* state. Unlike databases, the state of a table indicates whether the table is fully ingested
and should not be modified thereafter, regardless of the database's state.

Newly created tables are always in the *unpublished* state. Once all desired data is ingested into the table and it is ready for querying,
it should be published. This occurs indirectly when the database is published. After publication, the table is marked as *published*
and becomes visible to Qserv users.

During the table publishing stage the Replication/Ingest system:

- removes MySQL partitions from the data tables at workers
- (optionally) removes MySQL partitions from the *director* index table

The last step is optional. It only applies to the *director* tables, and only if the database was registered with
the optional attribute set as ``auto_build_secondary_index=1`` when calling the service:

- :ref:`ingest-db-table-management-register-db` (REST)`

