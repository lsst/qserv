
.. _ingest-api-concepts-database-families:

Database families
=================

The concept of *database families* originated from the need to correctly distribute data of the *partitioned* tables within Qserv. This allows
Qserv to accurately process queries that ``JOIN`` between the tables of different databases. A database family is a group of databases where
all tables within the family share the same partitioning parameters:

- the number of *stripes*
- the number of *sub-stripes*
- the *overlap* radius

This will ensure that all *chunk* tables with the same chunk number will have:

- the same spatial dimensions in the coordinate system adopted by Qserv
- the same number and sizes of *sub-chunks* within each chunk.

The families are defined by the Replication/Ingest system and are not visible to Qserv users. Each family has a unique
identifier (name). The system uses family names to correctly distribute the data of partitioned tables among Qserv worker
nodes, ensuring that the data of tables joined in queries are co-located on the same worker node.

Families must be defined before the databases and tables are registered in Qserv. The current implementation of the API
automatically creates a new family when the first database with a unique combination of partitioning parameters is registered
in the system using:

- :ref:`ingest-db-table-management-register-db` (REST)

If a family with the same partitioning parameters already exists in the system, the new database will be added to the existing family.
Existing databases and families can be found using the following service:

- :ref:`ingest-db-table-management-config` (REST)

For instructions on partitioning the tables with the desired set of parameters, refer to the following document:

- :ref:`ingest-data` (DATA)
