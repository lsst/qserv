.. _ingest-api-advanced-refmatch:

Ingesting ref-match tables
==========================

..  tip::

    See the following document first:

    - :ref:`ingest-api-concepts-table-types` (CONCEPTS)

..  note::

    The input data for *ref-match* tables must be partitioned differently than other subtypes of *partitioned* tables.
    Detailed instructions on this topic can be found in the section:

    - :ref:`ingest-data-partitioning-ref-match` (DATA)

The *ref-match* tables are a specialized class of *partitioned* tables that depend on (match rows of) two *director* tables.
These referenced *director* tables can be located within the same catalog as the *ref-match* table or in any other catalog
served by the same Qserv instance. The only additional requirement in the latter case is that all databases must belong to
the same database *family* (partitioned using the same values for the parameters ``stripes``, ``sub-stripes``, and ``overlap``).
This requirement is enforced by the table registration service of the Replication/Ingest system. From the system's perspective,
these tables are not different from any other *partitioned* tables. The only changes made to the table registration interface
to specifically support *ref-match* tables are redefining (extending) the syntax of the attribute ``director_table`` and adding
four optional attributes allowed in the JSON configurations of the tables as presented below:

``director_table`` : *string*
  A table referenced here must be the **first** *director* table that must be registered in Qserv before the *ref-match* table.
  The table registration service will refuse the operation if the *director* doesn't exist. The table name may also include
  the name of a database where the table is located if this database differs from the one where the *ref-match* itself
  will be placed. The syntax of the parameter's value:

  ..  code-block::

    [<database-name>.]<table-name>

  Note that the name cannot be empty, and the database (if specified) or table names should not be enclosed in quotes.

  If the database name is provided, the database should already be known to the Replication/Ingest system.

``director_key`` : *string*
  A non-empty string specifying the name of the primary key column of the referenced *director* table must be provided here.
  This column should also be present in the table schema.

``director_table2`` : *string*
  This is the **second** *director* table referenced by the *ref-match* table. The values for this attribute must adhere to the same
  requirements and restrictions as those specified for the ``director_table`` attribute.

``director_key2`` : *string*
  A non-empty string specifying the name of the primary key column of the **second** referenced *director* table must be provided here.
  This column should also be present in the table schema. Note that the name should be different from the one specified in
  the ``director_key`` attribute.

``flag`` : *string*
  The name of a column that stores flags created by the special partitioning tool ``sph-partition-matches``. This column should
  also be present in the table schema. Usually, the column has the SQL type ``UNSIGNED INT``.

``ang_sep`` : *double*
  The maximum angular separation (within the spatial coordinate system) between the matched objects. The value of this parameter
  must be strictly greater than ``0`` and must not exceed the *overlap* value of the database *family*. The table registration service
  will enforce this requirement and refuse to register the table if the condition is violated.

..  note::

    Spatial coordinate columns ``latitude_key`` and ``longitude_key`` are ignored for this class of tables.

Here is an example of the JSON configuration for a *ref-match* table:

..  code-block:: json

    {   "database" : "Catalog-A",
        "table" : "RefMatch_A_Object_B_DeepSource",
        "is_partitioned" : 1,
        "director_table" : "Object",
        "director_key" : "objectId",
        "director_table2" : "Catalog-B.DeepSource",
        "director_key2" : "deepSourceId",
        "flag" : "flags",
        "ang_sep" : 0.01667,
        "schema": [
            {"name" : "objectId", "type" : "BIGINT UNSIGNED"},
            {"name" : "deepSourceId", "type" : "BIGINT UNSIGNED"},
            {"name" : "flag", "type" : "INT UNSIGNED"},
            {"name" : "chunkId", "type" : "INT UNSIGNED"},
            {"name" : "subChunkId", "type" : "INT UNSIGNED"}
        ],
        "auth_key" : ""
    }

The configuration parameters of the *ref-match* tables can also be seen in the responses of the following REST services:

- :ref:`ingest-db-table-management-register-table` (REST)
- :ref:`ingest-db-table-management-config` (REST)
