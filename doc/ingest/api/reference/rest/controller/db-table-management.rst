Database and table management
=============================

.. _ingest-db-table-management-config:

Finding existing databases and database families
------------------------------------------------

The following service pulls all configuration information of of the Replication/Ingest System, including info
on the known database families, databases and tables:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``GET``
      - ``/replication/config``

Upon successful (see :ref:`ingest-general-error-reporting`) completion of the request, the service will return an object
that has the following schema (of which only the database and database family-related fields are shown):

.. code-block:: json

    {
        "config": {
            "database_families" : [
                {
                    "overlap" :               0.01667,
                    "min_replication_level" : 3,
                    "num_sub_stripes" :       3,
                    "name" :                  "production",
                    "num_stripes" :           340
                }
            ],
            "databases" : [
                {
                    "database" :     "dp01_dc2_catalogs_02",
                    "create_time" :  0,
                    "is_published" : 1,
                    "publish_time" : 1662688661000,
                    "family_name" :  "production",
                    "tables" : [
                        {
                            "ang_sep" :                 0,
                            "is_director" :             1,
                            "latitude_key" :            "coord_dec",
                            "create_time" :             1662774817703,
                            "unique_primary_key" :      1,
                            "flag" :                    "",
                            "name" :                    "Source",
                            "director_database_name" :  "",
                            "is_ref_match" :            0,
                            "is_partitioned" :          1,
                            "longitude_key" :           "coord_ra",
                            "database" :                "dp02_dc2_catalogs",
                            "director_table" :          "",
                            "director_key2" :           "",
                            "director_database_name2" : "",
                            "director_key" :            "sourceId",
                            "director_table2" :         "",
                            "director_table_name2" :    "",
                            "is_published" :            1,
                            "director_table_name" :     "",
                            "publish_time" :            1663033002753,
                            "columns" : [
                                {
                                    "name" : "qserv_trans_id",
                                    "type" : "INT NOT NULL"
                                },
                                {
                                    "type" : "BIGINT NOT NULL",
                                    "name" : "sourceId"
                                },
                                {
                                    "type" : "DOUBLE NOT NULL",
                                    "name" : "coord_ra"
                                },
                                {
                                    "type" : "DOUBLE NOT NULL",
                                    "name" : "coord_dec"
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    }

**Notes**:

- The sample object was truncated for brevity. The actual number of families, databases, tables and columns were
  much higher in the real response.
- The number of attributes varies depending on a particular table type. The example above shows
  attributes for the table ``Source``. This table is *partitioned* and is a *director* (all *director*-type tables
  are partitioned in Qserv).


.. _ingest-db-table-management-register-db:

Registering databases
----------------------

Each database has to be registered in Qserv before one can create tables and ingest data. The following
service of the Replication Controller allows registering a database:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - ``/ingest/database``

The service requires a JSON object of the following schema:

.. code-block::

    {
        "database" :                   <string>,
        "num_stripes" :                <number>,
        "num_sub_stripes" :            <number>,
        "overlap" :                    <number>,
        "auto_build_secondary_index" : <number>,
        "local_load_secondary_index" : <number>
    }

Where:

``database`` : *string*
  The required name of the database to be created.

``num_stripes`` : *number*
  The required number of stripes that was used when partitioning data of all tables to be ingested in a scope of the database.

``num_sub_stripes`` : *number*
  The required number of sub-stripes that was used when partitioning data of all tables to be ingested in a scope of the database.

``overlap`` : *number*
  The required overlap between the stripes.

``auto_build_secondary_index`` : *number* = ``1``
  The flag that specifies the desired mode for building the *director* (used to be known as the *secondary*)
  indexes of the director tables of the catalog. The flag controls the automatic building of the indexes, where:
  
  - ``1``: Build the index automatically during transaction commit time.
  - ``0``: Do not build the index automatically during transaction commit time. In this case, it will be up to a workflow
    to trigger the index building as a separated "post-ingest" action using the corresponding service:

    - :ref:`ingest-director-index-build`

  **Note**: Catalogs in Qserv may have more than one director table. This option applies to all such tables.

.. warning::

    - The service will return an error if the database with the same name already exists in the system.
    - Values of attributes ``num_stripes``, ``num_sub_stripes`` and ``overlap`` are expected to match
      the corresponding partitioning parameters used when partitioning all partitioned tables of the new database.
      Note that the current implementation of the Qserv Ingest system will not validate contributions to the partitioned
      tables to enforce this requirement. Only the structural correctness will be checked. It's up to a workflow
      to ensure the data ingested into tables are correct.
    - Building the *director* index during transaction commit time (for the relevant tables) may have a significant
      impact on the performance of the transaction commit operation. The impact is proportional to the size of the
      contributions made into the table during the transaction. This may orotolng the transaction commit time.
      An alternative option is to build the indexes as a separated "post-ingest" action using the corresponding service:

      - :ref:`ingest-director-index-build`

If the operation is successfully finished (see :ref:`ingest-general-error-reporting`) a JSON object returned by the service
will have the following attribute:

.. code-block::

    {
        "database": {
            ...
        }
    }

The object containing the database configuration information has the same schema as it was explained earlier in section:

- :ref:`ingest-db-table-management-config`


.. _ingest-db-table-management-register-table:

Registering tables
------------------

All tables, regardless if they are *partitioned* or *regular* (fully replicated on all worker nodes), have to be registered
using the following Replication Controller's service:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - ``/ingest/table``

The service requires a JSON object of the following schema:

Where a JSON object sent to the service with the request shall describe that table. This is a schema of the object for
the **partitioned** tables is presented below:

.. code-block::

    {
        "database"             : <string>,
        "table"                : <string>,
        "is_partitioned"       : <number>,
        "schema" : [
            {   "name" :    <string>,
                "type" :    <string>
            },
            ...
        ],
        "director_table"       : <string>,
        "director_key"         : <string>,
        "director_table2"      : <string>,
        "director_key2"        : <string>,
        "latitude_key"         : <string>,
        "longitude_key"        : <string>,
        "flag"                 : <string>,
        "ang_sep"              : <double>,
        "unique_primary_key"   : <number>
    }

A description of the *regular* tables has a fewer number of attributes (attributes that which are specific to the *partitioned*
tables are missing):

.. code-block::

    {
        "database" :        <string>,
        "table" :           <string>,
        "is_partitioned" :  <number>,
        "schema": [
            {
                "name" :    <string>,
                "type" :    <string>
            },
            ...
        ]
    }

Where the attributes are:

``database`` : *string*
  The required name of the existing database.

``table`` : *string*
  The required name of a table to be created.

``is_partitioned`` : *number*
  The required type of table. Allowed values:

  - ``1`` for partitioned tables (including any subtypes)
  - ``0`` for the regular tables.

``schema`` : *array*
  The required definition of the table schema, where each entry of the array is an object with the following attributes:

  - ``name``: The name of the column.
  - ``type``: The type of the column. The type must adhere to the MySQL requirements for column types.

``director_table`` : *string*
  The name of the corresponding first (or left) *director* table. The name is required to be not empty for
  the *dependent* tables and it has to be empty for the *director* tables. This is the only way to differentiate between
  two types of *partitioned* tables.

  **Note**: The *ref-match* tables are considered as the *dependent* tables since they have columns that are pointing
  to the corresponding *director* tables. See attributes: ``director_key``, ``director_table2``, and ``director_key2``.

``director_key`` : *string*
  The required name of a column in a *partitioned* table. A role of the column depends on a subtype of
  the table:

  - *director*: the primary key of the table
  - *dependent*: the foreign key pointing to the corresponding column of the *director* table

``director_table2`` : *string*
  The name of the corresponding second (or right) *director* table. The non-empty value
  name is required for the *ref-match* tables and it has to be empty for the *director* and *dependent* tables.

  **Note**: The very presence of this attribute in the input configuration would imply an intent to register
  a "ref-match*  table. In this case, non-empty values of the attributes ``director_key2`` , ``flag`` and ``ang_sep``
  will be required in order to succeed with the registration.

``director_key2`` : *string*
  The name of a column that is associated (AKA *foreign key*) with corresponding column of the second *director* table.
  A value of this attribute is required for and it must not be empty when registering the *ref-match*  tables.
  It will be ignored for other table types. See a description of the attribute ``director_table2``.

``latitude_key`` : *string*
  The required name of a column in a *director* table represents latitude. It's optional for the *dependent* tables.

``longitude_key`` : *string*
  The required name of a column in a *director* table represents longitude. It's optional for the *dependent* tables.

``flag`` : *string*
  The name of the special column that is required to be present on the *ref-match* tables.
  Values of the column are populated by the tool ``sph-partition-matches`` when partitioning the input files
  of the *ref-match* tables. The data type of this column is usually:

  .. code-block:: sql

      INT UNSIGNED

``ang_sep`` : *double*
  The value of the angular separation for the matched objects that is used by Qserv to process queries which
  involve the *ref-match* tables. The value is in radians. The value is required to be non-zero for the *ref-match* tables.

``unique_primary_key`` : *number* = ``0``
  The optional flag allows to drop the uniqueness requirement for the *director* keys of the table. The parameter
  is meant to be used for testing new table products, or for the *director* tables that won't have any dependants (child tables).
  Allowed values:

  - ``0``: The primary key is not unique.
  - ``1``: The primary key is unique.

.. warning::

    - The table schema does not include definitions of indexes. Those are managed separately after the table is published.
      The index management interface is documented in a dedicated document

      - **TODO**: Managing indexes of MySQL tables at Qserv workers.

    - The service will return an error if the table with the same name already exists in the system, or
      if the database didn' exist at a time when teh request was delivered to the service.

    - The service will return an error if the table schema is not correct. The schema will be checked for the correctness.

.. note:: Requirements for the table schema:

    - The variable-length columns are not allowed in Qserv for the *director* and *ref-match* tables. All columns of these
      tables must have fixed lengths. These are the variable length types: ``VARCHAR``, ``VARBINARY``, ``BLOB``, ``TEXT``,
      ``GEOMETRY`` and ``JSON``.

    - The *partitioned* tables are required to have parameters ``director_key``, ``latitude_key`` and ``longitude_key``.
    - The *director* tables are required to have non-empty column names in the parameters  ``director_key``, ``latitude_key`` and ``longitude_key``.
    - The *dependent* tables are required to have a non-empty column name specified in the parameter ``director_key``.
    - The *dependent* tables are allowed to have empty values in the parameters ``latitude_key`` and ``longitude_key``.

    - For tables where the attributes ``latitude_key`` and ``longitude_key`` are provided (either because they are required
      of if they are optional), values must be either both non-empty or empty. An attempt to specify only one of the attribute
      or have a non-empty value in an attribute while the other one has it empty will result in an error.

    - All columns mentioned in attributes ``director_key``, ``director_key2``, ``flag``, ``latitude_key`` and ``longitude_key``
      must be present in the table schema.

    - Do not use quotes around the names or type specifications.

    - Do not start the columm names with teh reserved prefix ``qserv``. This prefix is reserved for the Qserv-specific columns.

An example of the schema definition for the table ``Source``:

.. code-block:: json

    [
        {
            "name" : "sourceId"
            "type" : "BIGINT NOT NULL",
        },
        {
            "name" : "coord_ra"
            "type" : "DOUBLE NOT NULL",
        },
        {
            "name" : "coord_dec"
            "type" : "DOUBLE NOT NULL",
        }
    ]

If the operation is successfully finished (see :ref:`ingest-general-error-reporting`) a JSON object returned by the service
will have the following attribute:

.. code-block::

    {
        "database": {
            ...
        }
    }

The object will contain the updated database configuration information that will also include the new table.
The object will have the same schema as it was explained earlier in section:

- :ref:`ingest-db-table-management-config`

**Notes on the table names**:

- Generally, the names of the tables must adhere to the MySQL requirements for identifiers
  as explained in:

  - https://dev.mysql.com/doc/refman/8.0/en/identifier-qualifiers.html

- The names of identifiers (including tables) in Qserv are case-insensitive. This is not the general requirement
  in MySQL, where the case sensitivity of identifiers is configurable one way or another. This requirement
  is enforced by the configuration of  MySQL in Qserv.

- The length of the name should not exceed 64 characters as per:

  - https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html

- The names should **not** start with the prefix ``qserv``. This prefix is reserved for the Qserv-specific tables.


.. _ingest-db-table-management-publish-db:

Publishing databases
--------------------

Databases are published (made visible to Qserv users) by calling this service:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``PUT``
      - ``/ingest/database/:database``

The name of the database is provided as a parameter ``database`` of the resource path. There are a few optional
parameters to be sent in the JSON body of the request:

.. code-block::

    {
        "consolidate_secondary_index" :  <number>,
        "row_counters_deploy_at_qserv" : <number>
    }

Where:

``consolidate_secondary_index`` : *number* = ``0``
  The optional parameter that controls the final format of all the *director* index tables of the database. 
  Normally, the *director* indexes are MySQL-partitioned tables.  If the value of this optional parameter is
  not ``0`` then the Ingest System will consolidate the MySQL partitions and turn the tables into the monolitical form.

  .. warning::

      Depending on the scale of the catalog (sizes of the affected tables), this operation may be quite lengthy (up to many hours).
      Besides, based on the up to the date experience with using the MySQL-partitioned director indexes, the impact of the partitions
      on the index's performance is rather negligible. So, it's safe to ignore this option in most but very special cases that are not
      discussed by the document.

  One can find more info on the MySQL partitioning at:

  - https://dev.mysql.com/doc/refman/8.0/en/partitioning.html

``row_counters_deploy_at_qserv`` : *number* = ``0``
  This optional flag that triggers scanning and deploying the row counters as explained at:

  - :ref:`admin-row-counters` (ADMIN)
  - :ref:`ingest-row-counters-deploy` (REST)

  To trigger this operation the ingest workflow should provide a value that is not ``0``. In this case the row counters
  collection service will be invoked with the following combination of parameters:

  ..  list-table::
      :widths: 50 50
      :header-rows: 1

      * - attr
        - value
      * - ``overlap_selector``
        - ``CHUNK_AND_OVERLAP``
      * - ``force_rescan``
        - ``1``
      * - ``row_counters_state_update_policy``
        - ``ENABLED``
      * - ``row_counters_deploy_at_qserv``
        - ``1``

.. warning::

    The row counters deployment is a very resource-consuming operation. It may take a long time to complete
    depending on the size of the catalog. This will also delay the catalog publiushing stage of an ingest compaign.
    A better approach is to deploy the row counters as the "post-ingest" operation as explained in:

    - (**TODO** link) Deploying row counters as a post-ingest operation

.. note::

    The catalogs may be also unpublished to add more tables. The relevant REST service is documented in:

    - (**TODO** link) Un-publishing databases to allow adding more tables


.. _ingest-db-table-management-unpublish-db:

Un-publishing databases to allow adding more tables
---------------------------------------------------

Unpublished databases as well as previously ingested tables will be still visible to users of Qserv.
The main purpose of this operation is to allow adding new tables to the existing catalogs.
The new tables won't be seen by users until the catalog is published back using the following REST service:

- :ref:`ingest-db-table-management-publish-db`

Databases are un-published by calling this service:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``PUT``
      - ``/replication/config/database/:database``

The name of the database is provided in a parameter ``database`` of the resource. The only mandatory parameter
to be sent in the JSON body of the request is:

``admin_auth_key`` : *string*
  The administrator-level authentication key that is required to publish the database.
  The key is used to prevent unauthorized access to the service.

  **Note**: The key is different from the one used to publish the database. The elevated privileges
  are needed to reduce risks of disrupting user access to the previously loaded and published databases.


.. _ingest-db-table-management-delete:

Deleting databases and tables
-----------------------------

These services can be used for deleting non-*published* (the ones that are still ingested) as well as *published* databases,
or tables, including deleting all relevant persistent structures from Qserv:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``DELETE``
      - | ``/ingest/database/:database``
        | ``/ingest/table/:database/:table``

To delete a non-*published* database (or a table from such database) a client has to provide the normal level authentication
key ``auth_key`` in a request to the service:

.. code-block::

    {   "auth_key" : <string>
    }

The name of the databases affected by the operation is specified at the resource's path.

Deleting databases (or tables from those databases) that have already been published requires a user to have
elevated administrator-level privileges. These privileges are associated with the authentication key ``admin_auth_key``
to be sent with a request instead of ``auth_key``:

.. code-block::

    {   "admin_auth_key" : <string>
    }

Upon successful completion of the request (for both above-mentioned states of the database), the service will return the standard
response as explained in the section mentoned below. After that, the database (or the table, depending on a scope of a request)
name can be reused for further ingests if needed.

- :ref:`ingest-general-error-reporting`

