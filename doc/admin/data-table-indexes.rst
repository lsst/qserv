
.. _admin-data-table-index:

==================
Data Table Indexes
==================

.. note::

    All operations on indexes are only allowed on databases that are in the published state. The system will
    refuse all requests on databases that are in a process of being ingested. This is done for three reasons:
    
    #. To avoid interfering with the catalog ingest operations.
    #. To prevent returning an inconsistent (or wrong) state of the indexes
    #. To prevent transient errors due to potential race conditions since the overall state of the distributed catalogs
       might be being changed by the Ingest system.

.. _admin-data-table-index-intro:

Introduction
------------

This document explains how to use the index management API of the Qserv Replication System. Services provided by the system
are related to the following index management operations in MySQL:

.. code-block:: sql

    SHOW INDEX FROM <table> ...
    CREATE ... INDEX <index> ON <table> ...
    DROP INDEX ON <table> ...

However, unlike the single table operations listed above, the services allow managing groups of related tables distributed
across Qserv worker nodes. Hence, each request for any service refers to a single such group. In particular:

- if a request refers to the *regular* (fully replicated) table then all instances of the table will be affected by the request.
- otherwise (in the case of the *partitioned* tables) each replica of every chunk of the table will be included in an operation.
  Note that the index management services differentiate between the *chunk* tables themselves and the corresponding *full overlap*
  tables. When submitting a request, a user will have to choose which of those tables will be included in the operation.

The latter would also be reflected in the result and error reports made by the services. The JSON  objects returned by
the services would return the names of the *final* tables affected by the operations, not the names of the corresponding
*base* name of a table specified in service requests. This is done to facilitate investigating problems with Qserv should
they occur. See more on the *base* and *final* table names in the section:

- :ref:`ingest-general-base-table-names` (REST)

Things to consider before creating indexes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The recommendations mentioned in this section are not complete. Please, get yourself familiarized with
the MySQL documentation on indexes before attempting the operation. Read the following instructions on
the index creation command:

- https://dev.mysql.com/doc/refman/8.4/en/create-index.html

Always keep in mind that there is the MySQL (or alike) machinery behind Qserv and that the data tables
in Qserv are presently based on the MyISAM storage engine:

-  https://dev.mysql.com/doc/refman/8.4/en/myisam-storage-engine.html

These are a few points to consider.

General:

- Indexes are created on tables, and not on views.
- Indexes are created in the scope of the table, and not databases.
- In the case of the *partitioned* tables, the *chunk* and *full* overlap may not need to have the same set of indexes.
  In some cases, it may not be even possible, for instance, due to different ``UNIQUE`` constraints requirements.
- Please, provide a reasonable comment for an index, even though, comments aren't mandatory. Your comments could be useful
  for bookkeeping purposes to know why and what the index was created for.
- Be aware that indexes take additional space on the Qserv workers' filesystems where the database tables are residing.
  Potentially, if too many indexes were created, MySQL may run out of disk space and stop working. The rule of thumb for
  estimating the amount of space to be taken by an index is based on the sizes of columns mentioned in an index
  specification (explained in the next section) multiplied by the total number of rows in a table (on which the index
  is being created). There are other factors here, such as the type of a table (regular or partitioned) as well as
  the number of replicas for the partitioned tables. The index management service which is used for creating indexes
  will also make the best attempt to prevent creating indexes if it will detect that the amount of available disk space
  is falling too low. In this case, the service will refuse to create an index and an error will be reported back.

When choosing a name for an index:

- The name should be unique (don't confuse this with the UNIQUE  keyword used in the index specifications) in the scope
  of the table. It means it should not be already taken by some other index. Always check which indexes already exist
  in a table before creating a new one.
- Generally, the name must adhere to the MySQL requirements for identifiers as explained
  in:

  - https://dev.mysql.com/doc/refman/8.4/en/identifier-qualifiers.html

- Keep in mind that names of identifiers (including indexes) in Qserv are case-insensitive. This is not the general
  requirement in MySQL, where the case sensitivity of identifiers is configurable one way or another. It's because
  of a design decision the original Qserv developers made to configure the underlying MySQL machinery.
- To make things simple, restrain from using special characters (all but the underscore one ``_``).
- The length of the name should not exceed 64 characters:

  - https://dev.mysql.com/doc/refman/8.4/en/identifier-length.html

Error reporting
^^^^^^^^^^^^^^^^

All services mentioned in the document are the Replication system's services, and they adhere to the same error reporting
schema. The schema is explained in the document:

- :ref:`ingest-general-error-reporting` (REST)

In addition, the index managemnt services may return the service-specific error conditions in the ``error_ext`` attribute:

.. code-block:

    "error_ext" : {
        "job_state" : <string>,
        "workers" : {
            <worker> : {
                <table> : {
                    "request_status" : <string>,
                    "request_error" : <string>
                },
                ...
            },
            ...
        }
    }

Where:

``job_state`` : *string*
    The completion status (state) of the corresponding C++ job classes:
    
    - ``lsst::qserv::replica::SqlGetIndexesJob``
    - ``lsst::qserv::replica::SqlCreateIndexesJob``
    - ``lsst::qserv::replica::SqlDropIndexesJob``

    The status will be serialized into a string. The explanation of the possible values could be found in
    the following C++ header:

    - https://github.com/lsst/qserv/blob/master/src/replica/jobs/Job.h

    Look for this enum type:

    .. code-block::

        enum ExtendedState {
            NONE,                ///< No extended state exists at this time.
            SUCCESS,             ///< The job has been fully implemented.
            CONFIG_ERROR,        ///< Problems with job configuration found.
            FAILED,              ///< The job has failed.
            ...
        }

    Also look for worker/table-specific errors in a JSON object explained below.

``workers`` : *object*
    The JSON object which has unique  identifiers of workers (attribute ``worker``) as the keys, where the corresponding
    value  for the worker is another JSON object which has names of worker-side tables as the next-level keys for
    descriptions of problems with managing indexes for the corresponding tables.

    .. note:
    
        Only the problematic tables (if any) would be mentioned in the report. If no problems were seen during
        the index management operations then a collection of workers and tables will be empty.

``request_status`` : *string*
    The completion status (state) of the index creation C++ request classes:

    - ``SqlGetIndexesRequest``
    - ``SqlCreateIndexesRequest``
    - ``SqlDropIndexesRequest``

    The status will be serialized into a string. More information on possible values could be found in the
    following C++ header:

    - https://github.com/lsst/qserv/blob/main/src/replica/requests/Request.h

    Look for this enum type:

    .. code-block::

        enum ExtendedState {
            NONE,           /// No extended state exists at this time
            SUCCESS,        /// The request has been fully implemented
            CLIENT_ERROR,   /// The request could not be implemented due to
                            /// an unrecoverable client-side error.
            SERVER_BAD,     /// Server reports that the request can not be implemented
                            /// due to incorrect parameters, etc.
            SERVER_ERROR,   /// The request could not be implemented due to
                            /// an unrecoverable server-side error.
            ...
        };

``request_error`` : *string*
    This string provides an expanded explanation of an error reported by the Replication system's worker (in case if the
    request failed on the worker's side and is reported to the service).

.. note::

    **Reporting partial successes or failures**

    Since the index management requests may (will) involve multiple tables, the corresponding operations may be potentially
    partially successful and partially not successful. All failures for specific indexes which couldn't be managed (created,
    queried, or deleted) would be reported as explained in the previous section. For example, that would be a case if a request
    was made to drop a known to-exist index, and if no such index existed for some final tables. There may be various reasons
    why this might happen. An explanation of the reasons is beyond a scope of this document. The best way a user should treat
    this situation is to expect that the service would do the "best effort" of removing the index. It's also allowed to run
    the index removal request multiple times. This won't make any harm. All subsequent requests will report failures for all
    final tables in the specified group of tables.

.. _admin-data-table-index-create:

Creating
--------

To create a new index, a user must submit a request to the service:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - ``/replication/sql/index``

Where the request object has the following schema:

.. code-block::

    {   "database" : <string>,
        "table" : <string>,
        "overlap" : <number>,
        "index" : <string>,
        "spec" : <string>,
        "comment" : <string>,
        "columns" : [
            {   "column" : <string>,
                "length " :<number>,
                "ascending" : <number>
            },
            ..
        ],
        "auth_key" : <string>
    }

Where:

``database`` : *string*
    The required name of the database where the table resides.

``table`` : *string*
    The required *base* name of the table where the index will be created.

``overlap`` : *number* := ``0``
    The optional *overlap* flag indicating a sub-type of the *chunk* table. The value should be one of the following:

    - ``1`` : *full overlap*
    - ``0`` : *chunk*

``index`` : *string*
    The required name of the index to be created.

``spec`` : *string*
    The required index specification. The specification should adhere to the MySQL requirements for the index creation command:

    - https://dev.mysql.com/doc/refman/8.4/en/create-index.html

    Where a value of the parameter should be one of the following keywords: ``DEFAULT``, ``UNIQUE``, ``FULLTEXT``, or ``SPATIAL``.
    All but the first one (``DEFAULT``) are mentioned in the MySQL documentation. The keywords map to the indfex
    creation command:

    .. code-block:: sql

        CREATE [UNIQUE | FULLTEXT | SPATIAL] INDEX index_name ...

    The REST service expects ``DEFAULT`` in those cases when none of the other three specifications are provided.
    Any other value or a lack of ant will be considered as an error.

``comment`` : *string* := ``""``
    The optional comment for the index. The value will be passed via the ``COMMENT`` parameter of the MySQL index
    creation command: 

    .. code-block:: sql

        CREATE ... INDEX ... COMMENT 'string' ...

``columns`` : *array*
    The required non-empty array of JSON  objects keys mentioned in the key_part  of the index creation statements: 

    .. code-block:: sql

        CREATE ... INDEX index_name ON tbl_name (key_part,...) ...

    .. note::

        The current implementation of the service doesn't support the extended-expression syntax of key_part introduced
        in MySQL version 8.

    These are the mandatory attributes of each key-specific object:

    ``column`` : *string*
        The required name of a column.
    ``length`` : *number*
        The required length of a substring used for the index. It only has a meaning for columns of
        types: ``TEXT``, ``CHAR(N)``, ``VARCHAR(N)``, ``BLOB`` , etc. And it must be always 0 for other column
        types (numeric, etc.). Otherwise, an index creation request will fail. 

    ``ascending`` : *number*
        The required sorting order of the column in the index. It translates into ``ASC`` or ``DESC`` options
        in the key definition in ``key_part``. A value of ``0`` will be interpreted as ``DESC``.
        Any other positive number will be imterpreted as to ``ASC``.

``auth_key`` : *string*
    The required zauthorization key.

Here is an example of the index creation request. Let's supposed we have the *regular* (fully replicated)
table that has the following schema:

.. code-block:: sql

    CREATE TABLE `sdss_stripe82_01`.`Science_Ccd_Exposure_NoFile` (
      `scienceCcdExposureId` BIGINT(20)   NOT NULL,
      `run`                  INT(11)      NOT NULL,
      `filterId`             TINYINT(4)   NOT NULL,
      `camcol`               TINYINT(4)   NOT NULL,
      `field`                INT(11)      NOT NULL,
      `path`                 VARCHAR(255) NOT NULL
    );

And, suppose we are going to create the ``PRIMARY`` key index based on the very first column ``scienceCcdExposureId``.
In this case the request object will look like this:

.. code-block:: json

    {   "database" : "sdss_stripe82_01",
        "table" : "Science_Ccd_Exposure_NoFile",
        "index" : "PRIMARY",
        "spec" : "UNIQUE",
        "comment" : "This is the primary key index",
        "columns" : [
            {   "column" : "scienceCcdExposureId",
                "length" : 0,
                "ascending" : 1
            }
        ],
        "auth_key" : ""
    }

The request deliberately misses the optional ``overlap`` attribute since it won't apply to the regular tables.

Here is how the request could be submitted to the service using ``curl``:

.. code-block:: bash

    curl 'http://localhost:25081/replication/sql/index' \
      -X POST -H "Content-Type: application/json" \
      -d '{"database":"sdss_stripe82_01","table":"Science_Ccd_Exposure_NoFile",
           "index":"PRIMARY","spec":"UNIQUE","comment":"This is the primary key index",
           "columns":[{"column":"scienceCcdExposureId","length":0,"ascending":1}],
           "auth_key":""}'

.. _admin-data-table-index-delete:

Deleting
--------

To delete an existing index, a user must submit a request to the service:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``DELETE``
      - ``/replication/sql/index``

Where the request object has the following schema:

.. code-block::

    {   "database" : <string>,
        "table" : <string>,
        "overlap" : <number>,
        "index" : <string>,
        "auth_key" : <string>
    }

Where:

``database`` : *string*
    The required name of the database where the table resides.

``table`` : *string*
    The required *base* name of the table where the index will be created.

``overlap`` : *number* := ``0``
    The optional *overlap* flag indicating a sub-type of the *chunk* table. The value should be one of the following:

    - ``1`` : *full overlap*
    - ``0`` : *chunk*

``index`` : *string*
    The required name of the index to be dropped.

Here is an example of the index deletion request. It's based on the same table that was mentioned in the previous section.
The request object will look like this:

.. code-block:: json

    {   "database" : "sdss_stripe82_01",
        "table" : "Science_Ccd_Exposure_NoFile",
        "index" : "PRIMARY",
        "auth_key" : ""
    }

Here is how the request could be submitted to the service using ``curl``:

.. code-block:: bash

    curl 'http://localhost:25081/replication/sql/index' \
      -X DELETE -H "Content-Type: application/json" \
      -d '{"database":"sdss_stripe82_01","table":"Science_Ccd_Exposure_NoFile",
           "index":"PRIMARY",
           "auth_key":""}'

.. _admin-data-table-index-inspect:

Inspecting
----------

To inspect the existing indexes, a user must submit a request to the service:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``GET``
      - ``/replication/sql/index/:database/:table[?overlap={0|1}]``

Where the service path has the following parameters:

``database`` : *string*
    The name of a database affected by the operation.

``table`` : *string*
    The name of the table for which the indexes are required to be collected.

The optional query parameter is

``overlap`` : *number* := ``0``
    The optional *overlap* flag indicating a sub-type of the *chunk* table. The value should be one of the following:

    - ``1`` : *full overlap*
    - ``0`` : *chunk*

In case of successful completion of the request the JSON object returned by the service will have the following schema:

.. code-block::

    {   "status": {
            "database" : <string>,
            "table" : <string>,
            "overlap" : <number>,
            "indexes" : [
                {   "name" : <string>,
                    "unique" : <number>,
                    "type" : <string>,
                    "comment" : <string>,
                    "status" : <string>,
                    "num_replicas_total" : <number>,
                    "num_replicas" : <number>, 
                    "columns" : [
                        {   "name" : <string>,
                            "seq" : <number>,
                            "sub_part" : <number>,
                            "collation" : <string>
                        },
                        ...
                    ]
                },
                ...
            ]
        }
    }

Where:

``name`` : *string*
    The name of the index (the key).

``unique`` : *number*
    The numeric flag indicates of the index's keys are unique, where a value of ``0`` means they're unique. Any other 
    value would mean the opposite.

``type`` : *string*
    The type of index, such as ``BTREE``, ``SPATIAL``, ``PRIMARY``, ``FULLTEXT``, etc.

``comment`` : *string*
    An optional explanation for the index passed to the index creation statement:

    .. code-block:: sql

        CREATE ... INDEX ... COMMENT 'string' ...

``status`` : *string*
    The status of the index. This parameter considers the aggregate status of the index across all replicas of the table.
    Possible values here are:

    - ``COMPLETE`` : the same index (same type, columns) is present in all replicas of the table (or its chunks in the case
      of the partitioned table)
    - ``INCOMPLETE`` : the same index is present in a subset of replicas of the table, where all indexes have the same
      definition.
    - ``INCONSISTENT`` : instances of the index that have the same name have different definitions in some replicas

    .. warning::

        The result object reported by the service will not provide any further details on the last status ``INCONSISTENT``
        apart from indicating the inconsistency. It will be up to the data administrator to investigate which replicas have
        unexpected index definitions.

``num_replicas_total`` : *number*
    The total number of replicas that exist for the table. This is the target number of replicas where the index is expected
    to be present.

``num_replicas`` : *number*
    The number of replicas where the index was found to be present. If this number is not the same as the one reported
    in the attribute
    ``num_replicas_total`` then the index will be ``INCOMPLETE``.

``columns`` : *array*
    The collection of columns that were included in the index definition. Each entry of the collection has:

    ``name`` : *string*
        The name of the column
    ``seq`` : *number*
        The 1-based position of the column in the index.
    ``sub_part`` : *number*
        The index prefix. That is, the number of indexed characters if the column is only partly indexed 0 if
        the entire column is indexed.
    ``collation`` : *string*
        How the column is sorted in the index. This can have values ``ASC``, ``DESC``, or ``NOT_SORTED``.

The following request will report indexes from the fully-replicated table ``ivoa.ObsCore``:

.. code-block:: bash

    curl http://localhost:25081/replication/sql/index/ivoa/ObsCore -X GET

The (truncated and formatted for readability) result of an operation performed in a Qserv deployment with 6-workers may
look like this:

.. code-block:: json

    {   "status" : {
        "database" : "ivoa"
        "indexes" : [
        {   "name" : "idx_dataproduct_subtype",
            "type" : "BTREE",
            "unique" : 0,
            "status" : "COMPLETE",
            "num_replicas" : 6,
            "num_replicas_total" : 6,
            "comment" : "The regular index on the column dataproduct_subtype",
            "columns" : [
                {   "collation" : "ASC",
                    "name" : "dataproduct_subtype",
                    "seq" : 1,
                    "sub_part" : 0
                }
            ]
        },
        {   "name" : "idx_s_region_bounds",
            "type" : "SPATIAL",
            "unique" : 0,
            "status" : "COMPLETE",
            "num_replicas" : 6,
            "num_replicas_total" : 6,
            "comment" : "The spatial index on the geometric region s_region_bounds",
            "columns" : [
                {   "collation" : "ASC",
                    "name" : "s_region_bounds",
                    "seq" : 1,
                    "sub_part" : 32
                }
            ]
        },
        {   "name" : "idx_lsst_tract_patch",
            "type" : "BTREE",
            "unique" : 0,
            "status" : "COMPLETE",
            "num_replicas" : 6,
            "num_replicas_total" : 6,
            "comment" : "The composite index on the columns lsst_tract and lsst_patch",
            "columns" : [
                {   "collation" : "ASC",
                    "name" : "lsst_tract",
                    "seq" : 1,
                    "sub_part" : 0
                },
                {   "collation" : "ASC",
                    "name" : "lsst_patch",
                    "seq" : 2,
                    "sub_part" : 0
                }
            ]
        },
    }

.. note::

    - The second index ``idx_s_region_bounds`` is spatial. It's based on the binary column of which only
      the first 32 bytes are indexed.

    - The third index ``idx_lsst_tract_patch`` is defined over two columns.
