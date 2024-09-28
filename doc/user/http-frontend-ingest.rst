.. _http-frontend-ingest:

The User Table Ingest Interface
===============================

The frontend provides a simple interface for ingesting and managing user-defined tables in Qserv. Key features and limitations include:

- Supports creation and ingestion of simple (non-partitioned) tables only.
- Table sizes are constrained by the frontend's available memory:

  - Practical limit is a few GB.
  - Limit may be lower for multiple simultaneous table ingest requests.

- Only synchronous interface is currently supported.

  - **Asynchronous interface is under evaluation.**

- Schema definition and table data are sent to the frontend in a single JSON object.
- Clients can request optional indexes on ingested tables during the ingest process.
- User-defined tables are automatically created by the request processor within the user databases.

  - The service enforces a specific naming convention for user databases to avoid conflicts with production data products in Qserv.
  - The naming convention is detailed in the relevant section of the document.

- Users can delete tables or their corresponding databases.

  - **Currently, the service does not support authentication/authorization or user quota control.**

- No backups are provided for tables ingested using this mechanism.
- Binary data types are supported.

  - **Binary data must be converted to an appropriate format for inclusion in the JSON request object.**
  - See: :ref:`ingest-general-binary-encoding` (REST)

- The API follows the same :ref:`http-frontend-general-error-reporting` scheme as adopted for :ref:`http-frontend-query`.

Ingesting tables
----------------

The following REST service implements the synchronous interface for ingesting a table into Qserv:

.. list-table::
   :widths: 10 90
   :header-rows: 0

   * - ``POST``
     - ``/ingest/data``

A client must include a JSON object in the request body to specify the operation to be performed. The object follows this schema:

.. code-block::

    {   "database" :        <string>,
        "table" :           <string>,
        "binary_encoding" : <string>,
        "timeout" :         <number>,
        "schema" :          <array>,
        "indexes" :         <array>,
        "rows" :            <array>
    }

Where:

``database`` : *string*
  The required name of a user database. The names of the databases must start with the following prefix:

  .. code-block::

    user_<username>

  The rest of the name should include the name of a user or a role. For example:

  .. code-block::

    user_gapon

``table`` : *string*
  The required name of a table. Table names may not start with the following prefix:

  .. code-block::

    qserv_

  This prefix is reserved for naming internal tables that Qserv places into user databases.

``binary_encoding`` : *string* = ``hex``
  The optional binary encoding of the binary data in the table. For further details see:

  - :ref:`ingest-general-binary-encoding` (REST)

``schema`` : *array*
  The required schema definition. Each element of the array defines a column:

  .. code-block::

    {   "name" : <string>,
        "type" : <string>
    }

  Where:

  ``name``
    The name of a column
  ``type``
    A valid MySQL type

  **Note**: The service preserves the column order when creating a table.

``indexes`` : *array* = ``[]``
  The optional indexes will be created after ingesting the table.

  More information on the index specification requirements can be found in the dedicated section of the document:
  :ref:`http-frontend-ingest-indexes`.

``rows`` : *array*
  The required collection of the data rows to be ingested. Each element of the array represents a complete row,
  where elements of the row correspond to the values of the respective columns:

  .. code-block::

    [ [ <string>, ... <string> ],
                  ...
      [ <string>, ... <string> ]
    ]

  The number of elements in each row must match the number of columns defined in the table schema. In case of a mismatch,
  the service will complain and refuse to execute the request.

  The order and types of elements in each row should correspond to the order and types of the corresponding columns in
  the table schema. The service will attempt to convert the data to the appropriate types. If the conversion fails, the
  service will refuse to execute the request.

``timeout`` : *number* = ``300``
  The optional timeout (in seconds) that limits the duration of the internal operations initiated by the service.
  In practical terms, this means that the total wait time for the completion of a request will not exceed the specified timeout.

  **Note**: The number specified as a value of the attribute can not be ``0``.

A call to this service will block the client application for the time required to create
a database (if it does not already exist), create a table, process and ingest the data, and perform
additional steps (such as creating indexes). The request will fail if it exceeds the specified (or implied) timeout.

Here is an example of the simple table creation specification:

.. code-block:: json

    {   "version" :  38,
        "database" : "user_gapon",
        "table" :    "employee",
        "schema" : [
            { "name" : "id",     "type" : "INT" },
            { "name" : "val",    "type" : "VARCHAR(32)" },
            { "name" : "active", "type" : "BOOL" }
        ],
        "rows" : [
            [ "123", "Igor Gaponenko", 1 ],
            [ "2",   "John Smith",     0 ]
        ]
    }

The description could be pushed to the service using:

.. code-block:: bash

    curl -k 'https://localhost:4041/ingest/data' -X POST \
         -H 'Content-Type: application/json' \
         -d'{"version":38,"database":"user_gapon",..}'

If the request succeeds then the following table will be created:

.. code-block:: sql

    CREATE TABLE `user_gapon`.`employee` (

        `qserv_trans_id` int(11)     NOT NULL,
        `id`             int(11)     DEFAULT NULL,
        `val`            varchar(32) DEFAULT NULL,
        `active`         tinyint(1)  DEFAULT NULL,

    ) ENGINE=MyISAM DEFAULT CHARSET=latin1;

.. _http-frontend-ingest-indexes:

Creating indexes
^^^^^^^^^^^^^^^^

.. note::

  For detailed information on the schema of the index specifications, please refer to the following document:

  - :ref:`admin-data-table-index` (ADMIN)

Indexes, if needed, must be specified in the ``indexes`` attribute of the table creation request. This attribute is a JSON array,
where each entry represents an index specification:

.. code-block::

    {   "version" : 38,
        "indexes" : [
          { "index" :   <string>,
            "spec" :    <string>,
            "comment" : <string>,
            "columns" : [
              { "column" : <string>, "length" : <number>, "ascending" : <number> },
              ...
            ]
          },
          ...
        ],
        ...
    }

A few notes:

- It is possible to create one or many indexes in such specifications.
- Index names (attribute ``index``) must be unique for the table.
- An index may involve one or many columns as specified in the array ```columns```.
- Index comment (attribute ``comment``) is optional.
- Other attributes are mandatory.

Here is an example of the earlier presented simple table creation specification which also
includes an index specification:

.. code-block:: json

    {   "version" :  38,
        "database" : "user_gapon",
        "table" :    "employee",
        "schema" : [
            { "name" : "id",     "type" : "INT" },
            { "name" : "val",    "type" : "VARCHAR(32)" },
            { "name" : "active", "type" : "BOOL" }
        ],
        "rows" : [
            [ "123", "Igor Gaponenko", 1 ],
            [ "2",   "John Smith",     0 ]
        ],
        "indexes" : [
            {   "index" :   "idx_id",
                "spec" :    "UNIQUE",
                "comment" : "This is the primary key index",
                "columns" : [
                    { "column" : "id", "length" : 0, "ascending" : 1 }
                ]
            }
        ]
    }

This specification will result in creating the following table:

.. code-block:: sql

    CREATE TABLE `user_gapon`.`employee` (

        `qserv_trans_id` int(11)     NOT NULL,
        `id`             int(11)     DEFAULT NULL,
        `val`            varchar(32) DEFAULT NULL,
        `active`         tinyint(1)  DEFAULT NULL,

        UNIQUE KEY `idx_id` (`id`) COMMENT 'This is the primary key index'

    ) ENGINE=MyISAM DEFAULT CHARSET=latin1;

Deleting tables
---------------

Existing tables can be deleted with the following service:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``DELETE``
      - ``/ingest/table/:database/:table``

Where:

``database`` : *string*
  The required name of the user database containing the table to be deleted.

  **Note**: Database names must start with the following prefix:

  .. code-block::

    user_<username>_

``table`` : *string*
  The required name of a table to be deleted.

For example:

.. code-block:: bash

    curl -k 'https://localhost:4041/ingest/table/user_gapon/employees' -X DELETE \
         -H 'Content-Type: application/json' \
         -d'{"version":38}'

A few notes:

- Option ``-k`` is used to ignore the SSL certificate verification.
- The sender's content header (option ``-H``) is required by the service.
- The request's body can be empty. However, it needs to be a valid JSON object, such as ``{}``. 
- The present implementation of the service doesn't provide user authentication/authorization
  services that prevent the deletion of someone else's tables.

Deleting user databases
-----------------------

Existing databases (including all tables within such databases) can be deleted with the following service:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``DELETE``
      - ``/ingest/database/:database``

Where:

``database`` : *string*
  The required name of a database to be deleted.

  **Note**: Database names must start with the following prefix:

  .. code-block::

    user_<username>_

For example:

.. code-block:: bash

    curl -k 'https://localhost:4041/ingest/database/user_gapon' -X DELETE \
         -H 'Content-Type: application/json' \
         -d'{"version":38}'

A few notes:

- The ``-k`` option is used to ignore SSL certificate verification.
- The ``-H`` option is required to specify the content type as JSON.
- The request body can be empty but must be a valid JSON object, such as ``{}``.
- The current implementation does not provide authentication/authorization to prevent
  the deletion of other users' databases.

Possible extensions of the table ingest service
-----------------------------------------------

.. warning::

  None of the improvements mentioned below have been implemented. This section is primarily
  to outline potential future enhancements.

Potential enhancements for the table ingest service include:

- Adding services to manage (create or drop) indexes on existing tables.
- Introducing a service for asynchronous table ingests.
- Implementing a service to track the status and progress of asynchronous requests.
- Providing a service to cancel queued asynchronous requests.
- Supporting table ingests using the ``multipart/form-data`` format, where data is sent as
  a ``CSV``-formatted file.
