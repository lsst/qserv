.. _http-frontend-ingest:

The User Table Ingest Interface
===============================

The frontend offers a simple interface for ingesting and managing user-defined tables in Qserv.
These are the key features and limitations of the mechanism:

- it allows creating (and ingesting into) simple (non-partitioned) tables only
- tables sizes are limited by the amount of memory available to the frontend:

  - one should consider a practical limit to be on a scale of a few GB
  - the limit could be lower for many simultaneous table ingest requests

- only the synchronous interface is supported in the current implementation

  - **The necessity of having an asynchronous interface is still being evaluated.**

- schema definition and table data as sent to the frontend in a single JSON object
- a client may also request optional indexes on the ingested tables to be created during ingest
- user-defined tables are created automatically by the request processor in a scope of
  the user databases

  - The service enforces a specific naming convention for the names of the user databases
    to avoid conflicts with the production data products that may exist in Qserv.
  - The convention is explained in the corresponding section of the document.

- users are allowed to delete tables or the corresponding databases

  - **The service presently supports no authentication/authorization, or user quota control.**

- no backups of table ingested using this mechanism are provided at this stage

- binary data types are also supported

  - **Data in the corresponding columns are required to be converted into an appropriate
    format to be packaged into the JSON object of a request.**

- the API adheres to the same :ref:`Error reporting` scheme as addopted for :ref:`The Query Interface`.

Ingesting tables
----------------

The following REST service implements the asynchronous interface for ingesting a table into Qserv:

.. list-table::
   :widths: 10 90
   :header-rows: 0

   * - ``POST``
     - ``/ingest/data``

A client is required to pass the JSON  object in the request body to specify what needs to be executed. The object has the following schema:

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
  The required name of a user database. The names of the databases are required to start with the following prefix:

  .. code-block::

    user_<username>_

  It's also assumed that the rest of the name includes the name of a user or a role. For example:

  .. code-block::

    user_gapon

``table`` : *string*
  The required name of a table. Table names may not  start with the following prefix:

  .. code-block::

    qserv_

  The prefix is reserved for naming internal tables to be placed into user databases by Qserv.

``binary_encoding`` : *string* = ``hex``
  The optional binary encoding of the binary data in the table. For further details see:

  - :ref:`ingest-general-binary-encoding`

``schema`` : *array*
  The required schema definition. Each row of the array defined a column:

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

  More info on the index specification requirements can be found in the dedicated section of the document
  :ref:`Creating indexes`.

``rows`` : *array*
  The required collection of the sata rows to be ingested. Each element of the array represents a complete row,
  where elements of the row represent values of the corresponding columns:

  .. code-block::

    [ [ <string>, ... <string> ],
                  ...
      [ <string>, ... <string> ]
    ]

  **Note**:

  - The number of elements in each row must be the same as the number of columns in the table schema.
  - Positions of the elements within rows should match the positions of the corresponding columns in the table schema.

``timeout`` : *number* = ``300``
  The optional timeout (seconds) that limits a duration of the internal operations launched by the service.
  In practical terms, it means that the total wait time for the completion of a request will not exceed the timeout.

  **Note**: The number specified as a value of the attribute can not be ``0``.

A call to this service will block a client application for the duration of time needed to create
a database (if the one didn't exist), create a table, process and ingest the data, and perform
extra steps (creating indexes, etc.). The request will fail if the request will fail to meet
the time constraint specified (or implied) in the timeout.

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

.. _Creating indexes:

Creating indexes
^^^^^^^^^^^^^^^^

.. note::

  The information found in this section, especially regarding the schema of the index
  specifications, is largely based on the following:

  - https://rubinobs.atlassian.net/wiki/spaces/DM/pages/48830694/Managing+indexes+of+MySQL+tables+at+Qserv+workers#ManagingindexesofMySQLtablesatQservworkers-Request

  Please, refer to this document for further instructions on the attribute values in the index specifications.

Indexes if needed must be specified in the attribute indexes of the table creation request. The attribute represents the JSON array, in which each entry is an index specification:

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

- it's possible to create one or many indexes in such specifications
- index names (attribute index) must be unique for the table
- an index may involve one or many columns as specified in the array columns 
- index comment (attribute comment) is optional
- other attributes are mandatory

Here is an example of the earlier presented simple table creation specification which also
has an index specification:

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
  The required name of the user database where the deleted table is residing.

  **Note**: The names of the databases are required to start with the following prefix:

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
- the present implementation of the service doesn't provide user authentication/authorization
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

  **Note**: The names of the databases are required to start with the following prefix:

  .. code-block::

    user_<username>_

For example:

.. code-block:: bash

    curl -k 'https://localhost:4041/ingest/database/user_gapon' -X DELETE \
         -H 'Content-Type: application/json' \
         -d'{"version":38}'

A few notes:

- Option ``-k`` is used to ignore the SSL certificate verification.
- The sender's content header (option ``-H``) is required by the service.
- The request's body can be empty. However, it needs to be a valid JSON object, such as ``{}``. 
- The present implementation of the service doesn't provide user authentication/authorization
  services that prevent the deletion of someone else's databases.

Possible extensions of the table ingest service
-----------------------------------------------

.. warning::

  None of the improvements mentioned hereafter has been implemented. This section is here mainly
  to state a direction of thinking.

A few ideas exists for extending the table ingest service:

- adding services for managing (creating or dropping) indexes on an existing table
- adding a service for asynchronous ingests into user tables
- adding a service for tracking the status and progress of the asynchronous requests
- adding service for cancelling queued asynchronous requests
- adding support for ingesting tables in the multipart/form-data format, where the data
  are sent as a CVS-formatted file
