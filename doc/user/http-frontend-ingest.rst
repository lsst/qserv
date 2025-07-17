.. _http-frontend-ingest:

The User Table Ingest Interface
===============================

The frontend provides a simple interface for ingesting and managing user-defined tables in Qserv. Key features and limitations include:

- Supports creation and ingestion of simple (non-partitioned) tables only.
- Two options for ingesting table data are supported:

  - :ref:`http-frontend-ingest-json`: Both data and metadata are encapsulated into a single JSON object sent in the request body.
  - :ref:`http-frontend-ingest-multipart`: The data is sent as a ``CSV``-formatted file, and metadata
    is encapsulated in separate JSON objects. All this information is packaged into the body of a request.

  Each option has its own pros and cons. The choice depends on the specific requirements of the client application.
  The ``application/json`` option is more flexible and allows for more control over the ingest process.
  However, it may be less efficient for large tables compared to the ``multipart/form-data`` option.
  Additionally, with the ``application/json`` option, table sizes are constrained by the frontend's available memory, where:

  - Practical limit is a few GB.
  - Limit may be lower for multiple simultaneous table ingest requests.

- Schema and index definitions are sent to the frontend in the JSON format.
- Clients can request optional indexes on ingested tables during the ingest process.
- Only *synchronous* interface is currently supported.
- User-defined tables are automatically created by the request processor within the user databases.

  - The service enforces a specific naming convention for user databases to avoid conflicts with production data products in Qserv.
  - The naming convention is detailed in the following section of the document:
  
    - :ref:`http-frontend-ingest-names`

- Users can delete tables or their corresponding databases.

  - **Currently, the service does not support authentication/authorization or user quota control.**

- No backups are provided for tables ingested using this mechanism.
- Binary data types are supported in both ingest options. However, there are differences in how binary data is handled:

  - When the table data is sent in a ``multipart/form-data``-formatted request body, the binary data needs to be encoded
    as explained in the MySQL documentation:

    - https://dev.mysql.com/doc/refman/8.4/en/load-data.html

  - When the table data is sent in an ``application/json``-formatted request body, the binary data must be converted to
    an appropriate format for inclusion in the JSON request object. This is explained in:

    - See: :ref:`ingest-general-binary-encoding` (REST)

- The API follows the same :ref:`http-frontend-general-error-reporting` scheme as adopted for :ref:`http-frontend-query`.

.. _http-frontend-ingest-names:

Naming conventions
------------------

Most services described in this document require user database and table names. These names must follow the conventions outlined below:

``database`` : *string*
  The name of a user database. The names of the databases must start with the following prefix:

  .. code-block::

    user_<username>

  The rest of the name should include the name of a user or a role. For example:

  .. code-block::

    user_gapon

``table`` : *string*
  The name of a table. Table names may not start with the following prefix:

  .. code-block::

    qserv_

  This prefix is reserved for naming internal tables that Qserv places into user databases.

Depending on the version of the Qserv AOI, there are additional restrictions on the names of databases and tables.
Before version number **46** of the API, database and table names could only container alphanumeric characters and underscores.
This restriction was imposed to avoid issues with the MySQL server. This restriction was relaxed in version **46** of the API
to allow the following special characters:

  -       (white space)
  - ``-`` (hyphen)
  - ``.`` (dot)
  - ``@`` (at sign)
  - ``+`` (plus sign)
  - ``#`` (hash)
  - ``$`` (dollar sign)
  - ``%`` (percent sign)
  - ``&`` (ampersand)
  - ``!`` (exclamation mark)
  - ``=`` (equal sign)
  - ``?`` (question mark)
  - ``~`` (tilde)
  - ``^`` (caret)
  - ``|`` (vertical bar)
  - ``:`` (colon)
  - ``;`` (semicolon)
  - ``'`` (single quote)
  - ``"`` (double quote)
  - ``<`` (less than)
  - ``>`` (greater than)
  - ``(`` (left parenthesis)
  - ``)`` (right parenthesis)
  - ``{`` (left brace)
  - ``}`` (right brace)
  - ``[`` (left bracket)
  - ``]`` (right bracket)
  - ``/`` (forward slash)
  - ``\`` (backslash)

A failure to follow these conventions will result in an error response from the service.

Ingesting tables
----------------

.. _http-frontend-ingest-json:

application/json
^^^^^^^^^^^^^^^^

The following REST service implements the synchronous interface for ingesting a table into Qserv:

.. list-table::
   :widths: 10 90
   :header-rows: 0

   * - ``POST``
     - ``/ingest/data``

The service requires a ``application/json``-formatted request body. The body must include a single JSON object
to specify the operation to be performed. The object follows this schema:

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
  The required name of a user database.

``table`` : *string*
  The required name of a table.

``binary_encoding`` : *string* = ``hex``
  The optional binary encoding of the binary data in the table. For further details see:

  - :ref:`ingest-general-binary-encoding` (REST)

``schema`` : *array*
  The required schema definition. The schema must be a JSON array, where each entry represents a column specification.
  More information on the schema specification requirements can be found in the dedicated section of the document:

  - :ref:`http-frontend-ingest-schema-spec`

``indexes`` : *array* = ``[]``
  The optional indexes will be created after ingesting the table. See the example below for a scenario when indexes are needed.
  More information on the index specification requirements can be found in the dedicated section of the document:

  - :ref:`http-frontend-ingest-index-spec`

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

    {   "version" :  39,
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
         -d'{"version":39,"database":"user_gapon",..}'

If the request succeeds then the following table will be created:

.. code-block:: sql

    CREATE TABLE `user_gapon`.`employee` (

        `qserv_trans_id` int(11)     NOT NULL,
        `id`             int(11)     DEFAULT NULL,
        `val`            varchar(32) DEFAULT NULL,
        `active`         tinyint(1)  DEFAULT NULL,

    ) ENGINE=MyISAM DEFAULT CHARSET=latin1;

Here is an example of the earlier presented simple table creation specification which also
includes an index specification:

.. code-block:: json

    {   "version" :  39,
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

.. _http-frontend-ingest-multipart:

multipart/form-data
^^^^^^^^^^^^^^^^^^^

..  warning::

    - The order of parts in the request body is important. The service expects the table payload to be sent last.
      Otherwise, the service will fail to process the request.
    - The ``multipart/form-data`` header is not required when using ``curl`` to send the request. The service will
      automatically recognize the format of the request body.

The following REST service implements the synchronous interface for ingesting a table into Qserv:

.. list-table::
   :widths: 10 90
   :header-rows: 0

   * - ``POST``
     - ``/ingest/csv``

The service requires a ``multipart/form-data``-formatted request body. The body must include the following parts
and files:

``database`` : *part*
  The required name of a user database.

``table`` : *part*
  The required name of a table.

``fields_terminated_by`` : *part* = ``\t``
  The optional parameter of the desired CSV dialect: a character that separates fields in a row.
  The dafault value assumes the tab character.

``fields_enclosed_by`` : *part* = ``""``
  The optional parameter of the desired CSV dialect: a character that encloses fields in a row.
  The default value assumes no quotes around fields.

``fields_escaped_by`` : *part* = ``\\``
  The optional parameter of the desired CSV dialect: a character that escapes special characters in a field.
  The default value assumes two backslash characters.

``lines_terminated_by`` : *part* = ``\n``
  The optional parameter of the desired CSV dialect: a character that separates rows.
  The default value assumes the newline character.

``charset_name`` : *part* = ``latin1``
  The optional parameters specify the desired character set name to be assumed when ingesting
  the contribution. The default value may be also affected by the ingest services configuration.
  See the following document for more details:

  - :ref:`ingest-api-advanced-charset` (ADVANCED)

``timeout`` : *part* = ``300``
  The optional timeout (in seconds) that limits the duration of the internal operations initiated by the service.
  In practical terms, this means that the total wait time for the completion of a request will not exceed the specified timeout.

  **Note**: The number specified as a value of the attribute can not be ``0``.

``schema`` : *file*
  The required schema definition. More information on the schema specification requirements can be found in the dedicated
  section of the document:

  - :ref:`http-frontend-ingest-schema-spec`

``indexes`` : *file*  = ``[]``
  The optional indexes will be created after ingesting the table. The indexes must be a JSON file that follows
  the index specification as described in the following section:

  - :ref:`http-frontend-ingest-index-spec`

``rows`` : *file*
  The required CSV file containing the data to be ingested.

A call to this service will block the client application for the time required to create
a database (if it does not already exist), create a table, process and ingest the data, and perform
additional steps (such as creating indexes). The request will fail if it exceeds the specified (or implied) timeout.

Here is an example of the simple table creation specification, which also includes an index specification. The table schema
is sent as a JSON file ``schema.json`` presented below:

.. code-block:: json

    [   { "name" : "id",     "type" : "INT" },
        { "name" : "val",    "type" : "VARCHAR(32)" },
        { "name" : "active", "type" : "BOOL" }
    ]

The index specification is sent as a JSON file ``indexes.json`` presented below:

.. code-block:: json

    [   {   "index" :   "idx_id",
            "spec" :    "UNIQUE",
            "comment" : "This is the primary key index",
            "columns" : [
                { "column" : "id", "length" : 0, "ascending" : 1 }
            ]
        }
    ]

And the CSV file ``employee.csv`` containing the data to be ingested:

.. code-block::

   123,Igor Gaponenko,1
   2,John Smith,0

The request could be pushed to the service using:

.. code-block:: bash

    curl -k 'https://localhost:4041/ingest/csv' \
         -F 'database=user_gapon' \
         -F 'table=employee' \
         -F 'fields_terminated_by=,' \
         -F 'timeout=300' \
         -F 'schema=@/path/to/schema.json' \
         -F 'indexes=@/path/to/indexes.json' \ 
         -F 'rows=@/path/to/employee.csv'

**Note**: The ``-k`` option is used to ignore SSL certificate verification.

Here is the complete Python code that does the same:

.. code-block:: python

    import requests
    from requests_toolbelt.multipart.encoder import MultipartEncoder
    import urllib3

    # Supress the warning about the self-signed certificate
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    database = "user_gapon"
    table = "employee"
    url = "https://localhost:4041/ingest/csv?verion=39"
    encoder = MultipartEncoder(
        fields = {
            "version": (None, "41"),
            "database" : (None, database),
            "table": (None, table),
            "fields_terminated_by": (None, ","),
            "timeout": (None, "300"),
            "schema": ("schema.json", open("/path/to/schema.json", "rb"), "application/json"),
            "indexes": ("indexes.json", open("/path/to/indexes.json", "rb"), "application/json"),
            "rows": ("employee.csv", open("/path/to/employee.csv", "rb"), "text/csv"),
        }
    )
    req = requests.post(url, data=encoder,
                        headers={"Content-Type": encoder.content_type},
                        verify=False)
    req.raise_for_status()
    res = req.json()
    if res["success"] == 0:
        error = res["error"]
        raise RuntimeError(f"Failed to create and load the table: {table} in user database: {database}, error: {error}")

**Notes**:

- The parameter ``verify=False`` is used to ignore SSL certificate verification. Note using ``urllib3`` to suppress
  the certificate-related warning. Do not use this in production code.
- The class ``MultipartEncoder`` is required for streaming large files w/o loading them into memory.
- The preferred method for passing the version number to the frontend is to include it in the query string of the request. 
  In case the version number is found both in the query string and the body of a request, the number found in the body
  will take precedence.

.. _http-frontend-ingest-schema-spec:

Schema specification
^^^^^^^^^^^^^^^^^^^^

.. note::

  The service preserves the column order when creating a table.

The table schema must be specified in the ``schema`` attribute of the table creation request. This attribute is a JSON array,
where each element of the array defines a column:

.. code-block::

  [   { "name" : <string>, "type" : <string> },
      ...
  ]

Where:

``name``
  The name of a column

``type``
  A valid MySQL type

For example:

.. code-block:: json

    [   { "name" : "id",     "type" : "INT" },
        { "name" : "val",    "type" : "VARCHAR(32)" },
        { "name" : "active", "type" : "BOOL" }
    ]

.. _http-frontend-ingest-index-spec:

Index specification
^^^^^^^^^^^^^^^^^^^

.. note::

  For detailed information on the schema of the index specifications, please refer to the following document:

  - :ref:`admin-data-table-index` (ADMIN)

Indexes, if needed, must be specified in the ``indexes`` attribute of the table creation request. This attribute is a JSON array,
where each entry represents an index specification. 

.. code-block::

    [   { "index" : <string>,
          "spec" : <string>,
          "comment" : <string>,
          "columns" : [
              { "column" : <string>, "length" : <number>, "ascending" : <number> },
              ...
          ]
        },
        ...
    ]

A few notes:

- It is possible to create one or many indexes in such specifications.
- Index names (attribute ``index``) must be unique for the table.
- An index may involve one or many columns as specified in the array ```columns```.
- Index comment (attribute ``comment``) is optional.
- Other attributes are mandatory.

For example:

.. code-block:: json

    [   {   "index" :   "idx_id",
            "spec" :    "UNIQUE",
            "comment" : "This is the primary key index",
            "columns" : [
                { "column" : "id", "length" : 0, "ascending" : 1 }
            ]
        }
    ]

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

``table`` : *string*
  The required name of a table to be deleted.

For example:

.. code-block:: bash

    curl -k 'https://localhost:4041/ingest/table/user_gapon/employees' -X DELETE \
         -H 'Content-Type: application/json' \
         -d'{"version":39}'

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

For example:

.. code-block:: bash

    curl -k 'https://localhost:4041/ingest/database/user_gapon' -X DELETE \
         -H 'Content-Type: application/json' \
         -d'{"version":39}'

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
