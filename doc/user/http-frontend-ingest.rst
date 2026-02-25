.. _http-frontend-ingest:

The User Table Ingest Interface
===============================

.. warning::

   The service does not support any table ownership mechanism or user quota control.
   All user tables are accessible to any client with the appropriate API credentials.

The frontend provides a simple interface for ingesting and managing user-defined tables in Qserv. Key features and limitations include:

- Supports creation and ingestion of the following table types:

  - Fully-replicated (non-partitioned) tables (JSON and CSV options)
  - Director (partitioned) tables (CSV option only)
  - Dependent (partitioned) tables (CSV option only)

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
- The service automatically partitions (CSV) data of the director tables into ``chunks``  based on the default partitioning
  scheme of the corresponding Qserv deployment and distributes them across the cluster.
- User-defined tables are automatically created by the request processor within the user databases.

  - The service enforces a specific naming convention for user databases to avoid conflicts with production data products in Qserv.
  - The naming convention is detailed in the following section of the document:
  
    - :ref:`http-frontend-ingest-names`

- Users can delete tables or their corresponding databases.
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

Depending on the version of the Qserv API, there are additional restrictions on the names of databases and tables.
Before version number **46** of the API, database and table names could only contain alphanumeric characters and underscores.
This restriction is relaxed as of version **46** of the API to allow the following special characters:

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

Another restriction was introduced in version **51** of the API to ensure compatibility with MySQL limitations.
Specifically, the combined length of database and table names must not exceed a certain limit to accommodate
the naming conventions used by Qserv for its internal metadata tables. The combined length of the database
and table names must not exceed 56 characters. This limit is derived from MySQL's maximum
identifier length of 64 characters, minus the length of the suffixes used by Qserv for its internal tables.

A failure to follow these conventions will result in an error response from the service.

Ingesting tables
----------------

.. _http-frontend-ingest-json:

application/json
^^^^^^^^^^^^^^^^

..  note::

    This service can only be used for ingesting fully-replicated (non-partitioned) tables. For ingesting director (partitioned) tables,
    please use the ``multipart/form-data`` service described in the following section.

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
        "charset_name" :    <string>,
        "collation_name" :  <string>,
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

``charset_name`` : *part* = ``latin1``
  The optional parameter that affects the interpretation of the data in the CSV file when ingesting the contribution.
  The name will be used for setting the ``CHARSET`` attribute of the relevant MySQL tables created by the service.

``collation_name`` : *part* = ``latin1_swedish_ci``
  The optional parameter is used for setting the ``COLLATE`` attribute of the relevant MySQL tables created by the service.

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

    {   "version" :  55,
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
         -d'{"version":55,"database":"user_gapon",..}'

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

    {   "version" :  55,
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

The following REST service implements the synchronous interface for ingesting the fully-replicated (non-partitioned) tables and
the partitioned tables:

.. list-table::
   :widths: 10 90
   :header-rows: 0

   * - ``POST``
     - ``/ingest/csv``

The service requires a ``multipart/form-data``-formatted request body. 

.. hint::

    The ``multipart/form-data`` header is not required when using ``curl`` to send the request. The service will
    automatically recognize the format of the request body.

The service automatically partitions (CSV) data of the partitioned tables into ``chunks`` based on the default partitioning
scheme of the corresponding Qserv deployment and distributes them across the cluster.

A call to this service will block the client application for the time required to create
a database (if it does not already exist), create a table, process and ingest the data, and perform
additional steps (such as creating indexes). The request will fail if it exceeds the specified (or implied) timeout.

Parameters in the request body are sent as separate parts, and the data is sent as a file part.
The service recognizes the parts by their names.

.. warning::

    The order of parts in the request body is important. The service expects the table payload to be sent last.
    Otherwise, the service will fail to process the request.

Among all possible parameters, there are 8 special ones that are used for ingesting the partitioned tables:

- ``is_partitioned``
- ``is_director``
- ``id_col_name``
- ``longitude_col_name``
- ``latitude_col_name``
- ``ref_director_database``
- ``ref_director_table``
- ``ref_director_id_col_name``

The rest of this section lists the (required or optional) parts and files recognized by the service:

``database`` [ *repl*, *dir*, *dep* ] : *part*
  The required name of a user database.

``table`` [ *repl*, *dir*, *dep* ] : *part*
  The required name of a table.

``is_partitioned`` [ *dir*, *dep* ] : *part* = ``0``
  The optional parameter that indicates whether the table to be ingested is a partitioned table.
  The default value assumes that the table is not partitioned.

``is_director`` [ *dir*, *dep* ] : *part* = ``0``
  The optional parameter that indicates whether the table to be ingested is a director table or a dependent table;
  the default value assumes that the table is not a director table. This parameter is required if the table is partitioned
  and will be ignored otherwise.

``id_col_name`` [ *dir*, *dep* ] : *part* = ``""``
  The optional parameter that specifies the name of the column to be used as the primary key (unique row identifier) of the
  director tables or as the foreign key of dependent tables. Rules for using this parameter depend on the type of
  the table being ingested:

  - The director tables may or may not have the key:
  
    - If the column specified and if it's not empty then it must also be included in the table schema. It's recommended that the type
      of the column is ``INT`` (32-bit) or ``BIGINT`` (64-bit), or unsigned variants of those types.
      The data must also be provided for the column in the CSV file. Note that the values of the column must be unique across the entire table.
    - Another possibility for the director tables is not to specify this parameter (or to set it to an empty string). In this case, the service will
      automatically create an internal column ``qserv_id`` and populate it with unique values for each row. The type of the internal column will
      be ``BIGINT UNSIGNED``. A sequence of the values will be generated starting from ``1``.

    Regardless of which option is selected by a user, Qserv will create the unique index on the column in each chunk automatically.
    The *director* index will also be created on the column.

    Two examples of using this parameter are provided in the subsequent section of the document:

    - :ref:`http-frontend-ingest-example-user-pk`
    - :ref:`http-frontend-ingest-example-auto-pk`

  - The dependent tables must have the key. The column specified in this parameter must also be included in the table schema.
    Values of the column must be provided in the CSV file. They must match the values in the column ``ref_director_id_col_name``
    of the director table referenced in parameters ``ref_director_database`` and ``ref_director_table``.
    The service will validate the values of the column against the values in the director table and will refuse to execute
    the request if any mismatch is found.
    The service will use the column for partitioning the dependent tables to ensure the table's chunks are distributed across
    the cluster in the same way as the chunks of the referenced director table.
    The type of the column must be the same as the type of the primary key column of the referenced director table.

    Two following example illustrate the use of this parameter for dependent tables:

    - :ref:`http-frontend-ingest-example-user-dep`

``longitude_col_name`` [ *dir* ] : *part* = ``""``
  The optional parameter that specifies the name of the column to be used as the longitude (right ascension) coordinate for director tables.
  This parameter is required if the table is a director table; it will be ignored otherwise. The column specified in this parameter must
  be included in the table schema.
  It's recommended that the type of the column is ``FLOAT`` (32-bit) or ``DOUBLE`` (64-bit).
  Note that values of the longitude column are degrees, where the allowed range of (0,360] degrees is expected.

``latitude_col_name`` [ *dir* ] : *part* = ``""``
  The optional parameter that specifies the name of the column to be used as the latitude (declination) coordinate for director tables.
  This parameter is required if the table is a director table; it will be ignored otherwise. The column specified in this parameter must
  be included in the table schema.
  It's recommended that the type of the column is ``FLOAT`` (32-bit) or ``DOUBLE`` (64-bit).
  Note that values of the latitude column are degrees, where the allowed range of (-90,90) degrees is expected.

``ref_director_database`` [ *dep* ] : *part* = ``""``
  The optional parameter that specifies the name of the user database of the director table referenced by a dependent table.
  This parameter is expected to be specified if the table is a dependent table; it will be ignored otherwise.

  There is one exception for when the parameter can be omitted - if the director table was already ingested into the same
  user database.

``ref_director_table`` [ *dep* ] : *part* = ``""``
  The optional parameter that specifies the name of the director table referenced by a dependent table.
  This parameter is required if the table is a dependent table; it will be ignored otherwise.

``ref_director_id_col_name`` [ *dep* ] : *part* = ``""``
  The optional parameter that specifies the name of the column in the director table referenced by a dependent table that
  corresponds to the column specified in parameter ``id_col_name``. This parameter is required if the table is a dependent
  table; it will be ignored otherwise.

``fields_terminated_by`` [ *repl*, *dir*, *dep* ] : *part* = ``\t``
  The optional parameter of the desired CSV dialect: a character that separates fields in a row.
  The dafault value assumes the tab character.

``fields_enclosed_by`` [ *repl*, *dir*, *dep* ] : *part* = ``""``
  The optional parameter of the desired CSV dialect: a character that encloses fields in a row.
  The default value assumes no quotes around fields.

``fields_escaped_by`` [ *repl*, *dir*, *dep* ] : *part* = ``\\``
  The optional parameter of the desired CSV dialect: a character that escapes special characters in a field.
  The default value assumes two backslash characters.

``lines_terminated_by`` [ *repl*, *dir*, *dep* ] : *part* = ``\n``
  The optional parameter of the desired CSV dialect: a character that separates rows.
  The default value assumes the newline character.

``charset_name`` [ *repl*, *dir*, *dep* ] : *part* = ``latin1``
  The optional parameter that affects the interpretation of the data in the CSV file when ingesting the contribution.
  The name will be also used for setting the ``CHARSET`` attribute of the relevant MySQL tables created by the service.

``collation_name`` [ *repl*, *dir*, *dep* ] : *part* = ``latin1_swedish_ci``
  The optional parameter is used for setting the ``COLLATE`` attribute of the relevant MySQL tables created by the service.

``timeout`` [ *repl*, *dir*, *dep* ] : *part* = ``300``
  The optional timeout (in seconds) that limits the duration of the internal operations initiated by the service.
  In practical terms, this means that the total wait time for the completion of a request will not exceed the specified timeout.

  **Note**: The number specified as a value of the attribute can not be ``0``.

``schema`` : *file* [*repl*, *dir*, *dep*]
  The required schema definition. More information on the schema specification requirements can be found in the dedicated
  section of the document:

  - :ref:`http-frontend-ingest-schema-spec`

``indexes`` : *file*  = ``[]`` [*repl*, *dir*, *dep*]
  The optional indexes will be created after ingesting the table. The indexes must be a JSON file that follows
  the index specification as described in the following section:

  - :ref:`http-frontend-ingest-index-spec`

``rows`` : *file* [*repl*, *dir*, *dep*]
  The required CSV file containing the data to be ingested.

Example: ingesting fully-replicated table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
         -F 'charset_name=utf8mb4' \
         -F 'collation_name=utf8mb4_uca1400_ai_ci' \
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
    url = "https://localhost:4041/ingest/csv?version=55"
    encoder = MultipartEncoder(
        fields = {
            "version": (None, "55"),
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

.. _http-frontend-ingest-example-user-pk:

Example: ingesting the director table with user-defined primary key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is an example of the table creation specification for a director table. The table schema is sent as a JSON file ``schema.json`` presented below:

.. code-block:: json

    [   { "name" : "id",     "type" : "INT" },
        { "name" : "ra",     "type" : "DOUBLE" },
        { "name" : "dec",    "type" : "DOUBLE" },
        { "name" : "val",    "type" : "VARCHAR(32)" },
        { "name" : "active", "type" : "BOOL" }
    ]

And the CSV file ``employee.csv`` containing the data to be ingested:

.. code-block::

   1,0.99845583493592,-34.236453785757455,Igor,1
   2,23.45678901234567,0.67890123456789,John,0
   3,89.56789012345678,56.78901234567890,Charlie,1
   4,255.67890123456789,67.89012345678901,Jane,0

The request could be pushed to the service using:

.. code-block:: bash

    curl -k 'https://localhost:4041/ingest/csv' \
         -F 'database=user_gapon' \
         -F 'table=employee' \
         -F 'is_partitioned=1' \
         -F 'is_director=1' \
         -F 'id_col_name=id' \
         -F 'longitude_col_name=ra' \
         -F 'latitude_col_name=dec' \
         -F 'fields_terminated_by=,' \
         -F 'timeout=300' \
         -F 'charset_name=utf8mb4' \
         -F 'collation_name=utf8mb4_uca1400_ai_ci' \
         -F 'schema=@/path/to/schema.json' \
         -F 'indexes=@/path/to/indexes.json' \
         -F 'rows=@/path/to/employee.csv'

**Note**: The ``-k`` option is used to ignore SSL certificate verification.

To get an idea of how the Python code for this request would look like, please refer to the previous example of ingesting a fully-replicated table.
The only difference is that the parameters related to partitioning and director tables need to be added to the
``fields`` dictionary passed to the ``MultipartEncoder`` class as shown below:

.. code-block:: python

    encoder = MultipartEncoder(
        fields = {
            "version": (None, "55"),
            "database" : (None, database),
            "table": (None, table),
            "is_partitioned": (None, "1"),
            "is_director": (None, "1"),
            "id_col_name": (None, "id"),
            "longitude_col_name": (None, "ra"),
            "latitude_col_name": (None, "dec"),
            "fields_terminated_by": (None, ","),
            "timeout": (None, "300"),
            "schema": ("schema.json", open("/path/to/schema.json", "rb"), "application/json"),
            "indexes": ("indexes.json", open("/path/to/indexes.json", "rb"), "application/json"),
            "rows": ("employee.csv", open("/path/to/employee.csv", "rb"), "text/csv"),
        }
    )

.. _http-frontend-ingest-example-auto-pk:

Example: ingesting the director table with the automatically generated primary key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is an example of the table creation specification for a director table. The table schema is sent as a JSON file ``schema.json`` presented below:

.. code-block:: json

    [   { "name" : "ra",     "type" : "DOUBLE" },
        { "name" : "dec",    "type" : "DOUBLE" },
        { "name" : "val",    "type" : "VARCHAR(32)" },
        { "name" : "active", "type" : "BOOL" }
    ]

And the CSV file ``employee.csv`` containing the data to be ingested:

.. code-block::

   0.99845583493592,-34.236453785757455,Igor,1
   23.45678901234567,0.67890123456789,John,0
   89.56789012345678,56.78901234567890,Charlie,1
   255.67890123456789,67.89012345678901,Jane,0

The request could be pushed to the service using:

.. code-block:: bash

    curl -k 'https://localhost:4041/ingest/csv' \
         -F 'database=user_gapon' \
         -F 'table=employee' \
         -F 'is_partitioned=1' \
         -F 'is_director=1' \
         -F 'longitude_col_name=ra' \
         -F 'latitude_col_name=dec' \
         -F 'fields_terminated_by=,' \
         -F 'timeout=300' \
         -F 'charset_name=utf8mb4' \
         -F 'collation_name=utf8mb4_uca1400_ai_ci' \
         -F 'schema=@/path/to/schema.json' \
         -F 'indexes=@/path/to/indexes.json' \
         -F 'rows=@/path/to/employee.csv'

**Note**: The ``-k`` option is used to ignore SSL certificate verification.

To get an idea of how the Python code for this request would look like, please refer to the previous example of ingesting a fully-replicated table.
The only difference is that the parameters related to partitioning and director tables need to be added to the
``fields`` dictionary passed to the ``MultipartEncoder`` class as shown below:

.. code-block:: python

    encoder = MultipartEncoder(
        fields = {
            "version": (None, "55"),
            "database" : (None, database),
            "table": (None, table),
            "is_partitioned": (None, "1"),
            "is_director": (None, "1"),
            "longitude_col_name": (None, "ra"),
            "latitude_col_name": (None, "dec"),
            "fields_terminated_by": (None, ","),
            "timeout": (None, "300"),
            "schema": ("schema.json", open("/path/to/schema.json", "rb"), "application/json"),
            "indexes": ("indexes.json", open("/path/to/indexes.json", "rb"), "application/json"),
            "rows": ("employee.csv", open("/path/to/employee.csv", "rb"), "text/csv"),
        }
    )


.. _http-frontend-ingest-example-user-dep:

Example: ingesting the dependent table of an existing director table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here is an example of the table creation specification for a dependent table. An idea here
is to ingest the dependent table that references an existing director table ``dp1.Object``
to add some additional information (the ``flux``) about the objects in the director table.

The table schema is sent as a JSON file ``schema.json`` presented below:

.. code-block:: json

    [   { "name" : "id",   "type" : "BIGINT UNSIGNED" },
        { "name" : "flux", "type" : "DOUBLE" }
    ]

And the CSV file ``ObjectExt.csv`` containing the data to be ingested:

.. code-block::

    579574500513809549,0
    579574637952761859,1
    579575118989099291,2
    579575325147531114,3
    579577180573401666,4
    579577180573401667,5
    579577455451309231,6
    579582540692586641,7
    591817699928047627,8
    591817768647524543,9

The request could be pushed to the service using:

.. code-block:: bash

    curl -k 'https://localhost:4041/ingest/csv' \
         -F 'database=user_gapon' \
         -F 'table=ObjectExt' \
         -F 'is_partitioned=1' \
         -F 'is_director=0' \
         -F 'id_col_name=id' \
         -F 'ref_director_database=dp1' \
         -F 'ref_director_table=Object' \
         -F 'ref_director_id_col_name=objectId' \
         -F 'fields_terminated_by=,' \
         -F 'timeout=300' \
         -F 'charset_name=utf8mb4' \
         -F 'collation_name=utf8mb4_uca1400_ai_ci' \
         -F 'schema=@/path/to/schema.json' \
         -F 'indexes=@/path/to/indexes.json' \
         -F 'rows=@/path/to/ObjectExt.csv'


After ingesting the director table and the dependent table, the following query will be able to join the tables
and retrieve the data:

.. code-block:: sql

    SELECT o.objectId, o.ra, o.dec, e.flux
      FROM dp1.Object AS o
      JOIN user_gapon.ObjectExt AS e ON o.objectId = e.id
      WHERE o.objectId IN (
        579574500513809549,
        579574637952761859,
        579575118989099291,
        579575325147531114,
        579577180573401666,
        579577180573401667,
        579577455451309231,
        579582540692586641,
        591817699928047627,
        591817768647524543)

..  warning::

    A known limitation of the current Qserv implementation is that queries involving dependent tables
    may fail if the referenced director and dependent tables have different spatial coverage. This issue
    is under investigation and will be addressed in a future release. As a workaround, queries should
    explicitly constrain results to the common spatial coverage of both tables. Another workaround
    is to explicitly specify the object IDs in the query as shown in the example above.


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
         -d'{"version":55}'

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
         -d'{"version":55}'

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
