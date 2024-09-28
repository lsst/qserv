####################
Worker Ingest Server
####################

..  note::

    Services explained in this section are provided directly by Qserv workers, not by the main REST server of
    the Master Replication Controller. Each Qserv worker runs a dedicated Ingest Server that is reponsible for
    ingesting and managing catalogs located on the coresponding worker. Ingest workflows interact directly with
    workers using this API. The DNS names (IP addresses) of the corresponding hosts and the relevant port numbers
    of the worker services are returned by requests sent to the Master Replication Controller's services:

    - :ref:`table-location-chunks`
    - :ref:`table-location-chunks-one`
    - :ref:`table-location-regular`

.. _ingest-worker-contrib-by-ref:

Ingesting contributions by reference
====================================

Contribution ingest requests can be initiated using one of these techniques:

- *synchronous processing*: a client will get blocked for the duration of the request before it finishes (or failed)
  to be executed. After that, the client would have to analyze the final state of the request from a response sent
  by the service.
- *asynchronous processing*: a client will not be blocked. Once the request's parameters were successfully parsed and
  analyzed (and accepted), the request will be queued for asynchronous processing. After that, the service will send back
  a response with the unique identifier and the current status of the request. The workflow will have to use the identifier
  to track the progression of the request. For requests that failed the validation stage information on reasons for
  the failure will be returned.

The following REST services implement these protocols:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - | ``/ingest/file``
        | ``/ingest/file-async``

The services expect a caller to provide a description of a request in the request's body in a form of a JSON object.
The object should adhere to the following schema:

.. code-block::

   {
      "transaction_id" : <number>,
      "table"          : <string>,
      "chunk"          : <number>,
      "overlap"        : <number>,

      "url" : <string>,

      "fields_terminated_by" : <string>,
      "fields_enclosed_by"   : <string>,
      "fields_escaped_by"    : <string>,
      "lines_terminated_by"  : <string>,
      "charset_name"         : <string>,

      "http_method"  : <string>,
      "http_data"    : <string>,
      "http_headers" : <string>,

      "max_num_warnings" : <number>,
      "num_retries"      : <number>
   }


Where:

``transaction_id`` : *number*
  The required unique identifier of a transaction that is required to be in the ``STARTED`` state
  at a time when a request is received by a service. More information on the transaction management and transaction
  states can be found in: :ref:`ingest-trans-management`.

``table`` : *string*
  The required *base* name of a table receiving the contribution. See :ref:`ingest-general-base-table-names` for more details
  on the meaning of the attriute *base* in a context of this API.

``chunk`` : *number*
  The required chunk number for the partitioned tables.

  **Note**: ignored for the *regular* tables.

``overlap`` : *number*
  The required numeric flag indicates a kind of partitioned table (``0``  if this is not the *overlap*
  table or any other number of this is the *overlap* table).

  **Note**: ignored for the *regular* tables.

``url`` : *string*
  The required location of a file to be ingested. The current implementation supports the following schemes:

  - ``file:///<path>``:  A file on a filesystem that is mounted locally on the corresponding worker. Note that
    the file path must be absolute. See details on this subject at: https://en.wikipedia.org/wiki/File_URI_scheme.

  - ``http://<resource>``, ``https://<resource>``: A file on a web server. For either of these schemes, additional
    attributes (if needed) for pulling a file over the specified protocol could be provided in optional parameters:
    ``http_method``, ``http_data`` and ``http_headers``. Descriptions of the parameters are provided below in this table.

    **Note**: Workflows may also configure the behavior of the ``libcurl`` library by settting the library-specific
    options at a level of a database. See instructions at: :ref:`ingest-config`.

``fields_terminated_by`` : *string* = ``\t``
  The optional parameter of the desired CSV dialect: a character that separates fields in a row.
  The dafault value assumes the tab character.

``fields_enclosed_by`` : *string* = ``""``
  The optional parameter of the desired CSV dialect: a character that encloses fields in a row.
  The default value assumes no quotes around fields.

``fields_escaped_by`` : *string* = ``\\``
  The optional parameter of the desired CSV dialect: a character that escapes special characters in a field.
  The default value assumes two backslash characters.

``lines_terminated_by`` : *string* = ``\n``
  The optional parameter of the desired CSV dialect: a character that separates rows.
  The default value assumes the newline character.

``charset_name`` : *string* = ``latin1``
  The optional parameters specify the desired character set name to be assumed when ingesting
  the contribution. The default value may be also affected by the ingest services configuration.
  See the following document for more details:

  - **TODO**: A reference to the page "Specifying character sets when ingesting tables into Qserv"

``http_method`` : *string* = ``GET``
  The optional method that is used to pull a file over the HTTP protocol.

``http_data`` : *string* = ``""``
  The optional data that is sent in the body of the HTTP request.
  The default value assumes no data are sent.

``http_headers`` : *string* = ``""``
  The optional list of headers that are sent in the HTTP request.
  The default value assumes no headers are sent. A value of the parameters is a string that contains
  zero, one or many headers definition string separated by a colon, where each such definition should look like:

  .. code-block::

      <header-name>: <header-value>

``max_num_warnings`` : *number* = ``64``
  The optional limit for the number of notes, warnings, and errors to be retained by MySQL/MariaDB when
  loading the contribution into the destination table.

  **Note**: The default number of the limit is determined by a configuration of the ingest services.
  The default value of the parameter in MySQL/MariaDB is ``64``. The upper limit for the parameter is ``65535``.
  Some workflows may choose to set a specific value for the limit when debugging data of the contributions.

  **TODO**: "Configuration Guide for the Replication/Ingest System" (a reference to the page)

``num_retries`` : *number* : **optional**
  The optional number of automated retries of failed contribution attempts in cases when
  such retries are still possible. The limit can be further limited by the ingest service to a value that will
  not exceed the "hard" limit set in the worker configuration parameter (``worker``, ``ingest-max-retries``).
  Setting a value of the parameter to ``0``  will explicitly disable automatic retries regardless of the server's
  configuration settings.

  **Notes**:

  - The parameter is ignored by the *synchronous* service.
  - The default number of retries set in the Inget Server's configuration parameter
    (``worker``, ``ingest-num-retries``) will be assumed.

  **TODO**: "Configuration Guide for the Replication/Ingest System" (a reference to the page)

The service will return the following JSON object:

.. code-block::

    {   "contrib": {
            ...
        }
    }

See the :ref:`ingest-worker-contrib-descriptor` section of document for the details on the schema of the response object.

.. _ingest-worker-contrib-by-val:

Ingesting contributions by value
================================

Contributions can be also ingested by sending data directly to the worker server in the request body. There are two sevices
in this category. Both techniques are *synchronous* and the client will be blocked until the request is processed:

- sending data as a JSON object
- sending data as a ``CSV`` file in the ``multipart/form-data`` formatted body

Each technique has its own pros and cons.

.. _ingest-worker-contrib-by-val-json:

JSON object
-----------

The following service allows a workflow to push both data and a description of the contribution request as a JSON object:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - ``/ingest/data``

The service expects a caller to provide a description of a request in the request's body in a form of a JSON object:

.. code-block::

   {
      "transaction_id" :   <number>,
      "table" :            <string>,
      "chunk" :            <number>,
      "overlap" :          <number>,
      "charset_name" :     <string>,
      "binary_encoding" :  <string>,
      "max_num_warnings" : <number>,
      "rows" : [
          <object>,
          ...
          <object>
      ]
   }

Where:

``transaction_id`` : *number*
  The required unique identifier of a transaction that has to be in the ``STARTED`` state
  at a time when a request is received by a service. More information on the transaction management and transaction
  states can be found in: :ref:`ingest-trans-management`.

``table`` : *string*
  The required *base* name of a table receiving the contribution. See :ref:`ingest-general-base-table-names` for more details
  on the meaning of the attriute *base* in a context of this API.

``chunk`` : *number*
  The required chunk number for the partitioned tables.

  **Note**: ignored for the *regular* tables.

``overlap`` : *number*
  The required numeric flag indicates a kind of partitioned table (``0``  if this is not the *overlap*
  table or any other number of this is the *overlap* table).

  **Note**: ignored for the *regular* tables.

``charset_name`` : *string* = ``latin1``
  The optional value depends on Qserv settings.

``binary_encoding`` : *string* = ``hex``
  See :ref:`ingest-general-binary-encoding` for more details.

``max_num_warnings`` : *number* = ``64``
  The optional limit for the number of notes, warnings, and errors to be retained by MySQL/MariaDB when
  loading the contribution into the destination table.

  **Note**: The default number of the limit is determined by a configuration of the ingest services.
  The default value of the parameter in MySQL/MariaDB is ``64``. The upper limit for the parameter is ``65535``.
  Some workflows may choose to set a specific value for the limit when debugging data of the contributions.

  **TODO**: "Configuration Guide for the Replication/Ingest System" (a reference to the page)

``rows`` : *array*
  The required collection of the data rows to be ingested. Each element of the array represents a complete row,
  where elements of the row represent values of the corresponding columns in the table schema:

  .. code-block::

    [[<string>, ... <string>],
      ...
      [<string>, ... <string>]
    ]

  **Note**:

  - The number of elements in each row must be the same as the number of columns in the table schema.
  - Positions of the elements within rows should match the positions of the corresponding columns in the table schema.
  - see the :ref:`ingest-db-table-management-register-table` section for the details on the table schema.

The service will return the following JSON object:

.. code-block::

    {   "contrib": {
            ...
        }
    }

See the :ref:`ingest-worker-contrib-descriptor` section of document for the details on the schema of the response object.

.. _ingest-worker-contrib-by-val-csv:

CSV file
--------

.. warning::

  The service expectes a certain order of the parts in the body of the request. The description of the contribution
  request should be posted first, and the file payload should be posted second. There must be exactly one file payload
  in the body of the request. No file or many files will be treated as an error and reported as such in the response.

The following service allows a workflow to push both data (a ``CSV`` file) and a description of the contribution request in
the ``multipart/form-data`` formatted body:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - ``/ingest/csv``

The body should contain two parts posted in the following order:

- a collection of the key-value pairs that represent the description of the contribution request
- a single file payload that contains the data to be ingested

Where the keys which describe the contribution request are presented below:

``transaction_id`` : *number*
  The required unique identifier of a transaction that has to be in the ``STARTED`` state
  at a time when a request is received by a service. More information on the transaction management and transaction
  states can be found in: :ref:`ingest-trans-management`.

``table`` : *string*
  The required *base* name of a table receiving the contribution. See :ref:`ingest-general-base-table-names` for more details
  on the meaning of the attriute *base* in a context of this API.

``chunk`` : *number*
  The required chunk number for the partitioned tables.

  **Note**: ignored for the *regular* tables.

``overlap`` : *number*
  The required numeric flag indicates a kind of partitioned table (``0``  if this is not the *overlap*
  table or any other number of this is the *overlap* table).

  **Note**: ignored for the *regular* tables.

``charset_name`` : *string* = ``latin1``
  The optional parameter that depends on Qserv settings.

``fields_terminated_by`` : *string* = ``\t``
  The optional parameter of the desired CSV dialect: a character that separates fields in a row.

``fields_enclosed_by`` : *string* = ``""``
  The optional parameter of the desired CSV dialect: a character that encloses fields in a row.
  The default value assumes no quotes around fields.

``fields_escaped_by`` : *string* = ``\\``
  The optional parameter of the desired CSV dialect: a character that escapes special characters in a field.
  The default value assumes two backslash characters.

``lines_terminated_by`` : *string* = ``\n``
  The default value of the optional parameter assumes the newline character.

``max_num_warnings`` : *number* = ``64``
  The optional limit for the number of notes, warnings, and errors to be retained by MySQL/MariaDB when
  loading the contribution into the destination table.

  **Note**: The default number of the limit is determined by a configuration of the ingest services.
  The default value of the parameter in MySQL/MariaDB is ``64``. The upper limit for the parameter is ``65535``.
  Some workflows may choose to set a specific value for the limit when debugging data of the contributions.

  **TODO**: "Configuration Guide for the Replication/Ingest System" (a reference to the page)

The service will return the following JSON object:

.. code-block::

    {   "contrib": {
            ...
        }
    }

See the :ref:`ingest-worker-contrib-descriptor` section of document for the details on the schema of the response object.

Here is an example of how the request could be formatted using ``curl``:

.. code-block:: bash

    curl http://localhost:25004/ingest/csv \
         -X POST -H 'Content-Type: multipart/form-data' \
         -F 'transaction_id=1630'\
         -F 'table=gaia_source' \
         -F 'chunk=675' \
         -F 'overlap=0' \
         -F 'charset_name=latin1' \
         -F 'fields_terminated_by=,' \
         -F 'max_num_warnings=64' \
         -F 'file=@/path/to/file.csv'

**Note**: the request header ``-H 'Content-Type: multipart/form-data'`` is not required when using ``curl``. The header
is added here for the sake of clarity.

Another example is based on Python's ``requests`` library and the ``requests_toolbelt`` package:

.. code-block:: python

    import requests
    from requests_toolbelt.multipart.encoder import MultipartEncoder
    import urllib3

    # Supress the warning about the self-signed certificate
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    url = "http://localhost:25004/ingest/csv"
    encoder = MultipartEncoder(
        fields = {
          "transaction_id": (None, "1630"),
          "table": (None, "gaia_source"),
          "chunk": (None, "675"),
          "overlap": (None, "0"),
          "charset_name": (None, "latin1"),
          "fields_terminated_by": (None, ","),
          "max_num_warnings": (None, "64"),
          "file": ("file.csv", open("/path/to/file.csv", "rb"), "text/csv")
        }
    )
    req = requests.post(url, data=encoder,
                        headers={"Content-Type": encoder.content_type},
                        verify=False)
    req.raise_for_status()
    res = req.json()
    if res["success"] == 0:
        error = res["error"]
        raise RuntimeError(f"Failed to ingest data ito the table: {error}")

**Notes**:

- The ``MultipartEncoder`` class from the ``requests_toolbelt`` package is used for both formatting
  the request and sending it in the *streaming* mode. The mode is essential for avoiding memory problem 
  on the client side when pushing large contributons into the service. W/o the streaming mode the client
  will try to load the whole file into memory before sending it to the server.
- The parameter ``verify=False`` is used to ignore SSL certificate verification. Also note using ``urllib3``
  to suppress the certificate-related warning. Do not use this in production code.

.. _ingest-worker-contrib-get:

Status of requests
==================

There are two services in this group. The first one allows retrieving the status info of a single request by
its identifier. The second service is meant for querying statuses of all asynchronous requests of the given transaction.

.. _ingest-worker-contrib-get-one:

One request
-----------

The service allows obtaining a status of the *asynchronous* contribution requests:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``GET``
      - ``/ingest/file-async/:id``

The services expect a caller to provide a unique identifier ``id`` of the contribution request in the resource path.
Values of the identifiers are returned by services that accept the contribution requests.

If the identifier is valid and the service could locate the desired record for the contributon it will return the following
JSON object:

.. code-block::

    {   "contrib": {
            ...
        }
    }

See the :ref:`ingest-worker-contrib-descriptor` section of document for the details on the schema of the response object.

.. _ingest-worker-contrib-get-trans:

All requests of a transaction
-----------------------------

The service allows obtaining a status of the *asynchronous* contribution requests submitted in a scope of
a given transaction:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``GET``
      - ``/ingest/file-async/trans/:id``

The services expect a caller to provide a unique identifier ``id`` of the transaction in the resource path.
Values of the transaction identifiers are returned by services that manage transactions. See :ref:`ingest-trans-management`
for more details.

If the identifier is valid and the service could locate the relevant contributons it will will return the following
JSON array:

.. code-block::

  {   "contribs": [
          <object>,
          ..
          <object>
      ]
  }

See the :ref:`ingest-worker-contrib-descriptor` section of document for the details on the schema of the contribution objects.


.. _ingest-worker-contrib-retry:

Retrying failed contributions
=============================

.. note::

  - Services, presented in this section complement those that were meant for the initial submission of the contribution
    requests posted by *by-reference*, regardless of the interface used (*synchronous* or *asynchronous*) as documented
    in :ref:`ingest-worker-contrib-by-ref`. The eligibility requirememnts for the requests are further explained in:

    - **TODO**: "Automatic retries for the failed contribution requests" (a reference to the page)

  - Unlike the *automatic* retries that may be configured in the original contribution request,
    the *explicit* retrying is a responsibility of the ingest workflow.
  - The number of the explicit retries is not a subject for limits set for the automatic retries.
    It's up to the workflow to decide how many such retries should be attempted. The workflow should coordinate
    the retries with the transaction managemnet to avoid the situation when the same request is retried
    while the transaction is already in a state that doesn't allow the contribution to be processed.
  - The workflow should avoid making multiple parallel requests to retry the same contribution request.
    The workflow should be always waiting for the response of the previous retry before making another one.
  - The *automatic* retries are disabled by the Ingest service while processing the explicitly made retries.

Both *synchronous* and *asynchronous* services are provided for the retrying of the failed contributions:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``PUT``
      - | ``/ingest/file/:id``
        | ``/ingest/file-async/:id``

The services expect a caller to provide a unique identifier ``id`` of the contribution request to be retried.

The services will locate and evaluate the specified request to see if it's eligible for retrying. And if it is then
the request will be processed in accordance with the logic of the called service. Specifically:

- If the *synchronous* interface was invoked then the request will be attempted right away and only once (no further
  automatic replies).
- If the alternative *asynchronous* interface was invoked then the request will be placed at the very end of the input
  queue. It will be processed in its turn when picked by one of the processing threads of the ingest server's pool.
  Likewise, in the case of *synchronous* processing, only one attempt to process the request will be made.

The service will return the following JSON object:

.. code-block::

    {   "contrib": {
            ...
        }
    }

See the :ref:`ingest-worker-contrib-descriptor` section of document for the details on the schema of the response object.

.. _ingest-worker-contrib-cancel:

Cancelling async requests
=========================

.. warning::

  In general, request cancellation is a non-deterministic operation that is prone to *race conditions*.
  An outcome of the cancellation request depends on the current state of a request within the worker service:

  - If the request is still in the wait queue then the cancellation will be successful.
  - If the request is already being processed by the ingest machinery then the cancellation will be successful
    only if the request is still in the data *reading* state.
  - Requests that are already in the *loading* state are presently not cancellable since MySQL table loading
    is a non-interruptible operation.
  - If the request is already in the *finished* or any form of the *failed* state then obviously no cancellation
    will happen.

  The workflow should be always inspect the state of the requests after the cancellation attempts
  to make sure that the requests were indeed cancelled.

There are two services in this group. The first one allows canceling a single request by its identifier.
The second service is meant for cancelling all asyncgronous requests of the given transaction.

.. _ingest-worker-contrib-cancel-one:

One request
-----------

The service allows cancelling an *asynchronous* contribution request:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``DELETE``
      - ``/ingest/file-async/:id``

The services expect a caller to provide a unique identifier ``id`` of the contribution request in the resource path.
Values of the identifiers are returned by services that accept the contribution requests.

If the identifier is valid and the service could locate the desired record for the contributon it will make an attempt
to cancel it. The service will return the following JSON object:

.. code-block::

  {   "contrib": {
          ...
      }
  }

See the :ref:`ingest-worker-contrib-descriptor` section of document for the details on the schema of the response object.

.. _ingest-worker-contrib-cancel-trans:

All requests of a transaction
-----------------------------

The service allows cancelling all *asynchronous* contribution requests submitted in a scope of
a given transaction:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``DELETE``
      - ``/ingest/file-async/trans/:id``

The services expect a caller to provide a unique identifier ``id`` of the corresponding transaction in the resource path.
Values of the transaction identifiers are returned by services that manage transactions. See :ref:`ingest-trans-management`
for more details.

If the identifier is valid and the service could locate the relevant contributons it will make an attempt
to cancel them. The service will return the following JSON array:

.. code-block::

  {   "contribs": [
          <object>,
          ..
          <object>
      ]
  }

See the :ref:`ingest-worker-contrib-descriptor` section of document for the details on the schema of the contribution objects.


.. _ingest-worker-contrib-descriptor:

Contribution descriptor
=======================

The following object illustrates the schema and a sample payload of the contribution descriptor:

.. code-block:: json
    
    {
      "id" :               2651966,
      "async" :            1,
      "database" :         "gaia_edr3",
      "table" :            "gaia_source",
      "worker" :           "db13",
      "chunk" :            675,
      "overlap" :          0,
      "transaction_id" :   1630,

      "status" :           "FINISHED",
      "create_time" :      1726026383616,
      "start_time" :       1726026383619,
      "read_time" :        1726026396161,
      "load_time" :        1726026412474,

      "url" :              "http://sdfqserv001:18080/gaia_edr3/gaia_source/files/chunk_675.txt",
      "http_method" :      "GET",
      "http_headers" :     [],
      "http_data" :        "",
      "tmp_file" :         "/qserv/data/ingest/gaia_edr3-gaia_source-675-1630-7570-6e63-d0b6-6934.csv",

      "max_num_warnings" : 64,
      "max_retries" :      4,

      "charset_name" :     "latin1",
      "dialect_input" : {
          "fields_enclosed_by" :   "\\0",
          "lines_terminated_by" :  "\\n",
          "fields_escaped_by" :    "\\\\",
          "fields_terminated_by" : ","
      },

      "num_bytes" :          793031392,
      "num_rows" :           776103,
      "num_rows_loaded" :    776103,

      "http_error" :         0,
      "error" :              "",
      "system_error" :       0,
      "retry_allowed" :      0,

      "num_warnings" :       0,
      "warnings" :           [],
      "num_failed_retries" : 0,
      "failed_retries" :     []
    }


The most important (for the ingest workflows) attributes of the contribution object are:

``status`` : *string*
  The status of the contribution requests. The possible values are:

  - ``IN_PROGRESS``: The transient state of a request before it's ``FINISHED`` or failed.
  - ``CREATE_FAILED``: The request was received and rejected right away (incorrect parameters, etc.).
  - ``START_FAILED``: The request couldn't start after being pulled from a queue due to changed conditions.
  - ``READ_FAILED``: Reading/preprocessing of the input file failed.
  - ``LOAD_FAILED``: Loading into MySQL failed.
  - ``CANCELLED``: The request was explicitly cancelled by the ingest workflow (ASYNC contributions only).
  - ``FINISHED``: The request succeeded,

``create_time`` : *number*
  The timestamp when the contribution request was received (milliseconds since the UNIX *Epoch*).
  A value of the attribute is guaranteed to be not ``0``.

``start_time`` : *number*
  The timestamp when the contribution request was started (milliseconds since the UNIX *Epoch*).
  A value of the attribute is ``0`` before the processing starts.

``read_time`` : *number*
  The timestamp when the Ingest service finished reading/preprocessing the input file (milliseconds since the UNIX *Epoch*).
  A value of the attribute is ``0`` before the reading starts.

``load_time`` : *number*
  The timestamp when the Ingest service finished loading the contribution into the MySQL table (milliseconds since the UNIX *Epoch*).
  A value of the attribute is ``0`` before the loading starts.

``url`` : *string*
  The URL of the input file that was used to create the contribution. Depending on a source of the data,
  the URL *scheme* could be:

  - ``http``, ``https``: The file was pulled from a remote Web server.
  - ``file``: The file was read from a filesystem that is mounted locally on the corresponding worker. The URL is a full path to the file.
  - ``data-json``:  The file was sent as a JSON object in the request body. The URL is a placeholder.
  - ``data-csv``: The file was sent as a CSV file in the ``multipart/form-data`` formatted body. The URL is a placeholder.

  **Note** that there is no guarantee that the URL will be valid after the contribution is processed.

``max_num_warnings`` : *number*
  The maximum number of the MySQL warnings to be captured after loading the contribution into the MySQL table.
  The number may correspond to a value that was explicitly set by workflow when making a contribution request.
  Otheriwse the default number configured at the system is assumed.

``max_retries`` : *number*
  The maximum number of retries allowed for the contribution. The number may correspond to a value that was explicitly set by workflow
  when making a contribution request. Otheriwse the default number configured at the system is assumed.

``num_bytes`` : *number*
  The total number of bytes in the input file. The value is set by the service after it finishes reading
  the file and before it starts loading the data into the MySQL table.

``num_rows`` : *number*
  The total number of rows parsed by the ingest service in the input file.

``num_rows_loaded`` : *number*
  The total number of rows loaded into the MySQL table. Normally the number of rows loaded should be equal to the number of rows parsed.
  If the numbers differ it means that some rows were rejected during the ingest process. The workflow should be always monitoring any
  mismatches in these values and trigger alerts.

``http_error`` : *number*
  The HTTP error code captured by the service when pulling data of the contribution from a remote Web server.
  This applies to the corresponidng URL *schemes*. The value is set only if the error was detected.

``error`` : *string*
  The error message captured by the service during the contribution processing. The value is set only if the error was detected.

``system_error`` : *number*
  The system error code captured by the service during the contribution processing. The value is set only if the error was detected.  

``retry_allowed`` : *number*
  The flag that tells if the contribution is allowed to be retried. The value is set by the service when the contribution
  processing was failed. The value is set to ``1`` if the contribution is allowed to be retried, and to ``0`` otherwise.

  **Important**: The workflow should be always analyze a value of this attribute to decide if the contribution should be retried.
  If the retry is not possible then the workflow should give up on the corresponding transaction, abort the one, and start
  another transaction to ingest all contributions attempted in a scope of the aborted one.

``num_warnings`` : *number*
  The total number of MySQL warnings captured after loading the contribution into the MySQL table.

  **Note**: The number is reported correctly regardless if the array in the attribute ``warnings``
  is empty or not.

``warnings`` : *array*
  The array of MySQL warnings captured after loading the contribution into the MySQL table. Each entry is
  an object that represents a warning/error/note. See the table in :ref:`ingest-worker-contrib-descriptor-warnings`
  for the details on the schema of the object.

  **Notes**: The maximum number of warnings captured is limited by the value of the attribute ``max_num_warnings``.

``num_failed_retries`` : *number*
  The total number of retries that failed during the contribution processing.

  **Note**: The number is reported correctly regardless if the array in the attribute ``failed_retries``
  is empty or not.

``failed_retries`` : *array*
  The array of failed retries captured during the contribution processing. Each such retry is represented
  by JSON object that has a schema explained in :ref:`ingest-worker-contrib-descriptor-retries`.

  **Note**: The maximum number of failed retries captured is limited by the value of the attribute ``max_retries``.

.. _ingest-worker-contrib-descriptor-warnings:

MySQL warnings
--------------

Warnings are captured into the JSON array of ``warnings``:

.. code-block::

  "warnings" : [
      <object>,
      ...
      <object>
  ]

The format of the object is presented below:

``level`` : *string*
  The severity of the warning reported by MySQL. Allowed values:

  - ``Note``
  - ``Warning``
  - ``Error``

``code`` : *number*
  The numeric error code indicates a reason for the observed problem.

``message`` : *string*
  The human-readable explanation for the problem.

Here is an example of what could be found in the array:

.. code-block:: json

    "warnings" : [
      {"code" : 1406, "level" : "Warning", "message" : "Data too long for column 's_region_scisql' at row 3670"},
      {"code" : 1261, "level" : "Warning", "message" : "Row 3670 doesn't contain data for all columns"},
      {"code" : 1406, "level" : "Warning", "message" : "Data too long for column 's_region_scisql' at row 3913"},
      {"code" : 1261, "level" : "Warning", "message" : "Row 3913 doesn't contain data for all columns"},
      {"code" : 1406, "level" : "Warning", "message" : "Data too long for column 's_region_scisql' at row 3918"},
      {"code" : 1261, "level" : "Warning", "message" : "Row 3918 doesn't contain data for all columns"}
    ],

More details on the values can be found in the MySQL documentation:

- https://dev.mysql.com/doc/refman/8.4/en/show-warnings.html

.. _ingest-worker-contrib-descriptor-retries:

Retries
-------

Retries are captured into the JSON array of ``failed_retries``:

.. code-block::

  "failed_retries" : [
      <object>,
      ...
      <object>
  ]

The format of the object is presented below:

.. code-block:: 

    {   "start_time" :   <number>,
        "read_time" :    <number>,
        "tmp_file" :     <string>,
        "num_bytes" :    <number>,
        "num_rows" :     <number>,
        "http_error"  :  <number>,
        "system_error" : <number>,
        "error" :        <string>
    }

Where:

``start_time`` : *number*
  The timestamp when the retry attempt was started (milliseconds since the UNIX *Epoch*).
  A value of the attribute is ``0`` before the processing starts.

``read_time`` : *number*
  The timestamp when the Ingest service finished reading/preprocessing the input file (milliseconds since the UNIX *Epoch*).
  A value of the attribute is ``0`` before the reading starts.

``num_bytes`` : *number*
  The total number of bytes in the input file. The value is set by the service after it finishes reading
  the file and before it starts loading the data into the MySQL table.

``num_rows`` : *number*
  The total number of rows parsed by the ingest service in the input file.

``http_error`` : *number*
  The HTTP error code captured by the service when pulling data of the contribution from a remote Web server.
  This applies to the corresponidng URL *schemes*. The value is set only if the error was detected.

``system_error`` : *number*
  The system error code captured by the service during the contribution processing. The value is set only if the error was detected.  

``error`` : *string*
  The error message captured by the service during the contribution processing. The value is set only if the error was detected.
