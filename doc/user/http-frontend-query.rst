.. _http-frontend-query:

The Query Interface
===================

Submitting queries
------------------

.. _http-frontend-query-sync:

Synchronous interface
^^^^^^^^^^^^^^^^^^^^^

The following REST service implements the synchronous interface:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - ``/query``

To specify the query to be executed, a client must include the following JSON object in the request body:

.. code-block::
    
    {   "query" :           <string>,
        "database" :        <string>,
        "binary_encoding" : <string>
    }

Where:

``query`` : *string*
  The required query text in which the default database may be missing. In the latter case,
  a client needs to provide the name in a separate parameter.

``database`` : *string* = ``""``
  The optional name of the default database for queries where no such name was provided explicitly.

``binary_encoding`` : *string* = ``hex``
  The optional binary encoding of the binary data in the table. For further details see:

  - :ref:`ingest-general-binary-encoding` (REST)

A call to this service will block a client application until one of the following events occurs:

- The query is successfully processed, and its result is returned to the caller.
- The query fails, and the corresponding error is returned to the caller.
- The frontend becomes unavailable (due to a crash, restart, or networking problem, etc.), and the network connection is lost.

If the request is successfully completed, the service will return a result set in the JSON object explained in the section Result sets.

.. _http-frontend-query-async:

Asynchronous interface
^^^^^^^^^^^^^^^^^^^^^^

.. note:: 

   As of the version **40** of the Qserv API, it's a responsibility of the client to explicitly
   delete results of the asynchronous queries. The result of a completed query will not be deleted automatically
   after the client's attempt to pull the result. This allows the client application to manage the lifecycle of
   the query results more effectively and address possible issues on the client side should any problem arise
   (such as network failures or timeouts). The client should use a technique described in the
   :ref:`http-frontend-async-delete-resultset` section to delete the results.

   Unclaimed results of the completed asynchronous queries will be deleted by Qserv automatically after a certain
   period of time as defined in a configuration of Qserv Czar. The default value of this parameter is set to 3600 seconds.

The following REST service implements the asynchronous interface:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - ``/query-async``

A client is required to pass the JSON object in the request body to specify what query
needs to be executed. The object has the following schema:

.. code-block::

    {   "query" :    <string>,
        "database" : <string>
    }

Where:

``query`` : *string*
  The required query text in which the default database may be missing. In the latter case, a client needs to provide
  the name in a separate parameter.

``database`` : *string* = ``""``
  The optional name of the default database for queries where no such name was provided explicitly.

A call to this service will normally block a client application for a short period until
one of the following will happen:

- The query will be successfully analyzed and queued for asynchronous processing by Qserv.
  In this case, a response object with the unique query identifier will be returned to a caller.
- The query will fail and the corresponding error will be returned to the caller.
- The frontend will become unavailable (due to a crash, restart, or networking problem, etc.)
  and a network connection will be lost.

In case of the successful completion of the request, the service will return the following JSON object:

.. code-block::

    {   "queryId" : <number>
    }

The number reported in the object should be further used for making the following requests explained
in the dedicated subsections below:

- checking the status of the query to see when it's finished
- requesting a result set of the query
- or, canceling the query if needed

Checking the status of the ongoing query
-----------------------------------------

This service also allows checking the status of queries submitted via the synchronous
interface, provided the unique identifier of such query is known to the user:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``GET``
      - ``/query-async/status/:queryId``

Where:

``queryId`` : *number*
  The required unique identifier of the previously submitted query.

If the query identifier is not valid, the service will report an error in the response object.
For example, consider the following request:

.. code-block:: bash

    curl -k 'https://localhost:4041/query-async/status/123?version=39' -X GET

It might result in the following response:

.. code-block:: json

  { "success" : 0,
    "error" :   "failed to obtain info for queryId=123,
                 ex: Czar::getQueryInfo Unknown user query, err=,
                 sql=SELECT status,messageTable,resultQuery FROM QInfo WHERE queryId=123",
    "error_ext" : {},
  }

If the query identifier is valid then the following object will be returned:

.. code-block::

    {   "success" : 1,
        ...
        "status" : {
            "queryId" :         <number>,
            "query" :           <string>,
            "status" :          <string>,
            "error" :           <string>,
            "czarId" :          <number>,
            "czarType" :        <string>,
            "totalChunks" :     <number>,
            "completedChunks" : <number>,
            "collectedBytes" :  <number>,
            "collectedRows" :   <number>,
            "finalRows" :       <number>,
            "queryBeginEpoch" : <number>,
            "lastUpdateEpoch" : <number>
        }
    }

Where the ``status`` is an object that has following attributes:

``queryId`` : *number*
  The unique identifier of the previously submitted query.

``query`` : *string*
  The text of the query that was submitted for processing.

``status`` : *string*
  The current status of the query can have one of the following values:

  - ``EXECUTING`` - The query processing is still in progress.
  - ``COMPLETED`` - The query has been completed.
  - ``FAILED`` - The query failed.
  - ``FAILED_LR`` - The query failed after hitting the limit of the maximum size of the result set to be returned.
  - ``ABORTED`` - The query was aborted:

    - explicitly by a user using the query cancellation REST service explained in the document.
    - or, implicitly by Qserv if the intermediate result set of the query exceeds the large result
      limit (which is configured by the Qserv administrators).
    - or, implicitly when the query processing service was restarted due to some failure or by
      Qserv administrators.

``error`` : *string*
  The error message in case the query has failed. The value of this attribute is an empty string
  if the query has been successfully processed.

``czarId`` : *number*
    The unique identifier of the Czar node that is responsible for processing the query.

``czarType`` : *string*
  The type of the Czar node that is responsible for processing the query. In the current version of Qserv, the
  value of this attribute will be either ``proxy`` or ``http`` depending on the type of the Czar frontend.

``totalChunks`` : *number*
  The total number of so-called "chunks" (spatial shards used for splitting the large tables in Qserv
  into smaller sub-tables to be distributed across worker nodes of Qserv).

``completedChunks`` : *number*
  The number of chunks that have been processed by Qserv so far. The value of this parameter varies
  from ``0`` to the maximum number reported in the attribute ``totalChunks``.

``collectedBytes`` : *number*
  The total number of bytes collected so far by Qserv from all worker nodes in response to the query.
  This value is reported only if the query has been successfully processed so far.

  **HINT**: This value may be used to estimate the size of the result set of the query using
  the following formula: ``finalBytes = collectedBytes * (1.0 * finalRows / collectedRows)``

``collectedRows`` : *number*
  The total number of rows collected so far by Qserv from all worker nodes in response to the query.
  This value is reported only if the query has been successfully processed so far.

``finalRows`` : *number*
  The total number of rows in the final result set of the query. This value is reported only if
  the query has been successfully processed so far.

``queryBeginEpoch`` : *number*
  The 32-bit number representing the start time of the query expressed in seconds since the UNIX *Epoch*.

``lastUpdateEpoch`` : *number*
  The 32-bit number represents the last time when the query status was recorded/updated by the Qserv
  internal monitoring system. The timestamp is the number of seconds since the UNIX *Epoch*.
  The service returns a value of ``0`` if either of the following is true:

  - the query processing didn't start
  - the requst wasn't inspected by the monitoring system

Below is an example response for a query that is currently being processed:

.. code-block::

    {
        "success" :   1,
        "error" :     "",
        "error_ext" : {},
        "warning" : "",
        "status" :  {
            "queryId" :         310554,
            "query" :           "SELECT objectId,coord_ra,coord_dec FROM dp02_dc2_catalogs.Object",
            "status" :          "EXECUTING",
            "error" :           "",
            "czarId" :          7,
            "czarType" :        "http",
            "totalChunks" :     1477,
            "completedChunks" : 112,
            "collectedBytes" :  0,
            "collectedRows" :   0,
            "finalRows" :       0,
            "queryBeginEpoch" : 1708141345,
            "lastUpdateEpoch" : 1708141359
        }
    }

Users can use the status service to estimate when the query will finish. Typically, client
applications should wait until the query status is "COMPLETED" before fetching
the result set by calling the next service explained below.

The next example illustrates a response for a query that has failed after hitting the limit
of the maximum size of the result set to be returned:

.. code-block::

    {
        "success" :   1,
        "error" :     "",
        "error_ext" : {},
        "warning" :   "",
        "status" :    {
            "queryId" :         404591,
            "query" :           "SELECT * FROM dp1.Source",
            "status" :          "FAILED_LR",
            "error" :           "MERGE_ERROR 1470 (QI=404591:81; cancelling the query,
                                 queryResult table result_404591 is too large
                                 at 571829258 bytes, max allowed size is 536870912 bytes)
                                 2025-07-21T05:36:23+0000",
            "collectedBytes" :  860211304,
            "collectedRows" :   521359,
            "completedChunks" : 86,
            "czarId" :          9,
            "czarType" :        "proxy",
            "finalRows" :       521359,
            "lastUpdateEpoch" : 1753076184,
            "queryBeginEpoch" : 1753076175,
            "totalChunks" :     86
        }
    }

.. _http-frontend-query-async-result:

Requesting result sets
----------------------

The query results are retrieved by calling the following service:

..  list-table::
    :widths: 10 25 65
    :header-rows: 1

    * - method
      - service
      - query parameters
    * - ``GET``
      - ``/query-async/result/:queryId``
      - ``binary_encoding=<encoding>``

Where:

``queryId`` : *number*
  The required unique identifier of the previously submitted query.

``binary_encoding`` : *string* = ``hex``
  The optional format for encoding the binary data into JSON:

  - ``hex`` - for serializing each byte into the hexadecimal format of 2 ASCII characters per each byte of
    the binary data, where the encoded characters will be in a range of ``0 .. F``. In this case,
    the encoded value will be packaged into the JSON string.
  - ``b64`` - for serializing bytes into a string using the Base64 algorithm with
    padding (to ensure 4-byte alignment).
  - ``array`` - for serializing bytes into the JSON array of numbers in a range of ``0 … 255``.

  Here is an example of the same sequence of 4-bytes encoded into the hexadecimal format:

  .. code-block::

    "0A11FFD2"

  The array representation of the same binary sequence would look like this:

  .. code-block::

    [10,17,255,210]

Like in the case of the status inquiry request, if the query identifier is not valid then
the service will report an error in the response object. Otherwise, a JSON object explained
in the section :ref:`http-frontend-query-resultsets` will be returned.

.. _http-frontend-query-resultsets:

Result sets
^^^^^^^^^^^

Both flavors of the query submission services will return the following JSON object in case of
the successful completion of the queries (**Note**: comments ``//`` used in this example are not allowed in JSON):

.. code-block::

    {   "schema" : [

          // Col 0
          { "table" :     <string>,
            "column" :    <string>,
            "type" :      <string>,
            "is_binary" : <number>
          },

          // Col 1
          { "table" :     <string>,
            "column" :    <string>,
            "type" :      <string>,
            "is_binary" : <number>
          },

          ...

          // Col (NUM_COLUMNS-1)
          { "table" :     <string>,
            "column" :    <string>,
            "type" :      <string>,
            "is_binary" : <number>
          }
        ],

        "rows" : [

          // Col 0     Col 1         Col (NUM_COLUMNS-1)
          // --------  --------      ------------------
          [  <string>, <string>, ... <string> ],         // Result row 0
          [  <string>, <string>, ... <string> ],         // Result row 1
          ...
          [  <string>, <string>, ... <string> ]          // Result row (NUM_ROWS-1)
        ]
    }

Where:

``schema`` : *array*
  A collection of rows, in which each row is a dictionary representing a definition of
  the corresponding column of the result set:

  ``table`` : *string*
    The name of the table the column belongs to.

  ``column`` : *string*
    The name of the column.

  ``type`` : *string*
    The MySQL type of the column as in the MySQL statement:
        
    .. code-block:: sql

        CREATE TABLE ...

  ``is_binary`` : *number*
    The flag indicating if the column type represents the binary type.
    A value that is not ``0`` indicates the binary type.
    The MySQL binary types are documented in the corresponding sections of the MySQL Reference Manual:

    - `The BINARY and VARBINARY Types <https://dev.mysql.com/doc/refman/8.3/en/binary-varbinary.html>`_
    - `The BLOB and TEXT Types <https://dev.mysql.com/doc/refman/8.3/en/blob.html>`_
    - `Bit-Value Type - BIT <https://dev.mysql.com/doc/refman/8.3/en/bit-type.html>`_

  **Attention**: Binary values need to be processed according to a format specified in the optional
  attribute "binary_encoding" in:

  - Processing responses of query requests submnitted via the :ref:`http-frontend-query-sync` 
  - :ref:`http-frontend-query-async-result` of queries submitted via the asynchronous interface

``rows`` : *array*
  A collection of the result rows, where each row is a row of strings representing values at positions
  of the corresponding columns (see schema attribute above).

For example, consider the following query submission request:

.. code-block:: bash

    curl -k 'https://localhost:4041/query' -X POST-H 'Content-Type: application/json' \
         -d'{"version":39,"query":"SELECT objectId,coord_ra,coord_dec FROM dp02_dc2_catalogs.Object LIMIT 5"}'

The query could return:

.. code-block:: json

  { "schema":[
      { "column" : "objectId", "table" : "", "type" : "BIGINT", "is_binary" : 0 },
      { "column" : "coord_ra", "table" : "", "type" : "DOUBLE", "is_binary" : 0 },
      { "column" : "coord_dec","table" : "", "type" : "DOUBLE", "is_binary" : 0 }],
    "rows":[
      [ "1248640588874548987", "51.5508603", "-44.5061095" ],
      [ "1248640588874548975", "51.5626104", "-44.5061529" ],
      [ "1248640588874548976", "51.5625138", "-44.5052961" ],
      [ "1248640588874548977", "51.3780995", "-44.5072101" ],
      [ "1248640588874548978", "51.374245",  "-44.5071616" ]],
    "success" :   1,
    "warning" :   "",
    "error" :     "",
    "error_ext" : {}
  }

.. _http-frontend-async-delete-resultset:

Deleting result sets
^^^^^^^^^^^^^^^^^^^^

Result sets of the finished queries are deleted using the following service:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``DELETE``
      - ``/query-async/result/:queryId``

Where:

``queryId`` : *number*
  The required unique identifier of the completed query.

If the query identifier is not valid, the service will report an error in the response object.

Canceling queries
-----------------

.. note::

   This service can be used for terminating queries submitted via the synchronous or asynchronous
   interfaces, provided the unique identifier of such query is known to a user.

The status of the query can be checked using:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``DELETE``
      - ``/query-async/<queryId>``

Where:

``queryId`` : *number*
  The required unique identifier of the previously submitted query.

If the query identifier is not valid, the service will report an error in the response object. For example, consider the following request:

.. code-block:: bash

    curl -k 'https://localhost:4041/query-async/123?version=39' -X DELETE

It might result in the following response:

.. code-block:: json

  { "success": 0,
     "error" : "failed to obtain info for queryId=123,
                ex: Czar::getQueryInfo Unknown user query, err=,
                sql=SELECT status,messageTable,resultQuery FROM QInfo WHERE queryId=123",
  }

If the query identifier is valid then the following object will be returned:

.. code-block:: json

    {   "success" : 1 
    }

