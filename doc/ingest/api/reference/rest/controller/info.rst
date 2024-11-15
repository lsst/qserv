Information services
====================

.. _ingest-info-chunks:

Chunk disposition
-----------------

.. warning::
    Do not use this service for the chunk placement decisions during catalog ingestion. The service is for
    informational purposes only.

The service of the **Master Replication Controller** return information about the chunk *replicas* in a scope of a given database:

..  list-table::
    :widths: 10 15 75
    :header-rows: 1

    * - method
      - service
      - query parameters
    * - ``GET``
      - ``/ingest/chunks``
      - ``database=<name>``

Where:
  
``name`` : *string*
  The required name of a database affected by the operation.

The resulting object has the following schema:

.. code-block::

    {
        "replica": [
            {
                "chunk" : <number>,
                "worker": <string>,
                "table" : {
                    <partitioned-table-name> : {
                        "overlap_rows" :       <number>,
                        "overlap_data_size" :  <number>,
                        "overlap_index_size" : <number>,
                        "rows" :               <number>,
                        "data_size" :          <number>,
                        "index_size" :         <number>
                    },
                    ...
                }
            },
            ...
        ]
    }

Where:

``replica`` : *array*
  A collection of chunk **replicas**, where each object representes a chunk replica. Replicas of a chunk
  are essentially the same chunk, but placed on different workers.

``chunk`` : *number*
  The chunk number.

``worker`` : *string*
  The unique identifier of a worker where the chunk replica is located.

``table`` : *object*
  The object with the information about the chunk replica in the scope of
  a particular *partitioned* table.

  **Attention**: The current implementation is incomplete. It will return ``0`` for all attributes
  of the table object.

``overlap_rows`` : *number*
  The number of rows in the chunk's overlap table.

``overlap_data_size`` : *number*
  The number of bytes in the chunk's overlap table (measured by the size of the corresponding file).

``overlap_index_size`` : *number*
  The number of bytes in the index of the chunk's overlap table (measured by the size
  of the corresponding file).

``rows`` : *number*
  The number of rows in the chunk table.

``data_size`` : *number*
  The number of bytes in the chunk table (measured by the size of the corresponding file).

``index_size`` : *number*
  The number of bytes in the index of the chunk table (measured by the size of
  the corresponding file).

.. _ingest-info-contrib-requests:

Status of the contribution request
----------------------------------

The service of the **Master Replication Controller** returns information on a contribution request:

..  list-table::
    :widths: 10 15 75
    :header-rows: 1

    * - method
      - service
      - query parameters
    * - ``GET``
      - ``/ingest/trans/contrib/:id``
      - | ``include_warnings=<0|1>``
        | ``include_retries=<0|1>``

Where:

``id`` : *number*
  The required unique identifier of the contribution request that was submitted
  to a Worker Ingest service earlier.

``include_warnings`` : *number* = ``0``
  The optional flag telling the service to include warnings into the response. Any value
  that is not ``0`` is considered as ``1``, meaning that the warnings should be included.

``include_retries`` : *number* = ``0``
  The optional flag telling the service to include retries into the response. Any value
  that is not ``0`` is considered as ``1``, meaning that the retries should be included. 

The resulting object has the following schema:

.. code-block::

    {   "contribution" : <object>
    }

Where the detailed description on the enclosed contribution object is provided in the section:

- :ref:`ingest-trans-management-descriptor-contrib-long`
