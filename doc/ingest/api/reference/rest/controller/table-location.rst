
.. _table-location:

Table location services
=======================

.. _table-location-regular:

Locate regular tables
---------------------

.. warning::
    This service was incorrectly designed by requiring the name of a database (attribute ``database``) be passed
    in the ``GET`` request's body. The same problem exists for the alternative method accepting a transaction identifier
    (attribute ``transaction_id``). This is not a standard practice. The ``GET`` requests are not supposed to have the body.
    The body may be stripped by some HTTP clients or proxies. Both problems will be fixed in the next releases of Qserv
    by moving the parameters into the query part of the URL.

The service returns connection parameters of the Worker Data Ingest Services which are available for ingesting
the regular (fully replicated) tables:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``GET``
      - ``/ingest/regular``

Where the request object passed in a request's body has the following schema, in which a client would have to provide the name of a database:

.. code-block::

    {   "database" : <string>
    }

The database should not be published at a time when the request was being called. Otherwise the service will return an error.

The service also supports an alternative method accepting a transaction identifier (transactions are always associated with
the corresponding databases):

.. code-block::

    {   "transaction_id" : <number>
    }

If the transaction identifier was provided then the transaction is required to be in the ``STARTED`` state at the time of a request.
See the section :ref:`ingest-trans-management` for more details on transactions.

In case of successful completion the service returns the following object:

.. code-block::

    {   "locations" : [
            {   "worker" :         <string>,
                "host" :           <string>,
                "host_name" :      <string>,
                "port" :           <number>,
                "http_host" :      <string>,
                "http_host_name" : <string>,
                "http_port" :      <number>
            },
            ...
        ]
    }

Where, each object in the array represents a particular worker. See an explanation of the attributes in:

- :ref:`table-location-connect-params`

**Note**: If the service will returns an empty array then Qserv is either not properly configured,
or it's not ready to ingest the tables.

.. _table-location-chunks:

Allocate/locate chunks of the partitioned tables
------------------------------------------------

The current implementation of the system offers two services for allocating (or determining locations of existing) chunks:

- :ref:`table-location-chunks-one`
- :ref:`table-location-chunks-many`

Both techniques are explained in the current section. The choice of a particular technique depends on the requirements
of a workflow. However, the second service is recommended as it's more efficient in allocating large quanities of chunks.

Also note, that once a chunk is assigned (allocated) to a particular worker node all subsequent requests for the chunk are guaranteed
to return the same name of a worker as a location of the chunk. Making multiple requests for the same chunk is safe. Chunk allocation
requests require a valid super-transaction in the ``STARTED`` state. See the section :ref:`ingest-trans-management` for more details on transactions.

.. _table-location-chunks-one:

Single chunk allocation
~~~~~~~~~~~~~~~~~~~~~~~

The following service is meant to be used for a single chunk allocation/location:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - ``/ingest/chunk``

Where the request object has the following schema, in which a client would have to provide the name of a database:

.. code-block::

    {   "database" : <string>,
        "chunk" :    <number>
    }

The service also supports an alternative method accepting a transaction identifier (transactions are always associated with the corresponding databases):

.. code-block::

    {   "transaction_id" : <number>,
        "chunk" :          <number>
    }

If a request succeeded, the System would respond with the following JSON object:

.. code-block::

    {   "locations" : [
            {   "worker" :         <string>,
                "host" :           <string>,
                "host_name" :      <string>,
                "port" :           <number>,
                "http_host" :      <string>,
                "http_host_name" : <string>,
                "http_port" :      <number>
            },
            ...
        ]
    }

Where, the object represents a worker where the Ingest system requests the workflow to forward the chunk contributions.
See an explanation of the attributes in:

- :ref:`table-location-connect-params`

.. _table-location-chunks-many:

Multiple chunks allocation
~~~~~~~~~~~~~~~~~~~~~~~~~~

For allocating multiple chunks one would have to use the following service:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - ``/ingest/chunks``

Where the request object has the following schema, in which a client would have to provide the name of a database:

.. code-block::

    {   "database" : <string>,
        "chunks" :   [<number>, <number>, ... <number>]
    }

Like the above-explained case of the single chunk allocation service, this one also supports an alternative method accepting
a transaction identifier (transactions are always associated with the corresponding databases):

.. code-block::

    {   "transaction_id" : <number>,
        "chunks" :        [<number>, <number>, ... <number>]
    }

**Note** the difference in the object schema - unlike the single-chunk allocator, this one expects an array of chunk numbers.

The resulting object  has the following schema:

.. code-block::

    {   "locations" : [
            {   "chunk" :          <number>,
                "worker" :         <string>,
                "host" :           <string>,
                "host_name" :      <string>,
                "port" :           <number>,
                "http_host" :      <string>,
                "http_host_name" : <string>,
                "http_port" :      <number>
            },
            ...
        ]
    }

Where, each object in the array represents a particular worker. See an explanation of the attributes in:

- :ref:`table-location-connect-params`

.. _table-location-connect-params:

Connection parameters of the workers
-------------------------------------

.. warning::
     In the current implementation of the Ingest system, values of the hostname attributes ``host_name`` and ``http_host_name`` are captured
     by the worker services themselves. The names may not be in the FQDN format. Therefore this information has to be used with caution and
     only in those contexts where the reported names could be reliably mapped to the external FQDN or IP addresses of the corresponding hosts
     (or Kubernetes *pods*).

Attributes of the returned object are:

``chunk`` : *number*
  The unique identifier of the chunk in Qserv.

  **Note**: This attribute is reported in the chunk location/allocation services:

  - :ref:`table-location-chunks`

``worker`` : *string*
  The unique identifier of the worker in Qserv.

  **Note**: The worker's identifier is not the same as the worker's host name.

``host`` : *string*
  The IP address of the worker's Ingest service that supports the proprietary binary protocol.

``host_name`` : *string*
  The DNS name of the worker's Ingest service that supports the proprietary binary protocol.

``port`` : *number*
  The port number of the worker's Ingest service that supports the proprietary binary protocol. This service requires 
  the content of an input file be sent directly to the service client. The Replication/Ingest system provides
  an application :ref:`ingest-tools-qserv-replica-file` that relies on this protocol.

``http_host`` : *string*
  The IP address of the worker's Ingest service that supports the HTTP protocol.      

``http_host_name`` : *string*
  The DNS name of the worker's Ingest service that supports the HTTP protocol.

``http_port`` : *number*
  The port number of the worker's Ingest service that supports the HTTP protocol. The REST server that's placed
  in front of the service allows ingesting a single file from a variety of external sources, such as the locally
  mounted (at the worker's host) filesystem, or a remote object store. It's also possible to push the content of a file
  in the request body ether as teh JSON object or as a binary stream (``multipart/form-data``).
