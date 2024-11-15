Director Index Management
=========================

.. _ingest-director-index-build:

(Re-)building the Index
-----------------------

.. note:: API version notes:

    - As of version ``21``, the service can no longer be used to (re-)build indexes of all *director*
      tables of a catalog. It's now required to provide the name of the affected table in the parameter ``director_table``.

    - As of version ``22``, the service no longer support the option ``allow_for_published``. Any attempts to specify
      the option will result in a warning reported by the service back to a client. The service will ignore the option.

.. warning::
    Be advised that the amount of time needed to build an index of a large-scale catalog may be quite large.
    The current implementation of the secondary index is based on MySQL's InnoDB table engine. The insert
    time into this B-Tree table has logarithmic performance. It may take many hours to build catalogs of
    billions of objects. In some earlier tests, the build time was 20 hours for a catalog of 20 billion objects.


The service of the **Master Replication Controller** builds or rebuilds (if needed) the *director* (used to be known as
the *secondary*) index table of a database. The target table must be *published* at the time of this operation.

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - ``/ingest/index/secondary``

The request object has the following schema:

.. code-block::

    {   "database" :       <string>,
        "director_table" : <string>,
        "rebuild" :        <number>,
        "local" :          <number>
    }

Where:

``database`` : *string*
  The required name of a database affected by the operation.

``director_table`` : *string*
  The required name of the *director* table for which the index is required to be (re-)built.

``rebuild`` : *number* = ``0``
  The optional flag that allows recreating an existing index. If the value is set to ``0`` the service
  will refuse to proceed with the request if the index already exists. Any other value would tell the service
  to drop (if exists) the index table before re-creating and re-populating it with entries.

``local`` : *number* = ``0``
  The optional flag that tells the service how to ingest data into the index table, where:

  - ``0``: Index contributions are required to be directly placed by the Replication/Ingest System at a location
    that is directly accessible by the MySQL server hosting the index table. This could be either some local folder
    of a host where the service is being run or a folder located at a network filesystem mounted on the host.
    Once a file is in place, it would be ingested into the destination table using this protocol:

    .. code-block:: sql

      LOAD DATA INFILE ...

    **Note**: Be aware that this option may not be always possible (or cause complications) in Kubernetes-based
    deployments of Qserv.

  - ``1`` (or any other numeric value): Index contributions would be ingested into the table using this protocol:

    .. code-block:: sql

      LOAD DATA LOCAL INFILE ...

    **Note**: Files would be first copied by MySQL at some temporary folder owned by the MySQL service before being
    ingested into the table. This option has the following caveats:

    - The protocol must be enabled in the MySQL server configuration by setting a system variable: ``local_infile=1``.
    - The temporary folder of the MySQL server is required to have sufficient space to temporarily accommodate index
      contribution files before they'd be loaded into the table. In the worst-case scenario, there should be enough
      space to accommodate all contributions of a given catalog. One could make a reasonable estimate for the latter
      by knowing the total number of rows in the director table of the catalog, the size of the primary
      key (typically the ``objectId`` column) of the table, as well as types of the ``chunk`` and ``subChunk``
      columns (which are usually the 32-bit integer numbers in Qserv).
    - This ingest option would also affect (lower) the overall performance of the operation due to additional
      data transfers required for copying file contributions from a location managed by the **Master Replication Controller**
      to the temporary folder of the MySQL server.

If the operation succeeded, the service will respond with the default JSON object which will not carry any additional
attributes on top of what's mandated in :ref:`ingest-general-error-reporting`.

In case of errors encountered during an actual attempt to build the index was made, the object may have a non-trivial
value of the ``error_ext``. The object wil carry specific reasons for the failures. The schema of the object
is presented below: 

.. code-block::

    "error_ext" : {
        <table> : {
            <worker> : {
                <chunk>  : <error>,
                ...
            },
        },
        ...
    }

Where:

``table`` : *string*
  The placeholder for the name of the director table.

``worker`` : *string*
  The placeholder for the name of the worker service that failed to build the index.

``chunk`` : *number*
  The placeholder for the chunk number.

``error`` : *string*
  The placeholder for the error message.

Here is an example of how this object might look like:

.. code-block::

    "error_ext" : {
        "object" : {
            "qserv-db01" : {
                122 : "Failed to connect to the worker service",
                3456 : "error: Table 'tes96__Object' already exists, errno: 1050",
            },
            "qserv-db23" : {
                123 : "Failed to connect to the worker service"
            }
        }
    }
