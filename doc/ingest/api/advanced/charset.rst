.. _ingest-api-advanced-charset:

Character sets in contributions
-------------------------------

.. note::

  This feature was added in the REST API version ``15``.

Background
^^^^^^^^^^

The current implementation of the Qserv Replication/Ingest system relies on the following SQL statement for ingesting
table data:

.. code-block:: sql

  LOAD DATA [LOCAL] INFILE ...

According to the MySQL/MariaDB documentation https://mariadb.com/kb/en/load-data-infile/#character-sets, the database
server may interpret or transform the input data (CSV) differently depending on the character set assumed by the operation.
Incorrect data transformation can result in distorted data being stored in the target table. This issue is most likely
to occur when the input data were produced from another MySQL server using the following SQL statement:

.. code-block:: sql

  SELECT INTO OUTFILE ...

The right approach to address this situation is twofold:

- Know (or configure) a specific character set when producing the data files (CSV).
- Use the same character set when loading these files into Qserv.

Before version ``15`` of the API, the Qserv Ingest System did not provide any control for the latter. The system relied on
the default character set in the database service configuration. This could lead to the following problems:

- The character set configured in this way may be random, as it may be set either by a database administrator who might not
  be aware of the problem (or the origin and parameters of the input data to be loaded into Qserv). Besides, the default
  character set may change between MySQL/MariaDB versions, or it could depend on the choice of the OS (Debian, CentOS, etc.)
  for the base Docker image.

- Different input data may be produced with different character sets. In this case, setting one globally at the database
  level (even if this could be done deterministically) wouldn't work for all inputs.

As of version ``15``:

- The implementation is reinforced to assume a specific default character set for the data loading operation. The default
  is presently set to ``latin1``.
- The ingest API is extended to allow overriding the default character set when loading contributions into Qserv.

Global configuration
^^^^^^^^^^^^^^^^^^^^

.. warning::

  Setting the server-side configuration parameters of the Ingest system is not supposed to be under the direct control of
  the workflow developers. Managing Qserv deployments and the configuration of the Ingest system is the responsibility of
  the Qserv administrators. Therefore, the workflow developers are advised to set the name of the desired character set in
  each contribution request as explained in the subsections below.

The configuration of the workers now includes the following parameter  ``(worker,ingest-charset-name)``. The parameter can
be set at the server startup via the command line option ``--worker-ingest-charset-name=<string>``. The default value is
``latin1``.

In the REST API
^^^^^^^^^^^^^^^

Overriding the default character set value is supported by all forms of contribution ingest services, whether contributions
are submitted *by reference* or *by value*, and whether they are *synchronous* or *asynchronous* requests.

The desired character set value is specified via the ``charset_name`` parameter. This parameter should be a string representing
the name of the character set. It is optional; if not provided, the Ingest system will use its default character set value.

All services that return the status of contribution requests will include the character set name used by the Ingest system when
processing the contributions. This information is reported in the JSON response object as:

.. code-block::

    {   "charset_name":<string>
    }


qserv-replica-file
^^^^^^^^^^^^^^^^^^

The command line tool :ref:`ingest-tools-qserv-replica-file` allows ingesting contributions via the proprietary binary protocol
and has an option ``--charset-name=<string>``:

.. code-block:: bash

  qserv-replica-file INGEST {FILE|FILE-LIST|FILE-LIST-TRANS} \
     ... \
     --charset-name=<string>
