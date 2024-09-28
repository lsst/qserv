.. _ingest-data-intro:

Introduction
============

This document presents the requirements for data to be ingested into Qserv and describes the process of preparing
data for ingestion.

The main data format supported by Qserv is ``CSV``. This choice is motivated by the fact that Qserv is implemented on top of MySQL,
which natively supports the ``CSV`` format. The ``CSV`` format is also very simple and can be easily generated from other
formats like ``Parquet``. Another reason for choosing the ``CSV`` format is the performance of the ``LOAD DATA INFILE`` statement
used to load data into MySQL. The ``LOAD DATA INFILE`` statement is the fastest way to load data into MySQL and is optimized for
the ``CSV`` format. More information on this subject can be found in:

- `LOAD DATA INFILE <https://dev.mysql.com/doc/refman/8.0/en/load-data.html>`_ (MySQL)

The second option for loading data into Qserv is to send the data packaged in the ``JSON`` format. This option is not as efficient as the
``CSV`` format but is more flexible and can be used for loading data into Qserv in a more complex way. The ``JSON`` format is supported by
the following service:

- :ref:`ingest-worker-contrib-by-val-json`

This format is also used by the simple ingest API documented in :ref:`http-frontend`. The API is meant for ingesting and managing user
tables which are relatively small in size.

The data preparation process includes 4 steps:

1. Data conversion
2. Partitioning
3. Post-processing (data sanitization)
4. Staging

Some steps may be optional depending on the data format and the requirements of the ingest workflow.

Data conversion
---------------

.. note::

    This section is not complete. More details on the data conversion process will be added in the future
    version of the document.

The data conversion step is required if the input data is not in the ``CSV`` format. The ost common data format
used in the LSST project is the ``Parquet`` format.

Partitioning
------------

This topic is covered in:

- :ref:`ingest-data-partitioning` (DATA)
- :ref:`ingest-data-partitioner` (DATA)
- :ref:`ingest-data-partitioning-ref-match` (DATA)


Post-processing
---------------

Besides converting the data to the CSV format, there are other operations that may optionally be performed on
the input data. The purpose of these operations is to ensure the values of the columns are compatible with
MySQL expectations. These are a few examples of what may need to be done:

- The ``BOOL`` type in MySQL maps to the ``TINYINT`` type in MySQL. Because of that, values like ``true``
  and ``false`` are not supported by MySQL. Hence, they need to be converted to ``1`` and ``0`` respectively.

- Some data tools may produce ``-inf`` and ``+inf`` values when converting floating point numbers
  into the ``CSV`` format. Neither of these values are supported by MySQL. Assuming the column type is ``REAL``,
  they need to be converted to ``-1.7976931348623157E+308`` and ``1.7976931348623157E+308`` respectively.

- The ``NULL`` values in the ``CSV`` files need to be converted into ``\N``.

Handling the binary data
------------------------

The binary data is supported by the Qserv ingest system in two ways:

- The ``CSV`` format supports the binary data. The coresponidng fields need to be properly escaped
  as explained in:

  - `LOAD DATA INFILE <https://dev.mysql.com/doc/refman/8.0/en/load-data.html>`_ (MySQL)

- The ``JSON`` format also supports the binary data. However, the data in the correspondin columns need
  to be encodeded as explained in:

  - :ref:`ingest-general-binary-encoding` (API)


Restrictions for the variable-length column types
-------------------------------------------------

Note that variable-length column types like ``VARCHAR`` and ``TEXT`` are not allowed in the *director* tables in Qserv.
This is because *director* tables are used to materialize sub-chunks of the data. Sub-chunks are the smallest units of
data that can be processed by Qserv workers. The sub-chunk tables are implemented using the ``MEMORY`` storage engine.
Further details on this subject can be found in:

- `MEMORY Storage Engine <https://dev.mysql.com/doc/refman/8.0/en/memory-storage-engine.html>`_ (MySQL)

Staging
-------

Once the data are converted and partitioned, they need to be staged at a location from where they can be loaded into Qserv.
Depending on the selected ingest method, the data may be:

- placed locally, from where they would be pushed into Qserv via the proprietary binary protocol or the REST API.
- placed on a distributed filesystem like ``GPFS``, ``Lustre``, etc., which is mounted at the Qserv workers.
- placed on a Web server, from where they could be pulled into Qserv via the HTTP/HTTPS protocol.
- placed into an Object Store (S3 compatible), from where they could be pulled into Qserv via the S3 protocol.

Besides availability, the workflow may also require the data to be retained until the ingest process is completed.
