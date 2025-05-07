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

.. _ingest-data-conversion:

Data conversion
---------------

.. note::

    The partitioning tool ``sph-partition`` presented in section :ref:`ingest-data-partitioner-parquet` has the built-in
    capability to convert the input data from the ``Parquet`` format to the ``CSV`` format. This means that
    the data conversion step may be skipped if the input data is in the ``Parquet`` format. Note that this
    statement only applies to the ``partitioned`` table types. Data files of the ``fully-replicated`` (also known as ``regular``)
    table type are not supported by the partitioning tool. These files need to be converted using a technique presented
    in the current section.

The data conversion step is required if the input data is not in the ``CSV`` format. The most common data format
used in the LSST project is the ``Parquet`` format. The Qserv binary container provides a tool called ``sph-parq2csv``
which can be used to convert the input data from the ``Parquet`` format to the ``CSV`` format. The tool expects
the input data to be in the ``Parquet`` format and produces the output data in the ``CSV`` format. The tool expects
the following command line arguments:

.. code-block:: bash

    Usage:
        sph-parq2csv [options] --parq=<file> --config=<file> --csv=<file>

    Options:
        -h [ --help ]                 Produce this help
        -v [ --verbose ]              Produce verbose output.
        --csv-quote-fields            Double quote fields as needed in the generated
                                      CSV.
        --max-proc-mem-mb arg (=3000) Max size (MB) of RAM allocated to the process.
        --buf-size-mb arg (=16)       Buffers size (MB) for translating batches.
        --parq arg                    Input file to be translated.
        --config arg                  Input JSON file with definition of columns to
                                      be extracted.
        --csv arg                     Output file to be written.

Note that the positional arguments are required in the order shown above. All three arguments are required
by the tool.

The optional argument ``--csv-quote-fields`` is used to double quote the fields in the
output ``CSV`` file. This is useful when the input data contains special characters like commas, quotes or spaces
within strings. The option is also needed if the input file contains the binary data or timestamps. The timestamps
will be translated into the human-readable format:

.. code-block:: bash

    2024-11-09 06:12:16.184491000

A few notes on translating the following types:

- Values of the boolean columns are converted to ``1`` and ``0``.
- Infinite values of the column types ``float`` and ``float`` and are converted to ``-inf`` and ``inf``.
- Undefined values of the column types ``float`` and ``float`` are converted to ``nan``.
- The ``NULL`` values in the ``Parquet`` files are converted to ``\N`` in the ``CSV`` files.

The configuration file is a JSON file that instructs the tool which columns to extract from the input
``Parquet`` file and which columns are optional. The file has the following JSON structure:

.. code-block::

    {
        "columns": [
            "col1",
            "col2",
            "col3"
            ...
        ],
        "optional": [
            "col2",
            "col3"
        ]
    }

The ``columns`` array contains the names of the columns to be extracted from the input file. The ``optional``
array contains the names of the columns that are optional. The ``optional`` array must be a subset of the ``columns`` array.

To monitor progress of the conversion process, the tool prints the number of bytes translated at each batch. The optional
argument ``--verbose`` will enable this behavior. For example:

.. code-block:: bash

    sph-parq2csv --parq=parquet_file.parquet --config=config.json --csv=csv_file.csv --verbose

    Translating 'parquet_file.parquet' into 'csv_file.csv'
    Writing   6370040 bytes
    Writing   8706229 bytes
    Writing   8095609 bytes
    Writing   9505451 bytes
    Writing   8558826 bytes
    Writing   8077584 bytes
    Writing   9657479 bytes
    Writing   8790640 bytes
    Writing   9610568 bytes
    Writing   9738326 bytes
    Writing   9212721 bytes
    Writing   9438231 bytes
    Writing   8569543 bytes
    Writing  10303718 bytes
    Writing   8985073 bytes
    Writing   8087323 bytes
    Writing   8120662 bytes
    Writing    930942 bytes
    Wrote   150758965 bytes

To get more insight into the conversion process, the user may configure the LSST Logger by setting the
environment variable ``LSST_LOG_CONFIG`` pointing to a configuration file. The file should contain
the following configuration:

.. code-block::

    log4j.rootLogger=INFO, CONSOLE
    log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
    log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
    log4j.appender.CONSOLE.layout.ConversionPattern=%d{yyyy-MM-ddTHH:mm:ss.SSSZ} LWP:%X{LWP} QID:%X{QID} %-5p %c{2} - %m%n
    log4j.logger.lsst.qserv.partitioner=DEBUG

Here is an example of the output:

.. code-block::

    2025-05-07T01:42:55.662Z LWP: QID: DEBUG qserv.partitioner - Parquet : Created
    2025-05-07T01:42:55.662Z LWP: QID: DEBUG qserv.partitioner - Parquet : VmSize [MB] : 78.3477
    2025-05-07T01:42:55.662Z LWP: QID: DEBUG qserv.partitioner - Parquet : VmRSS [MB] : 22.4062
    2025-05-07T01:42:55.662Z LWP: QID: DEBUG qserv.partitioner - Parquet : Shared Memory [MB] : 20.2578
    2025-05-07T01:42:55.662Z LWP: QID: DEBUG qserv.partitioner - Parquet : Private Memory [MB] : 2.14844
    2025-05-07T01:42:55.668Z LWP: QID: DEBUG qserv.partitioner - Parquet : Total file size [Bytes] : 54286326
    2025-05-07T01:42:55.668Z LWP: QID: DEBUG qserv.partitioner - Parquet : Number of row groups : 1
    2025-05-07T01:42:55.668Z LWP: QID: DEBUG qserv.partitioner - Parquet : Number of rows : 18730
    2025-05-07T01:42:55.700Z LWP: QID: DEBUG qserv.partitioner - Parquet : Record size [Bytes] : 5192
    2025-05-07T01:42:55.700Z LWP: QID: DEBUG qserv.partitioner - Parquet : Batch size mem [Bytes] : 2.67387e+09
    2025-05-07T01:42:55.700Z LWP: QID: DEBUG qserv.partitioner - Parquet : Max RAM [MB] : 3000
    2025-05-07T01:42:55.700Z LWP: QID: DEBUG qserv.partitioner - Parquet : Record size [Bytes] : 5192
    2025-05-07T01:42:55.700Z LWP: QID: DEBUG qserv.partitioner - Parquet : Batch size [Bytes] : 514997
    2025-05-07T01:42:55.700Z LWP: QID: DEBUG qserv.partitioner - Parquet : Record size (approx. CSV string length) [Bytes] :  15324
    2025-05-07T01:42:55.700Z LWP: QID: DEBUG qserv.partitioner - Parquet : Max buffer size [Bytes] : 16777216
    2025-05-07T01:42:55.700Z LWP: QID: DEBUG qserv.partitioner - Parquet : Record buffer size [Bytes] : 15324
    2025-05-07T01:42:55.700Z LWP: QID: DEBUG qserv.partitioner - Parquet : Batch buffer size [Bytes] : 1094
    2025-05-07T01:42:55.700Z LWP: QID: DEBUG qserv.partitioner - Parquet : RecordBatchReader : batchSize [Bytes] : 1094
    2025-05-07T01:42:55.700Z LWP: QID: DEBUG qserv.partitioner - Parquet : RecordBatchReader : batch number : 18
    2025-05-07T01:41:35.475Z LWP: QID: DEBUG qserv.partitioner - Parquet : Column name : col1
    2025-05-07T01:41:35.475Z LWP: QID: DEBUG qserv.partitioner - Parquet : Column name : col2
    2025-05-07T01:41:35.475Z LWP: QID: DEBUG qserv.partitioner - Parquet : Column name : col3 not found in the table
    2025-05-07T01:43:33.170Z LWP: QID: DEBUG qserv.partitioner - Parquet : Buffer size [Bytes] : 6370040 of 16777216
    2025-05-07T01:41:35.475Z LWP: QID: DEBUG qserv.partitioner - Parquet : Column name : col1
    2025-05-07T01:41:35.475Z LWP: QID: DEBUG qserv.partitioner - Parquet : Column name : col2
    2025-05-07T01:41:35.475Z LWP: QID: DEBUG qserv.partitioner - Parquet : Column name : col3 not found in the table
    ...
    2025-05-07T01:41:35.475Z LWP: QID: DEBUG qserv.partitioner - Parquet : Column name : col1
    2025-05-07T01:41:35.475Z LWP: QID: DEBUG qserv.partitioner - Parquet : Column name : col2
    2025-05-07T01:41:35.475Z LWP: QID: DEBUG qserv.partitioner - Parquet : Column name : col3 not found in the table
    2025-05-07T01:41:35.486Z LWP: QID: DEBUG qserv.partitioner - Parquet : Buffer size [Bytes] : 930942 of 16777216
    2025-05-07T01:41:35.490Z LWP: QID: DEBUG qserv.partitioner - Parquet : End of file reached
    2025-05-07T01:41:35.490Z LWP: QID: DEBUG qserv.partitioner - Parquet : Destroyed

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
