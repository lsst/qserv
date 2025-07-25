.. _ingest-api-advanced-generated:

Generated columns
=================

This document provides a reference for the generated columns in the Qserv Ingest API. Values in these columns
are automatically computed based on the data ingested. The mechanism is based on the following concept
presented in https://dev.mysql.com/doc/refman/8.4/en/create-table-generated-columns.html. The basic idea can be
illustrated by the following example. Let's supposed we want to create a table with two real and one generated column
that will contain the sum of the two real columns:

.. code-block:: sql

    CREATE TABLE sums (
        a DOUBLE,
        b DOUBLE,
        c DOUBLE GENERATED ALWAYS AS (`a` + `b`) VIRTUAL
    );
    INSERT INTO sums (a, b) VALUES(1,1),(3,4),(6,8);

The table will contain the following data:

.. code-block:: sql

    SELECT * FROM sums;
    +----+----+----+
    | a  | b  | c  |
    +----+----+----+
    | 1  | 1  | 2  |
    | 3  | 4  | 7  |
    | 6  | 8  | 14 |
    +----+----+----+

Note that in this case, the generated column is not stored in the table, but it is computed on the fly when the data are requested.
In reality, MySQL offers two types of generated columns: *VIRTUAL* and *STORED*. The former are computed on the fly, while the latter are
computed during data ingests and then physically stored in the table. The Ingest API supports both types of generated columns.

When deciding on what type of generated column to use, consider the following:

- ``VIRTUAL``:

  - Column values are computed when rows are read.
  - A virtual column takes no storage.
  - Indexes are not allowed on virtual columns.

- ``STORED``:

  - Column values are evaluated and stored when rows are inserted or updated.
  - A stored column does require storage space and can be indexed.

There are other restrictions on each column type, so please refer to the MySQL documentation for more details.

Registering tables with generated columns
-----------------------------------------

The Ingest API allows you to define generated columns in the table registration request:

- :ref:`ingest-db-table-management-register-table` (CONTROLLER)

Here is an example of a request to register the table ``sums`` with the generated column ``c``:

.. code-block::

    {
        "table": "sums",
        "schema": [
            {"name": "a", "type": "DOUBLE"},
            {"name": "b", "type": "DOUBLE"},
            {"name": "c", "type": "DOUBLE GENERATED ALWAYS AS (`a` + `b`) VIRTUAL"}
        ],
        ...
    }

Preparing CSV files with generated columns
------------------------------------------

When preparing CSV files for ingestion, you have to include the ``\N`` placeholder for all generated columns in the data.
For example, if you want to insert data into the ``sums`` table, your CSV file should look like this:

.. code-block::

    1,1,\N
    3,4,\N
    6,8,\N

Qserv tools that are described in this documentation portal will automatically replace the ``\N`` placeholders
for columns mentioned in the ```optional``` section of the JSON configuration file.

Translating parquet files into CSV files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When translating Parquet files into CSV files, the generated columns are automatically handled by the translation tool:

- :ref:`ingest-data-conversion` (TOOLS)

The following example illustrates how to prepare a CSV file for the ``sums`` table when translating the Parquet file
that has only two columns ``a`` and ``b``. The generated column ``c`` is not present in the Parquet file, so it is
represented by the ``\N`` placeholder in the resulting CSV file:

.. code-block::

    {
        "columns": [
            "a",
            "b",
            "c"
        ],
        "optional": [
            "c"
        ]
    }

Note that the missing column must be mentioned in the desired position in the ``columns`` section of the JSON configuration file.
And it has to to be mentioned in the ``optional`` section as well, so that the translation tool knows to replace it with
the ``\N`` placeholder in the CSV file.

Partitioning parquet files into CSV files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When partitioning Parquet files into CSV files, the generated columns are automatically handled by the partitioning tool:

- :ref:`ingest-data-partitioner` (TOOLS)

Likewise the translation tool, the partitioning tool will automatically replace the missing generated columns with the ``\N`` placeholder
in the CSV files. The following example illustrates how to configure the partitioner for the column ``c`` in the ``sums`` table:

..  code-block:: json

    {
        "in":{
            "csv":{
                "null":"\\N",
                "delimiter":"\t",
                "escape":"\\",
                "field":[
                    "a",
                    "b",
                    "c"
                ],
                "optional":[
                    "c"
                ]
            }
        }
    }
