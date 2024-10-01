
.. _ingest-api-advanced-warnings:

Using MySQL warnings for the data quality control
=================================================

The context
-----------

The table ingest is presently implemented using MySQL/MariaDB bulk insert statement:

.. code-block:: sql

  LOAD DATA [LOCAL] INFILE ...

This is currently the most efficient and performant method for adding rows into tables from input CSV files (:ref:`ingest-api-concepts-contributions`).
The technique is detailed in https://mariadb.com/kb/en/load-data-infile/.

This method differs significantly from the standard SQL ``INSERT``. One caveat of using this mechanism is that MySQL (MariaDB)
attempts to ingest all input data into the table, even if some rows (or fields within rows) cannot be correctly interpreted. Consequently:

- The table may contain fewer (or sometimes more) rows than expected.
- Some cell values may be truncated.
- Some cells may contain incorrect data.

In order to help client applications detect these problems, MySQL offers diagnostic tools (queries, counters) that report
internal issues encountered during data loading. These are detailed in https://mariadb.com/kb/en/show-warnings/.

The current implementation of the Ingest system leverages these features by capturing warnings (as well as notes and errors)
and recording them within the Replication database in association with the corresponding contribution requests. This is done
for each request regardless of how it was submitted, whether via the proprietary binary protocol of
:ref:`ingest-tools-qserv-replica-file`, or by calling the REST services of the Ingest system:

- :ref:`ingest-worker-contrib-by-ref` (WORKER)
- :ref:`ingest-worker-contrib-by-val` (WORKER)

Both interfaces also offer a parameter to control the depth of the warning reports by specifying the desired limit on
the number of warnings to be retained for each request. This limit is optional. If not specified at the time of request
submission, the service will use the limit configured at the worker ingest server's startup.

The REST services that return information on contributions have another optional parameter that indicates whether the client
is interested in seeing just the total number of warnings or the complete description of all warnings retained by the system.
The effect of this parameter on the resulting JSON object returned by the services is explained in:

- :ref:`ingest-worker-contrib-descriptor` (WORKER)

In addition to the individual descriptions (if required) of the warnings, the relevant services also report three summary counters:

``num_warnings``:
  The total number of warnings detected by MySQL when loading data into the destination table. Note that this number is not
  the same as the number of warning descriptions returned by the REST services. Unlike the latter, ``num_warnings`` represents
  the true number of warnings. Only a subset of those is captured in full detail by MySQL.

``num_rows``:
  The total number of rows parsed by the Ingest system in the input file. The ingest service always parses the input files as
  it needs to extend each row in order to prepend them with a unique identifier of the corresponding super-transaction (the name
  of the added column is ``qserv_trans_id``).

``num_rows_loaded``:
  The total number of rows that were actually loaded by the system into the destination table. Note that in case MySQL
  encountered any problems with the input data while interpreting and ingesting those into the destination table, this
  counter may not be the same as ``num_rows``. In practice, a value reported in ``num_rows_loaded`` could be either lower or
  higher than the value reported in ``num_rows``.

Using warnings and counters
---------------------------

The interfaces described above provide a range of options for workflow developers and Qserv data administrators:

- Workflow developers can enhance their workflows to analyze the reported counters for contributions. This helps determine
  if an ingest operation was genuinely successful or if issues occurred.
- Data administrators can utilize both counters and warning descriptions to analyze ingest results and debug input data.
  Any data issues can be traced back to their source (e.g., LSST Pipeline).

The following subsections present techniques that can be leveraged in this context.

Analyzing counters
^^^^^^^^^^^^^^^^^^

The ingest operation should be considered successful if both of the following conditions are met:

- ``num_warnings`` equals to ``0``
- ``num_rows`` is the same as ``num_rows_loaded``

If these conditions are not met, a data administrator should inspect the warning descriptions in detail to identify the cause
of the discrepancy.

Increasing the depth of the warnings queue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. hint::

  The default imposed by MySQL would be ``64``. And the upper bound for the limit is ``65535``.

Significantly increasing the limit above the default value should be considered a temporary measure. All warnings are recorded
within the persistent state of the Replication/Ingest system, and the database serving the system may have limited storage
capacity. Capturing many millions of descriptions across all contributions when ingesting medium-to-large scale catalogs may
also significantly reduce the overall performance of the ingest system.

Data administrators may temporarily increase the upper limit for the number of warnings to debug input data. The limit can be
set when submitting contribution requests via the APIs mentioned earlier in this chapter. Alternatively, the Replication/Ingest
worker server can be started with the following command-line option:

.. code-block:: bash

  qserv-replica-worker --worker-loader-max-warnings=<number>
