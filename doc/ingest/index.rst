
.. highlight:: sql

##################
Ingesting catalogs
##################

.. toctree::
   :maxdepth: 2

   api/index
   qserv-ingest/index

============
Introduction
============

Unlike traditional RDBMS systems, Qserv does not support direct ingestion of data via
SQL ``INSERT`` statements. Neither one can create databases or tables directly via SQL DDL
statements like ``CREATE DATABASE``, ``CREATE TABLE`` and similar. Instead, data must be ingested
into Qserv using a collection of the REST services. The services represent the Qserv Ingest
API (covered in `The Ingest Workflow Developer's Guide <api/>`_) which provides the functionaly complete
set of tools and instructions needed for ingesting and managing data in Qserv. There are several
reasons for this design choice:

- Implementing a parser for the SQL DDL and DML statements is a complex and time-consuming process.
  Implementing a correct semantic of the SQL statements in a realm of the distributed database
  is even more dounting task.
- The performace of the SQL-based ingest protocol is not sufficient for the high-throughput data ingestion.
   - **Note**: Qserv is designed to handle the data volumes of the order of many Petabytes.
- The REST services (unlike the simple text-based SQL statements) allow for more *structural* data formats
  for user inputs such as schemas (``JSON``) and data (``CSV``). Verifying the syntactical and semantical
  correctness of the data is easier when the data are structured.
- The REST services provide a reliable and transparent mechanism for managing and tracking the distributed
  state of the data products within Qserv.
- Many operations on the REST services can be made idempotent and can be easily retried in case of failures.
- By not being bound to a particular SQL dialect, the REST services provide a more flexible and portable
  interface for the data ingestion. The API can be extended to support new types of the data management requests,
  new data formats and data sources as needed without changing the core of the Qserv engine.

The API serves as a foundation for designing and implementing the data ingestion processes that
are loosely called the *ingest workflows*. There may be many such workflows depending on a particular
use case, the amount of data to be ingested, data delivery requirements, and the overall complexity
of the data.

Read `The Ingest Workflow Developer's Guide <api/>`_ for further details on the REST services and their
usage. An explanation of a simple Kubernetes-based ingest workflow application `qserv-ingest <qserv-ingest/>`_
is also provided in this documentation portal.

Also note that a simple ingest API is provided by :ref:`http-frontend` for integsting and managing user tables.
