.. _ingest-api-concepts-overview:

Overview of the ingest workflow
===============================

In the nutshell, any ingest workflow has to accomlish quite a few tasks in order to ingest data into Qserv.
These tasks are presented in the correct order below:

Plan the ingest
---------------

.. hint::

  You may also contact Qserv experts or Qserv administrators to get help on the planning stage.

There is a number of important questions to be answered and decisions to be made ahead of time in the following
areas before starting ingesting data into Qserv. Knowing these facts allows to organize the ingest activities in
the most efficient way.

Creating a new database or adding tables to the existing one?

- :ref:`ingest-api-concepts-publishing-data` (CONCEPTS)

What are the types of tables to be ingested?

- :ref:`ingest-api-concepts-table-types` (CONCEPTS)

What should be the values of the partitioning parameters of the partitioned tables?

- :ref:`ingest-api-concepts-database-families` (CONCEPTS)

What is a scale of the planned ingest effort?

- The amount of data (rows, bytes) to be ingested in each table
- The number of the ``CSV`` files to be ingested
- Sizes of the files
- The number of the workers that are available in Qserv
- :ref:`ingest-api-advanced-multiple-transactions` (ADVANCED)

Where the ready to ingest data will be located?

- Are there any data staging areas available?
- :ref:`ingest-api-advanced-contribution-requests` (ADVANCED)

Prepare the input data
----------------------

Data files (table *contributions*) to be ingested into Qserv need to be in the ``CSV`` format. It's up to the workflow
to ensure that the data is in the right format and that it's sanitized to ensure the values of the columns
are compatible with the MySQL expectations.

- :ref:`ingest-data` (DATA)

Prepare configuration files
---------------------------

The configurations of the ingested entities (databases, tables, etc.) are presented to the Ingest system
in a form of thw JSON objects.

Register or un-publish a database
---------------------------------

- :ref:`ingest-api-concepts-publishing-data` (CONCEPTS)
- :ref:`ingest-db-table-management-register-db` (REST)
- :ref:`ingest-db-table-management-unpublish-db` (REST)

Register tables
---------------

- name
- type

  - type-specific attributes, some which are referring to other tables (names and the foreign keys)
  - schema (including column names and types)

- :ref:`ingest-db-table-management-register-table` (REST)

Configure the Ingest service
----------------------------

This step is optional. And it's mostly needed to adjust the default configuration parameters of the Ingest service
to allow pulling contributions from the data staging areas, such as web servers, cloud storage, etc. Examples of
the configuration parameters are: timeouts, the number of parallel requests, SSL/TLS certificates, HTTP/HTTPS proxy
settings, etc. More information on this subject can be found in:

- :ref:`ingest-config` (REST)

These parameters can be adjusted in real time as needed. The changes get into effect immediately. Note that
the parameters are set on the database level. For example, the configuration parameters set for the database ``db1``
will not affect the ingest activities for the database ``db2``.

.. note::

  Please, be aware that the ingest activities can be also affected by the global configuration parameters of
  the Replication/Ingest system:

  - :ref:`ingest-api-advanced-global-config` (ADVANCED)

Start transactions
------------------

- :ref:`ingest-api-concepts-transactions` (CONCEPTS)
- :ref:`ingest-trans-management-start` (REST)

Figure out locations of tables and chunks
-----------------------------------------

- Connection parameters of the workers where the table and chunk data (*contributions*) will be sent.
- Note that it's a responsibility of the workflow to contact the workers and to provide them with the data
  or a reference to the data.
- The API provides the REST services for obtaining the desired information for the data products
  that are being ingested.

- :ref:`table-location` (REST)

Send the data to the workers
----------------------------

Initiate the ingest activities:

- :ref:`ingest-api-concepts-contributions` (CONCEPTS)
- :ref:`ingest-worker-contrib-by-ref` (REST)
- :ref:`ingest-worker-contrib-by-val` (REST)

Monitor the progress of the ingest activities
----------------------------------------------

- The workflow can query the REST services to get the status of databases, tables, transactions
  and the data contribution requests.

Commit the transactions
-----------------------

- :ref:`ingest-api-concepts-transactions` (CONCEPTS)
- :ref:`ingest-api-advanced-transaction-abort` (ADVANCED)
- :ref:`ingest-trans-management-end` (REST)

Publish the database
--------------------

- the table would be published automatically
- when the stage is fiished the database and the tables will be visible to the users
- :ref:`ingest-api-concepts-publishing-data` (CONCEPTS)
- :ref:`ingest-db-table-management-publish-db` (REST)

Verify the ingested data products
---------------------------------

- the data can be queried
- the data can be compared to the original data

Perform the optional post-ingest data management operation on the ingested tables
---------------------------------------------------------------------------------

- :ref:`ingest-api-post-ingest` (API)
