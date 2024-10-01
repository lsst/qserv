.. _ingest-api-concepts-overview:

Overview of the ingest workflow
===============================

The ingest workflow must accomplish a series of tasks to ingest data into Qserv.
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

Where the ready to ingest data will be located?

- Are there any data staging areas available?
- :ref:`ingest-api-advanced-contributions` (ADVANCED)

Prepare the input data
----------------------

Data files (table *contributions*) to be ingested into Qserv need to be in the ``CSV`` format. It's up to the workflow
to ensure that the data is in the right format and that it's sanitized to ensure the values of the columns
are compatible with the MySQL expectations.

- :ref:`ingest-data` (DATA)

Note that the data preparation stage depends on the types of tables to be ingested. Read about the table types in:

- :ref:`ingest-api-concepts-table-types` (CONCEPTS)

Register or un-publish a database
---------------------------------

The main goal of this step is to ensure that the database is ready for registering new tables. Firstly,
the database should be registered in the Replication/Ingest system. Secondly, the database should be
found (or put into) the *unpublished* state. Read about the database states in the following document
section:

- :ref:`ingest-api-concepts-publishing-data` (CONCEPTS)

Further steps depend on the state of the database. If the database doesn't exists in the Replication/Ingest system
it should be registered using:

- :ref:`ingest-db-table-management-register-db` (REST)

The newely registered database will be always in the *unpublished* state. If the database already exists in
the Replication/Ingest and it's in the *published* state it should be *unpublished* state using:

- :ref:`ingest-db-table-management-unpublish-db` (REST)

Register tables
---------------

Before ingesting data into Qserv the corresponding tables should be registered in the Replication/Ingest system.
Tables are registered using:

- :ref:`ingest-db-table-management-register-table` (REST)

Table registration requests should includes various information on each table, such as:

- the name of the database where the table belongs
- the name of the table
- the type of the table
- the schema

Detailed instructions on this subjects can be found in the description of the service mentioned above.

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

  Please be aware that the ingest activities can also be affected by the global configuration parameters of
  the Replication/Ingest system:

  - :ref:`ingest-api-advanced-global-config` (ADVANCED)

Start transactions
------------------

Making the right choices on how many transactions to start and how many contributions to send in a scope of each transaction
is a key to the ingest performance. The transactions are used to group the contributions. In some cases, when
contributions fail the transactions should be aborted. Should this happen all ingest efforts made in the scope of
the failed transactions would have to be rolled back, and the workflow would have to start the corresponding ingest
activities from the beginning. Hence the workflow should be prepared to handle the transaction aborts and make
reasonable decisions on the amount of data to be sent in a scope of each transaction (a "size" of the transaction)
based on the risk assesment made by the workflow developers or the data administrators who would be using the workflow
for ingesting a catalog.

.. hint::

  It's recommended to make the transaction management logic of the workflow configurable.

More information on this subject can be found in:

- :ref:`ingest-api-concepts-transactions` (CONCEPTS)
- :ref:`ingest-api-advanced-transactions` (ADVANCED)
- :ref:`ingest-trans-management-start` (REST)

Figure out locations of tables and chunks
-----------------------------------------

The design of the API requires the workflow to know the locations of the tables and chunks at workers.
The locations are needed to forward the table contribution requests directly to the corresponding worker
services. The locations can be obtained using services covered in the following document:

- :ref:`table-location` (REST)

Send the data to the workers
----------------------------

At this stage the actual ingest activities are started. The reader should read the following document document first
to understand the concepts of the *contributions*:

- :ref:`ingest-api-concepts-contributions` (CONCEPTS)

The REST API for initiating the contribuiton requests is covered in the following documents:

- :ref:`ingest-worker-contrib-by-ref` (REST)
- :ref:`ingest-worker-contrib-by-val` (REST)

Monitor the progress of the ingest activities
----------------------------------------------

The workflow should always be avare about the progress of the ingest activities, and about the status of the
contribution requests. This is need for (at least) three reasons:

#. To know when the ingest activities are finished
#. To know when the ingest activities (and which requests) are failed
#. To make more contribution requests if needed

In the simplest *linear* design of the workflow, such as the one presented in the :ref:`ingest-api-simple`,
the workflow may implement the monitoring as a separate step after making all contribution requests. In more
realistic scenarious the monitoring stage should be an integral part of the same logic that is responsible for
making the contribution requests.

Besides the monitoring of the contribution requests the workflow should also monitor the status of the databases,
transactions and Qserv workers to be sure that the ingest activities are going as planned and that the underlying
services are healthy. These are the relevant services for the monitoring:

- :ref:`ingest-config-global-workers` (REST)
- :ref:`ingest-trans-management-status` (REST)

Commit/abort the transactions
-----------------------------

Once all contributions are successfully ingested the transactions should be commited. If any problems occured within
the transactions the workflow should be prepared to handle the transaction aborts. Both operations are performed by:

- :ref:`ingest-trans-management-end` (REST)

Read more about the transactions and transaction aborts in:

- :ref:`ingest-api-concepts-transactions` (CONCEPTS)
- :ref:`ingest-api-advanced-transactions-abort` (ADVANCED)

Another option in the case of a catastrophic failure during the ingest would be to scrap the whole database
or the tables and start the ingest activities from the beginning. This is a more radical approach, but it's
sometimes the only way to recover from the failure. The services for deleting the database and the tables are
covered in:

- :ref:`ingest-db-table-management-delete` (REST)

.. warning::

  The deletion of the database or the tables is an irreversible operation. Use it with caution.

Publish the database
--------------------

.. warning::

  Depending on the types of tables created by the workflow, the amount of data ingested into the tables,
  and the number of transactions created during the effort, the database publishing operation may take a while.
  There is always a chance that it may fail should anything unpredicted happen during the operation. This could be
  a problem with the underlying infrastructure, the network, the database, the workers, etc. Or it could be a problem
  with the ingested data. The workflow should be prepared to handle the failure of the database publishing operation
  and check the completion status of the request.

.. hint::

  The current implementation of the operation is *synchronous*, which means the workflow would have to wait
  before the service sends back a response to be analyzed. However, the implementation of the operation is *idempotent*,
  which means the workflow can retry the operation as many times as needed without any side effects should any network
  problems occur during the operation.

Formally, this would be the last stage of the actual ingest. The database and the tables are published to make them
visible to the users. The database and the tables are published using the following services:

- :ref:`ingest-db-table-management-publish-db` (REST)

All new tables that were registered in the database by the workflow would be published automatically.
And the database would be placed into *published* state.

Read more on this concept in:

- :ref:`ingest-api-concepts-publishing-data` (CONCEPTS)

Verify the ingested data products
---------------------------------

This step is optional. A possibility of implementing the automatic verification if the ingested
data products are correct and consistent depends on the workflow requirements and the data.
These are some very basic verification steps that the workflow may want to implement:

- the data can be queried
- the data can be compared to the original data
- the number of rows in the tables is correct

Perform the optional post-ingest data management operation on the ingested tables
---------------------------------------------------------------------------------

This step is optional. The workflow may want to perform some post-ingest data management operations on the ingested tables.
An alternative approach is to perform these operations after verifying the ingested data products.
These operations are covered in:

- :ref:`ingest-api-post-ingest` (API)
