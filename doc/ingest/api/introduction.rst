Introduction
============

This document presents an API that is available in Qserv for constructing the data ingest applications (also mentioned
in the document as *ingest workflows*). The API is designed to provide a high-performance and reliable mechanism for
ingesting large quantities of data where the high performance or reliability of the ingests is at stake.
The document is intended to be a practical guide for the developers who are building those applications.
It provides a high-level overview of the API, its main components, and the typical workflows that can be built using the API.

At the very high level, the Qserv Ingest system is comprised of:

- The REST server that is integrated into the Master Replication Controller. The server provides a collection
  of services for managing metadata and states of the new catalogs to be ingested. The server also coordinates
  its own operations with Qserv itself and the Qserv Replication System to prevent interferences with those
  and minimize failures during catalog ingest activities.
- The Worker Ingest REST service run at each Qserv worker node alongside the Qserv worker itself and the worker MariaDB server.
  The role of these services is to actually ingest the client's data into the corresponding MySQL tables.
  The services would also do an additional (albeit, minimal) preprocessing and data transformation (where or when needed)
  before ingesting the input data into MySQL. Each worker server also includes its own REST server for processing
  the "by reference" ingest requests as well as various metadata requests in the scope of the workers.

Implementation-wise, the Ingest System heavily relies on services and functions of the Replication System including
the Replication System's Controller Framework, various (including the Configuration) services, and the worker-side
server infrastructure of the Replication System.

Client workflows interact with the system's services via open interfaces (based on the HTTP protocol, REST services,
JSON data format, etc.) and use ready-to-use tools to fulfill their goals of ingesting catalogs.

Here is a brief summary of the Qserv Ingest System's features:

- It introduces the well-defined states and semantics into the ingest process. With that, a process of ingesting a new catalog
  now has to go through a sequence of specific steps maintaining a progressive state of the catalog within Qserv
  while it's being ingested. The state transitions and the corresponding enforcements made by the system would
  always ensure that the catalog would be in a consistent state during each step of the process.
  Altogether, this model increases the robustness of the process, and it also makes it more efficient.

- To facilitate and implement the above-mentioned state transitions the new system introduces a distributed
  *tagging* and *checkpointing* mechanism called *super-transactions*. The transactions allow for incremental
  updates of the overall state of the data and metadata while allowing to safely roll back to a prior consistent
  state should any problem occur during data loading within such transactions.

  - The data tagging capability of the transactions can be also used by the ingest workflows and by
    the Qserv administrators for bookkeeping of the ingest activities and for the quality control of
    the ingested catalogs.

- In its very foundation, the system has been designed for constructing high-performance and parallel ingest
  workflows w/o compromising the consistency of the ingested catalogs.

- For the actual data loading, the system offers plenty of options, inluding pushing data into Qserv directly
  via a proprietary binary protocol using :ref:`ingest-tools-qserv-replica-file`, :ref:`ingest-worker-contrib-by-val`
  in the HTTP request body, or  :ref:`ingest-worker-contrib-by-ref`. In the latter case, the input data (so called table
  *contributions*) will be pulled by the worker services from remote locations as instructed by the ingest workflows.
  The presently supported sources include the object stores (via the HTTP/HTTPS protocols) and the locally mounted
  distributed filesystems (via the POSIX protocol).

  - The ongoing work on the system includes the development of the support for the ingesting contributions
    from the S3 object stores.

- The data loading services also collect various information on the ongoing status of the ingest activities,
  abnormal conditions that may occur during reading, interpreting, or loading the data into Qserv, as well
  as the metadata for the data that is loaded. The information is retained within the persistent
  state of the Replication/Ingest System for the monitoring and debugging purposes. A feedback is provided
  to the workflows on various aspects of the ingest activities. The feedback is useful for the workflows to adjust their
  behavior and to ensure the quality of the data being ingested.

  - To get further info on this subject, see sections :ref:`ingest-general-error-reporting` and
    :ref:`ingest-worker-contrib-descriptor-warnings`.
    In addition, the API provides REST services for obtaining metadata on the state of catalogs, tables, distributed
    transactions, contribution requests, the progress of the requested operations, etc.

**What the Ingest System does NOT do**:

- As per its current implementation (which may change in the future) it does not automatically partition
  input files. This task is expected to be a responsibility of the ingest workflows. The only data format
  is is presently supported for the table payload are ``CSV`` and ``JSON`` (primarily for ingesting
  user-generated data products as explained in :ref:`http-frontend-ingest`).

- It does not (with an exception of adding an extra leading column ``qserv_trans_id`` required by
  the implementation of the previously mentioned *super-transactions*) pre-process the input ``CSV``
  payload sent to the Ingest Data Servers by the workflows for loading into tables.
  It's up to the workflows to sanitize the input data and to make them ready to be ingested into Qserv.

More information on the requirements and the low-level technical details of its implementation (unless it's
needed for the purposes of this document's goals) can be found elsewhere.

It's recommended to read the document sequentially. Most ideas presented in the document are introduced in
a section :ref:`ingest-api-concepts` and illustrated with a simple practical example in  :ref:`ingest-api-simple`.
The section is followed by a few more sections covering :ref:`ingest-api-advanced` and :ref:`ingest-api-post-ingest`.
The :ref:`ingest-api-reference` section of the document provides complete descriptions of the REST services and tools
mentioned in the document.
