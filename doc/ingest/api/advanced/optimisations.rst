
.. _ingest-api-advanced-optimisations:

Optimizations in using the REST services
========================================

When designing a workflow, it is crucial to avoid overloading the REST services with repeated or inefficient requests.
Whenever possible, make certain requests once and reuse their results. This is particularly important for workflows
designed for parallel ingests, where the results of some requests can be shared among parallel
activities (processes, etc.) within the workflows. 

While this document does not cover all possible optimizations for interacting with the services, it is
the responsibility of the workflow developer to determine what can be cached or shared based on
the progressive state of the ingested catalog and the organization of the workflow
Below are some of the most useful techniques.

.. _ingest-api-advanced-optimisations-batch:

Batch mode for allocating chunks
--------------------------------

..  note::

    This optimization is feasible when all chunk numbers are known upfront.
    A common scenario is when the workflow is ingesting a large dataset that has already been
    *partitioned* into chunks. In such cases, the chunk numbers are known before the ingestion begins.

In the example of the :ref:`ingest-api-simple` presented earlier, chunk allocations were made on a per-chunk basis
(:ref:`table-location-chunks-one`). While this method works well for scenarios with a small number of chunks, it may
slow down the performance of workflows ingesting large numbers of chunks or making numerous requests to the chunk
allocation service. This is because chunk allocation operations can be expensive, especially in a Qserv setup with
many pre-deployed chunks. In such cases, chunk allocation requests may take a significant amount of time.
To address this issue, the system provides a service for allocating batches of chunks, as explained in:

- :ref:`table-location-chunks-many` (REST)

In the context of the earlier presented example of a simple workflow the chunk allocation request object would
look like this:

.. code-block:: json

    {   "transaction_id" : 123,
        "chunks" : [ 187107, 187108, 187109, 187110 ]
    }

The result could be reported as:

.. code-block:: json

    {   "location":[
            { "chunk":187107, "worker":"db01", "host":"qserv-db01", "port":25002 },
            { "chunk":187108, "worker":"db02", "host":"qserv-db02", "port":25002 },
            { "chunk":187109, "worker":"db01", "host":"qserv-db01", "port":25002 },
            { "chunk":187110, "worker":"db02", "host":"qserv-db02", "port":25002 }
        ]
    }

The request can be made once, and its results can be distributed among parallel activities within the workflow
to ingest the corresponding chunk contributions.
