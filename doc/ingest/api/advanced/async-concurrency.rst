.. _ingest-api-advanced-concurrency:

Concurrency control when processing async requests
--------------------------------------------------

.. note::

  This optional mechanism is designed specifically for handling contribution requests submitted asynchronously by reference.
  Workflows using the synchronous interface should implement their own request load balancing strategies.

The current implementation of the worker request processor for *asynchronously* submitted contribution requests
(:ref:`ingest-worker-contrib-by-ref`) follows a straightforward model:

- Incoming requests are managed by a single input queue.
- Requests are queued based on their creation timestamp.
- At server startup, a fixed-size pool of processing threads is initialized.
- The pool size is configured by the worker's parameter ``(worker,num-async-loader-processing-threads)``, which can be set
  at startup using the command line option ``--worker-num-async-loader-processing-threads=<num>``. By default, this value
  is twice the number of hardware threads on the worker host.
- Worker threads process requests sequentially from the queue's head.

This model can cause issues in some deployments where resource availability is limited, such as:

- Remote data sources (e.g., HTTP servers, object stores) may become overloaded if the total number of parallel requests from all
  workers exceeds the service capacity. For instance, disk I/O performance may degrade when services read too many files simultaneously.
- The performance of locally mounted distributed filesystems at workers may degrade if there are too many simultaneous file
  reads, especially when input data is located on such filesystems.
- Ongoing ingest activities can significantly degrade Qserv performance for user queries due to resource contention (memory, disk I/O, network I/O, CPU).
- The timing of ingests can be problematic. For instance, massive ingests might be scheduled at night, while less intensive
  activities occur during the day when user activity is higher.

Adjusting the number of processing threads in the service configuration is not an optimal solution because it requires restarting
all worker servers (or the entire Qserv in Kubernetes-based deployments) whenever the ingest workflow needs to manage resource usage.
Additionally, the constraints can vary based on the specific context in both "space" (ingesting particular databases from specific sources)
and "time" (when Qserv is under heavy load from user queries).

To mitigate these issues, the API provides a feature to control the concurrency level of processed requests. Limits can be configured
at the database level. Workflows can query or set these limits using the existing REST services, as detailed in the following section:

- :ref:`ingest-config` (REST)

Here is an example of how to configure all workers to limit concurrency to a maximum of 4 requests per worker for
the database ``test101``:

.. code-block:: bash

  curl http://localhost:25081/ingest/config \
    -X PUT -H 'Content-Type: application/json' \
    -d'{"database":"test101","ASYNC_PROC_LIMIT":4,"auth_key":""}'

Specifying a value of ``0`` will remove the concurrency limit, causing the system to revert to using the default number of processing threads.

Workflows can modify the limit at any time, and changes will take effect immediately. However, the new limit will only
apply to requests that are pulled from the queue after the change. Existing requests in progress will not be interrupted,
even if the limit is reduced.

The following example demonstrates how to use the related service to retrieve the current concurrency limit for a specific database:

.. code-block:: bash

    curl 'http://localhost:25081/ingest/config?database=test101' -X GET

This would return:

.. code-block:: json

    {   "config": {
            "ASYNC_PROC_LIMIT": 4,
            "database": "test101"
        },
        "error": "",
        "error_ext": {},
        "success": 1,
        "warning": "No version number was provided in the request's query."
    }
