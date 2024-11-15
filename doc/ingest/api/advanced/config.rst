.. _ingest-api-advanced-global-config:

Global configuration options
----------------------------

This section presents the configuration parameters of Qserv and the Replication/Ingest system which may
affect the ingest activities. This section provides instructions for finding values of the parameters
via the API, explains roles of the parameters, and outlines possble usage of the parameters by the workflow.

The number of workers
^^^^^^^^^^^^^^^^^^^^^

The total number of Qserv workers is the main factor contributing to the performance of the catalog ingest operations.
The number can be obtained using the following REST service:

- :ref:`ingest-config-global-workers` (REST)

The workflow needs to analyze a section ``config.workers`` to select workers in the following state (both apply):

- ``is-enabled=1``
- ``is-read-only=0``

There are a few possibilities how the workflow could use this information. For example, the workflow
could start a separate transaction (or a set of transactions) per worker.

Worker-specific parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^

The second group of parameters found in the section ``config.general.worker`` is related to resources which
are available to the individual worker ingest services for processing contributions. These are instructions
for some of the most relevant parameters:

- ``num-loader-processing-threads``:

  The parameter affects a flow of ingest requests made via the proprietary binary protocol using the command-line
  tool :ref:`ingest-tools-qserv-replica-file`. To achieve the maximum throughput of the ingest the workflows
  should aim at having each participated worker loaded with as many parallel requests as there are threads
  reported by this parameter.

  .. warning::

      Exceeding the number of threads will result in having clients to wait for connections to be established
      with the workers. In some cases this may lead to the performance degradation if the network connection
      is unstable.

- ``num-http-loader-processing-threads``:

  The parameter affects a flow of ingest requests submitted via the HTTP-based ingest service. Note that
  this service is used for processing *synchronous* contribution requests and for submitting the *asynchronous*
  requests to the service.

  The workflow may use a value of the parameter differently, depenidng on a type of the contribution request.
  Requests which are *synchronous* should be submitted to the service in a way that the number of such requests
  per worker was close to the number of threads reported by this parameter. In this case the workflow should
  expect the maximum throughput of the ingest. The *asynchronous* requests aren't affected by the parameter,
  unless another workflow is using the service in the *synchronous* mode at the same time.

- ``num-async-loader-processing-threads``:

  The parameter represents the number of the request processing threads in a thread pool allocated for
  the *asynchronous* contribution requests. The workflow should aim at having the number of unfinished
  *asynchronous* requests submitted to the service close to (or exceeding) the number of threads reported
  by this parameter. To do so, the workflow should monitor the number and the status of the *asynchronous*
  requests at each worker and submit new requests to the service when the number of the requests being processed
  is less than the number of threads. The relevant services for monitoring the contribution requests at workers
  are documented in:

  - :ref:`ingest-worker-contrib-get` (WORKER)

  .. note::

      An alternative approach is to submit all *asynchronous* requests to the relevant worker services at once.
      The services will take care of processing the requests in the same order they were submitted. This approach
      may not work well where a specific order of the requests is important, or if all input data is not available
      at the time of the submission.

- ``ingest-charset-name``:

  The name of a character set for parsing the payload of the contributions. The workflow may override the default
  value of the parameter if the payload of the contributions is encoded in a different character set. See an
  attrubute ``charset_name`` in:

  - :ref:`ingest-worker-contrib-by-ref` (WORKER)
  - :ref:`ingest-worker-contrib-by-val` (WORKER)

- ``ingest-num-retries``, ``ingest-max-retries``:

  These parameters are related to the number of the automatic retries of the failed *asynchronous* requests
  specific in the parameter ``num_retries`` of the contribution request. The workflow may adjust the number
  of such retries if needed. A good example is when the workflow knows that a connection to the data source
  (a Web server or the object store) is unstable, or if the server might be overloaded. The workflow may increase
  the number of retries to ensure that the data is ingested successfully.

  .. note::

      The parameter ``ingest-max-retries`` is a hard limit for the number of retries regardless of what's
      specified in the request's attribute ``num_retries``.

- ``loader-max-warnings``:

  This parameter sets the default number for the number of warnings that the worker ingest service can
  capture from MySQL after attempting to ingest a contribution. The workflow may adjust the parameter
  for individual contributions by setting the desired limit in the request's attribute ``max_warnings``.
  The main purpose for setting the limit higher than the default value is to debug problem with the
  data of the contributions.
