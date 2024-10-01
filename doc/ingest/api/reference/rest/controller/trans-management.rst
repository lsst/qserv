.. _ingest-trans-management:

Transaction management
======================

.. note::

  - The transaction management services which modify a state of transactions are available only to the authorized users.
    The authorization is based on the authentication key. The key is used to prevent unauthorized access to the services.

  - The schema of the JSON object returned for each transaction is the same for all services in the group.
    The schema is described in the section:

    - :ref:`ingest-trans-management-descriptor`

.. _ingest-trans-management-status:

Status of a transaction
-----------------------

There are two services in this group. They are documented in the dedicated sections below.

.. _ingest-trans-management-status-many:

Database transactions
^^^^^^^^^^^^^^^^^^^^^

The service returns the information on many transactions in a scope of a database or databases selected via optional
filters passed via the request's query. The service is meant to be used by workflows for monitoring the status of
transactions and for debugging purposes. To see an actual progress of a transaction (e.g. to see the contributions
loaded into the destination table) a workflow should use the service: :ref:`ingest-trans-management-status-one`.

..  list-table::
    :widths: 10 15 75
    :header-rows: 1

    * - method
      - service
      - query parameters
    * - ``GET``
      - ``/ingest/trans``
      - | ``database=<name>``
        | ``family=<name>``
        | ``all_databases={0|1}``
        | ``is_published={0|1}``
        | ``include_context={0|1}``
        | ``contrib={0|1}``
        | ``contrib_long={0|1}``
        | ``include_log={0|1}``
        | ``include_warnings={0|1}``
        | ``include_retries={0|1}``

Where:

``database`` : *string* = ``""``
  The optional name of the database to filter the transactions by. If the parameter is present and if
  it's not empty then attributes ``family``, ``all_databases`` and ``is_published`` are ignored.

``family`` : *string* = ``""``
  The optional name of the database family. If the parameter is present and if
  it's not empty then a scope of a request will be narrowed to databases - members of the given family.
  Otherwise all databases regardless of their family membership will be considered.

  **Notes**:

  - The parameter is ignored if the parameter ``database`` is present.
  - The final selection of the databases is also conditioned by the values of the optional parameters
    ``all_databases`` and ``is_published``. See the description of the parameters for more details.

``all_databases`` : *number* = ``0``
  The optional flag which is used for further filtering of databases selected by the parameter family.
  A value of ``0`` tells the service that the parameter ``is_published`` should be used to further filter database
  selection to the desired subset. Any other value would mean no additional filters (hence ignoring ``is_published``),
  hence including databases selected by the parameter family.

  **Note**: The parameter is ignored if the parameter ``database`` is present.

``is_published`` : *number* = ``0``
  The optional flag is used only if enabled by setting the previous parameter ``all_databases=0``.
  A value of ``0`` tells the service to narrow the database selection to databases which are not *published*.
  Any other value would select the *published* databases.

  **Note**: The parameter is ignored if the parameter ``database`` is present or when ``all_databases=1``.

``include_context`` : *number* = ``0``
  The optional flag tells the service to include the transaction context object in the report for each transaction.
  See the documentation on services :ref:`ingest-trans-management-start` or :ref:`ingest-trans-management-end` for further
  details.

  .. warning::

    Potentially, each context object could be as large as **16 MB**. Enable this option only if you really need
    to see contexts for all transactions. Otherwise use an alternative (single transaction) request to pull one
    transaction at a time.

``contrib`` : *number* = ``0``
  The optional flag tells the service whether the transaction contribution objects should be included
  into the report. See details on this flag in the dedicated section below.

  .. warning::

    Even though individual contribution objects aren't large, the total number of contribution ingested
    in a scope of each transaction (and all transactions of a database, etc.) could be quite large.
    This would result in a significant emount of data reported by the service. In extreme cases, the response
    object could be **1 GB** or larger. Enable this option only if you really need to see contributions
    for selected transactions. Otherwise use an alternative (single transaction) request to pull one transaction
    at a time: :ref:`ingest-trans-management-status-one`.

``contrib_long`` : *number* = ``0``
  This optional flag is considered only if ``contrib=1``. Setting a value of the flag to any value other
  than ``0`` will result in returning detailed info on the contributions. Otherwise (if a value of the parameter
  is set to ``0``) only the summary report on contributions will be returned. 

``include_log`` : *number* = ``0``
  The optional flag tells the service to include the transaction log in the report for each transaction.
  The log is a list of events that were generated by the system in response to the transaction management
  reequests. Each entry in the log is a JSON object that includes the timestamp of the event, the event type,
  etc. See **TODO** for the details on the log entries.

``include_warnings`` : *number* = ``0``
  The optional flag, if set to any value that differs from ``0``, tells the service to include MySQL warnings
  captured when loading contributions into the destination table. Warnings are reported in a context of
  contributiond should they be allow in the report.

  **Note**: The parameter is ignored if ``contrib=0`` or if ``contrib_long=0``.

``include_retries`` : *number* = ``0``
  The optional flag, if set to any value that differs from ``0``, tells the service to include the information
  on the retries to load contributions that were made during the transaction. Retries are reported in a context of
  contributiond should they be allow in the report.

  **Note**: The parameter is ignored if ``contrib=0`` or if ``contrib_long=0``.

This is an example of the most typical request to the service for pulling info on all transactions of ``gaia_edr3``:

.. code-block:: bash

    curl -X GET "http://localhost:25081/ingest/trans?database=gaia_edr3"

The service will return a JSON object with the summary report on the transactions in the following JSON object:

.. code-block:: json

    {
      "success" : 1,
      "warning" : "No version number was provided in the request's query.",
      "error" : "",
      "error_ext" : {},
      "databases" : {
          "gaia_edr3" : {
            "is_published" : 0,
            "num_chunks" : 1558,
            "transactions" : [
                {
                  "database" : "gaia_edr3",
                  "log" : [],
                  "start_time" : 1726026383559,
                  "end_time" : 0,
                  "begin_time" : 1726026383558,
                  "id" : 1632,
                  "state" : "STARTED",
                  "transition_time" : 0,
                  "context" : {}
                },
                {
                  "end_time" : 1727826539501,
                  "context" : {},
                  "begin_time" : 1726026383552,
                  "log" : [],
                  "transition_time" : 1727826539218,
                  "database" : "gaia_edr3",
                  "start_time" : 1726026383553,
                  "state" : "ABORTED",
                  "id" : 1631
                },
                {
                  "database" : "gaia_edr3",
                  "end_time" : 1727826728260,
                  "id" : 1630,
                  "transition_time" : 1727826728259,
                  "start_time" : 1726026383547,
                  "begin_time" : 1726026383546,
                  "log" : [],
                  "state" : "FINISHED",
                  "context" : {}
                },

**Note**: that the report doesn't have any entries for the contributions. The contributions are not included in the report since
the parameter ``contrib`` was not set to ``1``. The log entries are also missing since the parameter ``include_log`` was not set to ``1``.
Also, the transaction context objects are not included in the report since the parameter ``include_context`` was not set to ``1``.

.. _ingest-trans-management-status-one:

Single transaction finder
^^^^^^^^^^^^^^^^^^^^^^^^^

The service returns the information on a single transaction identified by its unique identifier ``<id>`` passed
via the request's query:

..  list-table::
    :widths: 10 15 75
    :header-rows: 1

    * - method
      - service
      - query parameters
    * - ``GET``
      - ``/ingest/trans/<id>``
      - | ``include_context={0|1}``
        | ``contrib={0|1}``
        | ``contrib_long={0|1}``
        | ``include_log={0|1}``
        | ``include_warnings={0|1}``
        | ``include_retries={0|1}``

Where the parameters are the same as for the service :ref:`ingest-trans-management-status-many`.

This is an example of using the service for pulling info on a transaction ``1630`` and obtaining
the summary report on contributions and the transaction context:

.. code-block:: bash

    curl -X GET "http://localhost:25881/ingest/trans/1630?contrib=1"

The service returns a JSON object that has the following structure (the report is truncated by removing stats
on all workers but ``db12`` for brevity):

.. code-block:: json

    {
      "databases" : {
          "gaia_edr3" : {
            "num_chunks" : 1558,
            "is_published" : 0,
            "transactions" : [
                {
                  "id" : 1630,
                  "database" : "gaia_edr3",
                  "end_time" : 1727826728260,
                  "start_time" : 1726026383547,
                  "begin_time" : 1726026383546,
                  "transition_time" : 1727826728259,
                  "log" : [],
                  "context" : {},
                  "state" : "FINISHED",
                  "contrib" : {
                      "summary" : {
                        "num_failed_retries" : 0,
                        "num_chunk_files" : 156,
                        "last_contrib_end" : 1726026945059,
                        "num_regular_files" : 0,
                        "num_rows" : 223420722,
                        "table" : {
                            "gaia_source" : {
                              "num_failed_retries" : 0,
                              "overlap" : {
                                  "num_rows" : 6391934,
                                  "num_warnings" : 0,
                                  "num_rows_loaded" : 6391934,
                                  "data_size_gb" : 5.97671127319336,
                                  "num_files" : 155,
                                  "num_failed_retries" : 0
                              },
                              "num_files" : 156,
                              "num_rows_loaded" : 217028788,
                              "num_warnings" : 0,
                              "data_size_gb" : 201.872497558594,
                              "num_rows" : 217028788
                            }
                        },
                        "num_workers" : 9,
                        "first_contrib_begin" : 1726026383616,
                        "num_rows_loaded" : 223420722,
                        "worker" : {
                            "db12" : {
                              "num_failed_retries" : 0,
                              "num_regular_files" : 0,
                              "num_chunk_files" : 18,
                              "num_rows_loaded" : 52289369,
                              "num_warnings" : 0,
                              "data_size_gb" : 48.6947402954102,
                              "num_chunk_overlap_files" : 23,
                              "num_rows" : 52289369
                            },
                        },
                        "num_warnings" : 0,
                        "num_files_by_status" : {
                            "LOAD_FAILED" : 0,
                            "IN_PROGRESS" : 0,
                            "CANCELLED" : 0,
                            "CREATE_FAILED" : 0,
                            "READ_FAILED" : 0,
                            "FINISHED" : 311,
                            "START_FAILED" : 0
                        },
                        "num_chunk_overlap_files" : 155,
                        "data_size_gb" : 207.849166870117
                      },
                      "files" : []
                  }
                },

**Note**: the report doesn't have any entries for individual contributions in the attribute ``files``. Only the summary info
in the attribute ``summary`` is provided.


.. _ingest-trans-management-start:

Start a transaction
-------------------

Transactions are started by this service:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - ``/ingest/trans``

The following JSON object is required to be sent in the body of a request:

.. code-block::

    {   "database" : <string>,
        "context" :  <object>
    }

Where:

``database`` : *string*
  The required name of the database definintg a scope of the new transaction.

``context`` : *object* = ``{}``
  The optional arbitrary workflow-defined object to be stored in the persistet state of
  the Ingest System for the transaction. It's up to the workflow to decide what to store in the object.
  For exaqmple, this information could be used later for recovering from errors during the ingest, for
  general bookkeeping, data provenance, visualization purposes, etc. A value of this attribute, if provided,
  must be a valid JSON object. The object could be empty.

  **Note**: The current implementation of the Qserv Ingest system limits the size of the context object by **16 MB**.

In case of successfull completion of a request (see :ref:`ingest-general-error-reporting`) the service will return
the JSON object with a description of the new transaction:

.. code-block::

    {
      "databases" : {
        <database-name> :  {
          "num_chunks" :   <number>,
          "transactions" : [
            {
              "begin_time" :      <number>,
              "context" :         {...},
              "database" :        <string>,
              "end_time" :        <number>,
              "id" :              <number>,
              "log" :             [],
              "start_time" :      <number>,
              "state" :           "STARTED",
              "transition_time" : <number>
            }
          ]
        }
      },
      "success" : <number>,
      ...
      }
    }

Where the attribute ``id`` representing a unique identifier of the transaction is the most important attribute
found in the object. A alue of the identifier needs to be memorized by a workflow to be used in the subsequent
requests to the transaction management services.

The attribute ``start_time`` will be set to the current time in milliseconds since the UNIX *Epoch*.
And the state of the new transaction will be set to ``STARTED``. The ``end_time`` will be ``0``.  A value of
the attribute ``context`` will be the same as it was provided on the input to the service, or the default
value if none was provided.

.. _ingest-trans-management-end:

Commit or abort a transaction
-----------------------------

.. note::

  - The current design of the service is not correct. The ``abort`` flag should be passed in the request's query
    rather than in the body of the request. The service will be updated in the future to reflect the correct design.  


Transactions are aborted or committed by the following service:

..  list-table::
    :widths: 10 25 65
    :header-rows: 1

    * - method
      - service
      - query parameters
    * - ``PUT``
      - ``/ingest/trans/:id``
      - ``abort=<0|1>``

A unique identifier of the transaction is passed into the service in the resource's path parameter ``id``.
The only mandatory parameter of the request query is ``abort``. The value of the parameter is ``0`` to tell the services
that the transaction has to be committed normally. Any other number will be interpreted as a request to abort the transaction.

Other parameters defining a request are  passed via the request's body:

.. code-block::

    {
      "context" :  <object>
    }

Where:

``context`` : *object* = ``{}``
  The optional arbitrary workflow-defined object to be stored in the persistet state of
  the Ingest System for the transaction. It's up to the workflow to decide what to store in the object.
  For exaqmple, this information could be used later for recovering from errors during the ingest, for
  general bookkeeping, data provenance, visualization purposes, etc. A value of this attribute, if provided,
  must be a valid JSON object. The object could be empty.

  **Notes**:

  - A value provided in the attribute will replace the initial value specified (if any) at the transaction
    start time (see :ref:`ingest-trans-management-start`).
  - The current implementation of the Qserv Ingest system limits the size of the context object by **16 MB**.

Upon successful completion of either request (see :ref:`ingest-general-error-reporting`) the service would return an updated
status of the transaction in a JSON object as it was explained in the section :ref:`ingest-trans-management-start`.

State transitions of the transactions:

- Aborted transactions will end up in the ``ABORTED`` state.
- Transactions that were committed will end up in the ``FINISHED`` state.
- In case of any problems encountered during an attempt to end a transaction, other states may be also reported
  by the service.

It's also safe to repeat either of the requests. The service will complain if the transaction won't be in
the ``STARTED`` state at a time when the request was received by the service.

More information on the statuses of transactions can be found at:

- :ref:`ingest-trans-management-status`

.. _ingest-trans-management-descriptor:

Transaction descriptor
----------------------

.. note::

  This section uses a database ``gaia_edr3`` and transaction ``1630`` as an example.

The content of a JSON object returned by the services varies depending on a presense of the optional parameters:

- ``include_context={0|1}``
- ``contrib={0|1}``
- ``contrib_long={0|1}``
- ``include_log={0|1}``
- ``include_warnings={0|1}``
- ``include_retries={0|1}``

Subsections below describe the gradual expantion of the JSON object returned by the services as the optional parameters
are set to ``1``.

.. _ingest-trans-management-descriptor-short:

Shortest form
^^^^^^^^^^^^^

The shortest form of the JSON object returned by the services when all optional parameters are set to ``0`` is:

.. code-block::

    {
      "databases" : {
          "gaia_edr3" : {
            "is_published" : <0|1>,
            "num_chunks" :   <number>,
            "transactions" : [
                {
                  "id" :              1630,
                  "database" :        "gaia_edr3",
                  "begin_time" :      <number>,
                  "start_time" :      <number>,
                  "end_time" :        <number>,
                  "transition_time" : <number>,
                  "state" :           <string>,
                  "context" :         <object>,
                  "log" :             <array>
                },

Where:

``is_published`` : *number*
  The flag tells whether the database is *published* or not.

``num_chunks`` : *number*
  The total number of chunks in the database, regardless if any contributons were made into the chunks
  in a context of any transaction. Chunks need to be registered in Qserv before the corresponding MySQL tables
  can be populated with data. This information is meant to be used for the monitoring and Q&A purposes.

``id`` : *number*
  The unique identifier of the transaction.

``database`` : *string*
  The name of the database the transaction is associated with.

``begin_time`` : *number*
  The timestamp of the transaction creation in milliseconds since the UNIX *Epoch*. The value is
  set by the service when the transaction is registered in the system and assigned
  a state ``IS_STARTING``. The value is guaranteed to be not ``0``.

``start_time`` : *number*
  The timestamp of the transaction start in milliseconds since the UNIX *Epoch*. The value is
  set by the service when the transaction is started (gets into the ``STARTED`` state).
  The value is ``0`` while while teh transaction is still in a state ``IS_STARTING``.

``end_time`` : *number*
  The timestamp of the transaction end in milliseconds since the UNIX *Epoch*. The value is
  set by the service when the transaction is ended (committed, aborted or failed). A value
  of the atrribite is ``0`` if the transaction is still active.

``transition_time`` : *number*
  The timestamp of the last state transition in milliseconds since the UNIX *Epoch*. The value is
  set by the service when the transaction gets into states ``IS_FINISHING`` (the committing process
  was initiated) or ``IS_ABORTING`` (the aborting process was initiated). The value would be set
  to ``0`` before that.

``state`` : *string*
  The current state of the transaction. The possible values and their meanings are explained in
  the dedicated section:

  - :ref:`ingest-trans-management-states`

``context`` : *object*
  The object that was provided by a workflow at the transaction start time, or updated during transaction
  commit/abort time. The object could be empty. The object could be used for the recovery from errors during
  the ingest, for general bookkeeping, data provenance, visualization purposes, etc.

``log`` : *array*
  The array of log entries. Each entry is a JSON object that has the following attributes:

  - ``id`` : *number* - The unique identifier of the log entry.
  - ``transaction_state`` *string* : - The state of the transaction at the time the log entry was generated.
  - ``name`` : *string* - The name of the event that triggered the log entry.
  - ``time`` : *number* - The timestamp of the event in milliseconds since the UNIX *Epoch*.
  - ``data`` : *object* - The data associated with the event.

.. _ingest-trans-management-descriptor-contrib-summary:

With a summary of contributions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Setting the query parameters to ``contrib=1`` (regardless if ``contrib_long`` is set to ``0`` or ``1``)
will result in expaning the ``transaction`` block with the ``summary`` object. The object will
include the summary info on all contributions made in a sewcope of the transaction.

The following object illustrates the idea (where most of the previous explained attributes and all
worker-level stats but the one for ``db12`` are omitted for brevity):

.. code-block::

    "transactions" : [
        {
          "contrib" : {
              "summary" : {
                "first_contrib_begin" : 1726026383616,
                "last_contrib_end" :    1726026945059,
                "num_rows" :            223420722,
                "num_rows_loaded" :     223420722,
                "num_regular_files" :   0,
                "num_chunk_files" :     156,
                "num_failed_retries" :  0,
                "num_workers" :         9,
                "table" : {
                    "gaia_source" : {
                      "data_size_gb" :       201.872497558594,
                      "num_rows_loaded" :    217028788,
                      "num_rows" :           217028788,
                      "num_files" :          156,
                      "num_failed_retries" : 0,
                      "num_warnings" :       0
                      "overlap" : {
                          "data_size_gb" :       5.97671127319336,
                          "num_rows" :           6391934,
                          "num_rows_loaded" :    6391934,
                          "num_files" :          155,
                          "num_failed_retries" : 0,
                          "num_warnings" :       0
                      }
                    }
                },
                "worker" : {
                    "db12" : {
                      "data_size_gb" :            48.6947402954102,
                      "num_rows" :                52289369,
                      "num_rows_loaded" :         52289369,
                      "num_regular_files" :       0,
                      "num_chunk_files" :         18,
                      "num_chunk_overlap_files" : 23,
                      "num_failed_retries" :      0,
                      "num_warnings" :            0,
                    },
                }
              }

The ``summary`` object includes 3 sets of attributes:

- The general stats on the contributions made in a scope of the transaction.
- The stats on the contributions made into the table ``gaia_source`` across all workers.
- The stats on the contributions made into into tables by the worker ``db12``.

These are the general (transaction-level) stats:

``first_contrib_begin`` : *number*
  The timestamp of the first contribution in milliseconds since the UNIX *Epoch*. This is the time when a processing of the contribution started.

``last_contrib_end`` : *number*
  The timestamp of the last contribution in milliseconds since the UNIX *Epoch*. This is the time when a processing of the contribution ended.

``num_rows`` : *number*
  The total number of rows parsed in all input contributions made in a scope of the transaction.

``num_rows_loaded`` : *number*
  The total number of rows that were actually loaded into the destination table(s) in all contributions made in a scope of the transaction.

  **Note**: Normally the number of rows loaded should be equal to the number of rows parsed. If the numbers differ it means that some
  rows were rejected during the ingest process. The workflow should be always monitoring any mismatches in these values and trigger alerts.

``num_regular_files`` : *number*
  The total number of regular files (not chunk files) parsed in all input contributions.

``num_chunk_files`` : *number*
  The total number of chunk files parsed in all input contributions.

``num_failed_retries`` : *number*
  The total number of retries that failed during the ingest process.

  **Note**: In most cases it's okay that the number of failed retries is not zero. The system is designed to retry
  the ingest of the failed contributions. A problem is when the number of such failures detected in the scope of
  a single contribution exceeds a limit set at the Ingest system. The workflow should be always monitoring
  the number of failed retries and trigger alerts if the number is too high.

``num_workers`` : *number*
  The total number of workers that were involved in the ingest process.


.. _ingest-trans-management-descriptor-contrib-long:

With detailed info on contributions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Setting the query parameters to ``contrib=1`` and ``contrib_long=1`` will result in expaning the ``contrib`` object
with the ``files`` array. Each entry (JSON object) in the array represents a contribution. The objects provides
the detailed info on all contributions made in a scope of the transaction:

.. code-block::

    "transactions" : [
        {
          "contrib" : {
              "files" : [
                  <object>,
                  ...
                  <object>
              ]
          }
        }
    ]

The schema of the contribution objects is covered by:

- :ref:`ingest-worker-contrib-descriptor`

**Note**: Extended info on warnings and retries  posted during contribution loading are still disabled in this case.
To enable warnings use the parameter ``include_warnings=1``. To enable retries use the parameter ``include_retries=1``.

.. _ingest-trans-management-states:

Transaction states
------------------

Transactions have well-defined states and the state transition algorithm. Normally, Ingest System moves a transaction
from one state to another in response the explicit transaction management requests made by a workflow. In some cases
the Replication/Ingest system may also change the states.

.. image:: /_static/ingest-transaction-fsm.png
   :target: ../../../../../_images/ingest-transaction-fsm.png
   :alt: State Transition Diagram

A few comments on the diagram:

- States ``STARTED``, ``FINISHED`` and ``ABORTED`` which are shown in grey boxes are the *intended* stable states of
  a transaction. These states are *expected* to be reached by the transactin management requests.
- States ``START_FAILED``, ``FINISH_FAILED`` and ``ABORT_FAILED`` which are shown in red are the *unintended* intermediate
  stable states of a transaction. the transaction gets into these states when the system encounters problems during
  processing of the corresponding transaction management requests. Transitions into these states are shown as red dashed lines.
  The only way to get out of these states is to fix the underlying problem (could be a problem with an infrastructure, data
  or bugs in the Inges system or Qserv) and issue another transaction management request to *abort* the transaction.

  .. hint::

    - In many cases a reason of the failure is reported in the response object returned by the corresponding transaction
      management request.

- States ``IS_STARTING``, ``IS_FINISHING`` and ``IS_ABORTING`` are the *transient* unstable states which are meant to be
  passed through by a transaction on its way to the desired *intended* stable state. The states are used by the system
  to indicate a significant (and often - lengthy) transformation of the data or metadata triggered by the state transition
  of the transaction.

  - In some cases the transaction may be staying on one of these states for a while. For example, when the *commit* request
    was initiated for the transaction and if the database options specified by a workflow require the system to build
    the *director* index of the *director* table at the *commit* time of the transactions. The system will keep
    the transaction in the state ``IS_FINISHING`` until the index is built. The state will be changed to ``FINISHED``
    once the index is built successfully.

  - It's possible that a transaction may get stuck in one of these *transient* states. The only scenario when this may
    happen in the current implementation would be when the Master Replication Controller gets restarted while the transaction
    is in one of these states. The system will not be able to resume the transaction processing after the restart.
    This limitation will be addresed in the future.


The following table explains possible state transitions of a transaction:

..  list-table::
    :widths: 10 80 10
    :header-rows: 1

    * - state
      - description
      - next states
    * - ``IS_STARTING``
      - The initial (transient) state assigned to a transaction right after it's registered in the system
        in response to a request to start a transaction: :ref:`ingest-trans-management-start`.
        This transient state that should be changed to ``STARTED`` or ``START_FAILED``.
        The former state is assigned to a transaction that was successfully started, the latter
        to a transaction that failed to start.

      - | ``START``
        | ``START_FAILED``

    * - ``STARTED``
      - The active state of a transaction that is ready to accept data ingest requests.
        When the system receives a request to commit or abort the transaction (see :ref:`ingest-trans-management-end`)
        the state would transition to the corresponding transient states ``IS_FINISHING`` or ``IS_ABORTING``.
      - | ``IS_FINISHING``
        | ``IS_ABORTING``

    * - ``IS_FINISHING``
      - The transient state assigned to a transaction that is in the process of being committed.
        Depending on the database options specified by a workflow, the transaction may stay in this state
        for a while.
        The state will change to ``FINISHED`` in case of the succesfull completion of a request, or it may
        land in in the ``FINISH_FAILED`` state in case of any problems en countered during the request
        execution. A transaction may also get into the ``IS_ABORTING`` state if a workflow issues the abort
        request while the transaction is being finished.

      - | ``FINISHED``
        | ``FINISH_FAILED``
        | ``IS_ABORTING``

    * - ``IS_ABORTING``
      - The transitional state triggered by the transaction abort request (see :ref:`ingest-trans-management-end`).
      - | ``ABORTED``
        | ``ABORT_FAILED``

    * - ``FINISHED``
      - The final state of a transaction that was successfully committed.
      -

    * - ``ABORTED``
      - The final state of a transaction that was successfully aborted.
      -

    * - ``START_FAILED``
      - The (inactive) state of a transaction that failed to start. The state allows
        a workflow to initiate the transaction abort request.
      - ``IS_ABORTING``

    * - ``FINISH_FAILED``
      - The (inactive) state of a transaction that failed to to be commited. The state allows
        a workflow to initiate the transaction abort request.
      - ``IS_ABORTING``

    * - ``ABORT_FAILED``
      - The (inactive) state of a transaction that failed to to be aborted. The state allows
        a workflow to initiate another transaction abort request (or requests).
      - ``IS_ABORTING``

