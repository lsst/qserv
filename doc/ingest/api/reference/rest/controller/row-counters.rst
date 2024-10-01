
Row counters
============

.. _ingest-row-counters-deploy:

Collecting row counters and deploying them at Qserv
---------------------------------------------------

The service collects row counters in the specified table and (optionally) deploys the counters
in Qserv to allow optimizations of the relevant queries. The database may or may not be in
the published state at the time of this operation.

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``POST``
      - ``/ingest/table-stats``

Where the request object has the following schema:

.. code-block::

    {   "database" :                         <string>,
        "table" :                            <string>,
        "overlap_selector" :                 <string>,
        "force_rescan" :                     <number>,
        "row_counters_state_update_policy" : <string>,
        "row_counters_deploy_at_qserv" :     <number>
    }

Where:

``database`` : *string*
  The required name of a database affected by the operation.

``table`` : *string*
  The required name of the table for which the row counters are required to be collected.

``overlap_selector`` : *string*  = ``CHUNK_AND_OVERLAP``
  The optional selector for a flavor of the table for which the counters will be collected.
  Possible options are:

  - ``CHUNK_AND_OVERLAP``: Both the chunk table itself and the overlap table.
  - ``CHUNK``:             Only the chunk table.
  - ``OVERLAP``:           Only the overlap table.

  **Note**: This parameter applies to the *partitioned* tables only. It's ignored for the *regular* (fully replicated)
  tables.

``force_rescan`` : *number*  = ``0``
  The optional flag that tells the service to rescan the counters that were recorded earlier.
  If the value is set to ``0`` the service will not rescan the counters if the previous version already exists.
  If the value is set to ``1`` (or any other number) the service will rescan the counters regardless of
  the previous version.

``row_counters_state_update_policy`` : *string*  = ``DISABLED``
  The optional parameter that drives the counters update policy within the persistent
  state of the Replication/Ingest system. These are the possible options:

  - ``DISABLED``: The service will collect the counters but it will not update the persistent state.
  - ``ENABLED``:  Update the counters in the system if the scan was successful and if no counters were
    recorded earlier. 
  - ``FORCED``:   Same as ``ENABLED`` except it allows overriding the previous state of the counters.

``row_counters_deploy_at_qserv`` : *number*  = ``0``
  The optional flag tells the service if the counters should be deployed at Qserv.
  If the value is set to ``0`` the service will not deploy the counters. Any other value would tell
  the service to drop the previous version of the counters (if any existed) in Qserv and update the counters.

.. _ingest-row-counters-delete:

Deleting row counters for a table
----------------------------------

The service removes the previously collected row counters of the specified table from Qserv and (optionally if requested)
from the Replication system's persistent state. The database may or may not be published at the time of this operation:

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``DELETE``
      - ``/ingest/table-stats/:database/:table``

Where the service path has the following parameters:

``database`` : *string*
  The name of a database affected by the operation.

``table`` : *string*
  The name of the table for which the row counters are required to be collected.

The request object sent in the JSON body has the following schema:

.. code-block::

    {   "overlap_selector" : <string>,
        "qserv_only" :       <number>
    }

Where:

``overlap_selector`` : *string* = ``CHUNK_AND_OVERLAP``
  The optional selector for a flavor of the table for which the counters will be collected.
  Possible options are:

  - ``CHUNK_AND_OVERLAP``: Both the chunk table itself and the overlap table.
  - ``CHUNK``:             Only the chunk table.
  - ``OVERLAP``:           Only the overlap table.

  **Note**: This parameter applies to the *partitioned* tables only. It's ignored for the *regular* (fully replicated)
  tables.

``qserv_only`` : *number* = ``0``
  The optional flag tells the service if the counters should be removed
  from Qserv and from the Replication system's persistent state as well:
  
  - ``0``: Remove the counters from both Qserv and the Replication system's persistent state.
  - ``1`` (or any other number which is not ``0``): Remove the counters only from Qserv.

.. _ingest-row-counters-inspect:

Inspecting rows counters of a table
-----------------------------------

The service retturns a status of the previously collected (if any) row counters of the specified table from
the Replication system's persistent state. The database may or may not be published at the time of this operation.

..  list-table::
    :widths: 10 90
    :header-rows: 0

    * - ``GET``
      - ``/ingest/table-stats/:database/:table``

Where:

``database`` : *string*
  The name of a database affected by the operation.

``table`` : *string*
  The name of the table for which the row counters are required to be collected.

The response returned by the service has the following JSON schema:

.. code-block::

    {   "database" : <string>,
        "table" :    <string>,
        "entries": [
            {   "transaction_id" : <number>,
                "chunk" :          <number>,
                "is_overlap" :     <number>,
                "num_rows" :       <number>,
                "update_time" :    <number>
            },
            ...
        ]
    }

Where:

``database`` : *string*
  The name of a database that was specified in the service resource path.

``table`` : *string*
  The name of the table was specified in the service resource path.

``entries`` : *array*
  The array of the collected row counters entries.
    
``transaction_id`` : *number*
  The unique identifier of a *super-transaction*.

``chunk`` : *number*
  The chunk number of the entry.

  **Note**: A value of ``0`` will be reported for the *regular* (fully-replicated) tables.

``is_overlap`` : *number*
  The flag indicates if the entry is reported for the chunk overlap (a value would differ from ``0``)
  rather than for the chunk itself (a value would be ``0``).

  **Note**: The parameter should be ignored for the *regular* (fully-replicated) tables.

``num_rows`` : *number*
  The number of rows in in a scope of (``transaction_id``, ``chunk``, ``is_overlap``).

``update_time`` : *number*
  The last time the counter was collected. The time is given as the number of milliseconds since
  the UNIX *Epoch* time.