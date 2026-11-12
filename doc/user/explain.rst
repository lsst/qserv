.. _query-explain:

Inspecting a query with EXPLAIN
===============================

``EXPLAIN`` runs Qserv's query analysis and chunk selection for a ``SELECT`` statement, then
submits an ``ANALYZE`` to a worker for one of the selected chunks. Use it to predict the scope
of a query (number of chunks matched, scan rating), check which spatial or director-index
restrictions apply, inspect rewritten SQL, understand scheduling classifications, and see how
a worker will execute a given query.

.. code-block:: sql

    EXPLAIN SELECT objectId FROM Object
    WHERE scisql_angSep(ra, decl, 55.5, -30.2) < 0.05

    EXPLAIN FORMAT=JSON
      SELECT filterId,
             COUNT(*) AS detections,
             AVG(psfFlux) AS meanPsfFlux
      FROM Source
      GROUP BY filterId
      ORDER BY detections DESC
      LIMIT 6

Results are returned in either a tabular format (default, ``FORMAT=TRADITIONAL``)
or as a JSON object (``FORMAT=JSON``).

The result contains these attributes:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Attribute
     - Meaning
   * - ``parsed_sql``
     - SQL representation, as interpreted by the SQL parser
   * - ``parsed_ir``
     - Qserv internal representation (IR)
   * - ``is_chunked``
     - Whether the analyzed query uses chunks.
   * - ``chunks_matched``
     - Number of chunks that matched the query; zero when coverage is empty.
   * - ``area_restrictors``
     - Spatial restrictions, or ``none``.
   * - ``secidx_restrictors``
     - Secondary index restrictions, or ``none``.
   * - ``worker_explain``
     - Result of running the worker SQL for one chunk with ``ANALYZE FORMAT=JSON``, or an
       error description. See `Worker analysis`_.
   * - ``worker_sql``
     - SQL templates that will be sent to the workers.
   * - ``needs_merge``
     - Whether a merge step is required to produce final results.
   * - ``merge_sql``
     - SQL template for the merge, or ``none``.
   * - ``scan_interactive``
     - Whether the query has been classified as interactive.
   * - ``scan_rating``
     - Query scan rating used for scheduling.
   * - ``scan_tables``
     - Shared scan information.

See :ref:`query-scan-params` for scan scheduling configuration.


Worker analysis
---------------

After initial query analysis, one relevant chunk is selected and an ``EXPLAIN`` is dispatched to its
corresponding worker. The worker runs each statement as ``ANALYZE FORMAT=JSON <statement>``, which
shows the worker's actual plan for that chunk, including index choices,
row counts examined and returned (``r_rows``), and timings (``r_total_time_ms``).

Keep in mind that the result describes a single chunk, so row counts and timings can vary across full queries.
``EXPLAIN`` needs catalog metadata and may perform director-index lookups to determine chunk
coverage. A successful worker analysis of one chunk does not guarantee that every chunk, or
the merge step, will succeed.
