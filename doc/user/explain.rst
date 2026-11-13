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
     - Value
   * - ``parsed_sql``
     - SQL representation, as interpreted by the SQL parser
   * - ``parsed_ir``
     - Qserv internal representation (IR)
   * - ``is_chunked``
     - Whether the analyzed query uses chunks
   * - ``chunks_matched``
     - Number of chunks that matched the query; zero when coverage is empty
   * - ``area_restrictors``
     - Spatial restrictions, or ``none``
   * - ``secidx_restrictors``
     - Secondary index restrictions, or ``none``
   * - ``worker_explain``
     - Result of running the worker SQL for one chunk with ``ANALYZE FORMAT=JSON``, or an
       error description. See `Worker analysis`_.
   * - ``worker_sql``
     - SQL templates that will be sent to the workers.
   * - ``needs_merge``
     - Whether a merge step is required to produce final results
   * - ``merge_sql``
     - SQL template for the merge, or ``none``
   * - ``scan_interactive``
     - Whether the query has been classified as interactive
   * - ``scan_rating``
     - Query scan rating used for scheduling
   * - ``scan_tables``
     - Shared scan information

See :ref:`query-scan-params` for scan scheduling configuration.


Output formats
--------------

By default, ``EXPLAIN`` output is shown in tabular format (``EXPLAIN FORMAT=TRADITIONAL``).
Worker-side analysis is always provided in JSON format.

Consider the following queries:

.. code-block:: sql

    EXPLAIN SELECT COUNT(*) AS n
    FROM Object
    WHERE qserv_areaspec_box(0.5, -1, 1.5, 1)

    -- and --

    EXPLAIN FORMAT=JSON SELECT COUNT(*) AS n
    FROM Object
    WHERE qserv_areaspec_box(0.5, -1, 1.5, 1)

Their output in both formats follows.

Traditional Format
^^^^^^^^^^^^^^^^^^

The traditional format matches the table of attributes shown above. Here's a demonstration of the output
with the ``\G`` terminator to display the attributes line-by-line:

.. code-block:: text

    *************************** 1. row ***************************
    attribute: area_restrictors
        value: qserv_areaspec_box(0.5,-1,1.5,1)
    *************************** 2. row ***************************
    attribute: chunks_matched
        value: 1
    *************************** 4. row ***************************
    attribute: merge_sql
        value: SELECT SUM(`QS1_COUNT`) AS `n` FROM `qcase01`.`Object` AS `qcase01.Object`
    *************************** 7. row ***************************
    attribute: parsed_sql
        value: SELECT COUNT(*) AS `n` FROM `qcase01`.`Object` AS `qcase01.Object` WHERE scisql_s2PtInBox(`qcase01.Object`.`ra_PS`,`qcase01.Object`.`decl_PS`,0.5,-1,1.5,1)=1
    *************************** 8. row ***************************
    attribute: scan_interactive
        value: yes
    *************************** 13. row ***************************
    attribute: worker_sql
        value: SELECT COUNT(*) AS `QS1_COUNT` FROM `qcase01`.`Object_%CC%` AS `qcase01.Object` WHERE scisql_s2PtInBox(`qcase01.Object`.`ra_PS`,`qcase01.Object`.`decl_PS`,0.5,-1,1.5,1)=1

*(some rows were removed for brevity)*

JSON format
^^^^^^^^^^^

``EXPLAIN FORMAT=JSON SELECT ...`` returns a single row with one column, ``EXPLAIN``, holding a
JSON object with the same attributes. The output below has ``parsed_ir`` and ``worker_explain`` shortened:

.. code-block:: json

    {
      "area_restrictors": "qserv_areaspec_box(0.5,-1,1.5,1)",
      "chunks_matched": 1,
      "is_chunked": true,
      "merge_sql": "SELECT SUM(`QS1_COUNT`) AS `n` FROM `qcase01`.`Object` AS `qcase01.Object`",
      "needs_merge": true,
      "parsed_ir": "SelectStmt(SelectList(ValueExpr(\"n\", FactorOp(ValueFactor(query::ValueFactor::AGGFUNC, ...",
      "parsed_sql": "SELECT COUNT(*) AS `n` FROM `qcase01`.`Object` AS `qcase01.Object` WHERE scisql_s2PtInBox(`qcase01.Object`.`ra_PS`,`qcase01.Object`.`decl_PS`,0.5,-1,1.5,1)=1",
      "scan_interactive": true,
      "scan_rating": 0,
      "scan_tables": [],
      "secidx_restrictors": "none",
      "worker_explain": "{WorkerResult: explainSuccess=1 ... chunkId=7140 ... explainResult=[{\"ANALYZE\":\"{\n  \"query_optimization\": { ... },\n  \"query_block\": {\n    ...\n    \"nested_loop\": [\n      {\n        \"table\": {\n          \"table_name\": \"qcase01.Object\",\n          \"access_type\": \"ALL\",\n          ...\n          \"r_rows\": 10,\n          ...\n          \"r_filtered\": 60,\n          \"attached_condition\": \"scisql_s2PtInBox(...) = 1\"\n        }\n      }\n    ]\n  }\n}\"}] ...}",
      "worker_sql": "SELECT COUNT(*) AS `QS1_COUNT` FROM `qcase01`.`Object_%C\u0007C%` AS `qcase01.Object` WHERE scisql_s2PtInBox(`qcase01.Object`.`ra_PS`,`qcase01.Object`.`decl_PS`,0.5,-1,1.5,1)=1"
    }



Worker analysis
---------------

After initial query analysis, one relevant chunk is selected and an ``ANALYZE`` is dispatched to its
corresponding worker. The worker runs each statement as ``ANALYZE FORMAT=JSON <statement>``, which
shows the worker's actual plan for that chunk, including index choices,
row counts examined and returned (``r_rows``), and timings (``r_total_time_ms``).

The fields in this JSON output vary depending on the plan the worker chooses. For example, an optimized
lookup may report only a ``message`` explaining the optimization, while a table scan reports
``access_type``, ``r_rows``, and ``r_filtered``. See the `MariaDB documentation
<https://mariadb.com/kb/en/analyze-format-json/>`_ for a description of
each field.

Keep in mind that the result describes a single chunk, so row counts and timings can vary across full queries.
``EXPLAIN`` needs catalog metadata and may perform director-index lookups to determine chunk
coverage. A successful worker analysis of one chunk does not guarantee that every chunk, or
the merge step, will succeed.


Worked Example: Optimizing a Source query
-----------------------------------------

Suppose you are interested in a particular ``sourceId``:

.. code-block:: sql

    SELECT *
    FROM Source
    WHERE sourceId = 29763859300222250

While this query is correct, ``sourceId`` is not a director key, so Qserv doesn't know which chunk
holds the relevant row. The query will be dispatched to all chunks. ``EXPLAIN`` gives us more
context on the query planning decisions that were made. Here's the output from running
the query above with ``EXPLAIN`` on a small test dataset that contains 14 chunks, ``qcase01``:

.. code-block:: text

    attribute           value
    ------------------  ----------------------------------------------------------
    chunks_matched      14
    secidx_restrictors  none
    scan_interactive    no
    scan_rating         1
    scan_tables         [{"db": "qcase01", "table": "Source", "rating": 1, ...}]
    worker_explain      {WorkerResult: explainSuccess=1 ... chunkId=7054 ...
                          "table": {"message": "no matching row in const table"} ...}

*(some output rows have been removed for brevity)*

Here's how we can interpret the output:

* ``secidx_restrictors`` is ``none`` and ``chunks_matched`` is 14: Qserv was not able to
  reduce the search space for this query and will need to dispatch it to all 14 chunks of this
  test dataset. In a production setting, the scope would be much larger.
* ``scan_interactive`` is ``no`` and ``scan_tables`` lists ``Source``: this is a *shared scan*
  and will be scheduled with other full-catalog queries rather than in the
  interactive queue's "fast lane."
* ``worker_explain`` analyzed a sample chunk that did not hold a matching row. This will be the case
  for most of the chunks involved in this query, since they don't actually hold relevant data.

The main takeaway here is that Qserv is not able to easily pinpoint the location of the row we're
interested in, and the ``EXPLAIN`` output underscores the broad scope of the work required to resolve
the query. **However**, consider the case where we also know the ``objectId`` the source belongs to:

.. code-block:: sql

    SELECT *
    FROM Source
    WHERE sourceId = 29763859300222250
      AND objectId = 386942193651348

Here, ``objectId`` **IS** a director key, which allows Qserv to look up the exact chunk that holds the
object and source. We can confirm this with ``EXPLAIN``:

.. code-block:: text

    attribute           value
    ------------------  ----------------------------------------------------------
    chunks_matched      1
    secidx_restrictors  `qcase01`.`Object`.`objectId`=386942193651348
    scan_interactive    yes
    scan_rating         0
    scan_tables         []
    worker_explain      {WorkerResult: explainSuccess=1 ... chunkId=6630 ...
                          "access_type": "ALL", "r_rows": 110,
                          "r_filtered": 0.909090909, ...}

We're now searching only 1 chunk (restricted by ``objectId``), and our query runs in the
interactive queue "fast lane." The detailed ``worker_explain`` shows the remaining cost of the query:
the worker reads the entire chunk (``access_type: ALL`` indicates a full table scan) to find the
row we were interested in.

Both of the queries above return the same single result, but the second option is much more efficient.

General Qserv EXPLAIN Tips
--------------------------

1. Check that ``chunks_matched`` is reasonable for the query you are submitting. A lookup that should
   return only a few rows should ideally only need to match a small number of chunks.
2. If the number of matched chunks seems too high, consider restricting your query, and confirm that
   restrictions you added are included in the ``EXPLAIN`` output. Director keys will appear in the
   ``secidx_restrictors`` row, and spatial regions will appear in ``area_restrictors``
   (see "Spatial filtering and chunk selection" in the :doc:`SQL guide <sql>`).
3. Use ``worker_explain`` to estimate the cost of executing the query on the worker, but keep in mind that
   it samples from a single chunk.
