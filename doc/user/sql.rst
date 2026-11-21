
.. highlight:: sql

#################
Qserv SQL Dialect
#################

Introduction
============

LSST Query Services (Qserv) provides access to the LSST Database Catalogs. Users can query the catalogs using
standard SQL query language with a few restrictions described below. These restrictions make it possible for
Qserv to rewrite general SQL queries into distributed equivalents that operate in parallel over a cluster of
compute resources.

Qserv supports a subset of SQL with MySQL/MariaDB syntax. A query must also be executable using
the catalog's partitioning and Qserv's distributed execution rules; accepting a query's syntax
does not guarantee that Qserv can execute it. The simplest way to look at it is to treat it as a MySQL database
server, modulo restrictions and extensions described in this document.

If you run into any syntax that this manual fails to document, please report it to the Qserv developers.

Selected Design Aspects
=======================

Partitioning and Sharding
-------------------------

Qserv is designed to handle large volumes of data that can be partitioned in one or more dimensions (e.g., by spatial locality).
Once the partitioning columns and parameters (such as partition size) are chosen, the data is directed to appropriate
partitions ("chunks"), which are then distributed (sharded) across the nodes in the cluster.

In this scheme, a single large table typically consists of many chunks (potentially hundreds of thousands). This facilitates
running full-table-scan queries, as each query can be executed in parallel across multiple chunks. For example, if we have
an Object table split into ``12345`` chunks, Qserv will execute:

..  code-block:: sql

   SELECT * FROM Object

as ``12345`` worker-side queries processed in parallel:

..  code-block:: sql

   SELECT * FROM Object_00001
   SELECT * FROM Object_00002
   ...
   SELECT * FROM Object_12345


Director Table
--------------

Often, multiple large tables need to be partitioned and joined together. To ensure joining such tables is
possible without sending lots of information between nodes, Qserv has a way to ensure all related chunks
always end up on the same machine. To enable that, Qserv has a notion of a "Director Table", which "drives"
partitioning. For example, consider two tables:

..  code-block:: sql

    CREATE TABLE Object (
        objectId BIGINT PRIMARY KEY, -- unique identifier
        ra DOUBLE,                   -- spatial location (right ascension)
        decl DOUBLE                  -- spatial location (declination)
    )

which contains information about astronomical objects (galaxies, stars), one row = one object, and:

..  code-block:: sql

    CREATE TABLE Source (
        sourceId BIGINT PRIMARY KEY, -- unique identifier
        objectId BIGINT,             -- pointer to corresponding object
                                     -- Note, there maybe many sources per object
        ra DOUBLE,                   -- spatial location (right ascension)
        decl DOUBLE                  -- spatial location (declination)
    )

which contains information about individual detections of astronomical objects, one row representing one detection of one object.

Note that astronomical objects tend to move, so individual detections of the same object might have different ``ra`` / ``decl``
positions than the "average" location represented by the ra/decl of their corresponding object.

If we select the Object table to be the "Director Table", not only will the Object table be partitioned according to
its ``ra`` / ``decl`` values, but more importantly, the Source table will be partitioned based on the ``ra`` / ``decl``
of corresponding objects.


The Director Index
------------------

The sharding scheme described above has a problem with locating data by objectId. To alleviate this, Qserv maintains a specialized
index that maps a primary key of the director table to a chunkId of the chunk that contains a given row. Consider the query:

..  code-block:: sql

    SELECT * FROM Object WHERE objectId = <id>

behind the scene, it will be executed as:

..  code-block:: sql

    SELECT chunkId FROM IdToChunkMapping WHERE objectId = <id>

which is a quick index lookup, followed by:

..  code-block:: sql

    SELECT * FROM Object_<chunkId> WHERE objectId = <id>

which is another quick index lookup inside one small chunk.

Note that the use of the *director* index has some restrictions, as explained in the restrictions section below.

By the way, do not attempt to issue queries directly on our internal chunk tables. It is blocked.


Extensions
==========

This section covers extensions to sql which we introduced.

Spatial Constraints
-------------------

Spatial constraints in Qserv can be expressed using one of the functions we introduced. Currently supported:

..  code-block:: sql

    qserv_areaspec_box(
        lonMin  DOUBLE,  -- [deg] Minimum longitude angle
        latMin  DOUBLE,  -- [deg] Minimum latitude angle
        lonMax  DOUBLE,  -- [deg] Maximum longitude angle
        latMax  DOUBLE   -- [deg] Maximum latitude angle
    )

..  code-block:: sql

    qserv_areaspec_circle(
        lon     DOUBLE,  -- [deg] Circle center longitude
        lat     DOUBLE,  -- [deg] Circle center latitude
        radius  DOUBLE   -- [deg] Circle radius
    )

..  code-block:: sql

    qserv_areaspec_ellipse(
        lon                 DOUBLE,  -- [deg] Ellipse center longitude
        lat                 DOUBLE,  -- [deg] Ellipse center latitude
        semiMajorAxisAngle  DOUBLE,  -- [arcsec] Semi-major axis length
        semiMinorAxisAngle  DOUBLE,  -- [arcsec] Semi-minor axis length
        positionAngle       DOUBLE   -- [deg] Ellipse position angle, east of north
    )

..  code-block:: sql

    qserv_areaspec_poly(
        v1Lon  DOUBLE,  -- [deg] Longitude angle of first polygon vertex
        v1Lat  DOUBLE,  -- [deg] Latitude angle of first polygon vertex
        v2Lon  DOUBLE,  -- [deg] Longitude angle of second polygon vertex
        v2Lat  DOUBLE,  -- [deg] Latitude angle of second polygon vertex
        ...
    )

Example:

..  code-block:: sql

    SELECT objectId FROM Object WHERE qserv_areaspec_box(0, 0, 3, 10)

Qserv can also identify spatial constraints written with SciSQL functions. For example, if
``ra`` and ``decl`` are the table's configured partitioning coordinates, either of these
queries allows Qserv to restrict the chunks it searches:

.. code-block:: sql

    SELECT objectId FROM Object
    WHERE scisql_angSep(ra, decl, 55.5, -30.2) < 0.05

    SELECT objectId FROM Object
    WHERE scisql_s2PtInCircle(ra, decl, 55.5, -30.2, 0.05) = 1

Coordinates and the angular radius in these examples are in degrees. The center and radius
must be numeric constants. The coordinate columns must match the table's configured partitioning columns.
A filter on other coordinates will work, but will not apply chunk pruning optimizations.

For SciSQL area detection, put a single region predicate in the top-level ``AND``
conditions of ``WHERE``. Negated predicates, predicates inside ``OR``, and multiple detected
SciSQL region predicates do not provide this single-region optimization. Use :ref:`query-explain`
to check ``area_restrictors`` and ``chunks_matched`` rather than assuming that a spatial
expression reduces chunk coverage.

Aggregates and expressions
--------------------------

Qserv supports ``COUNT``, ``SUM``, ``AVG``, ``MIN``, ``MAX``, and the integer bitwise aggregates
``BIT_AND``, ``BIT_OR``, and ``BIT_XOR`` across chunks. Scalar functions and arithmetic can
surround aggregates, and aggregate arguments can themselves be expressions. Both can be nested
to any depth. For example:

.. code-block:: sql

    SELECT ROUND(AVG(flux), 2) AS meanFlux,
           MAX(flux) - MIN(flux) AS fluxRange,
           ROUND(100.0 * COUNT(flux) / COUNT(*), 1) AS pctWithFlux,
           MAX(ABS(decl)) AS maxAbsDecl,
           BIT_OR(flags & 255) AS combinedFlags
    FROM Object

These expressions are evaluated using the combined results from all selected chunks.

Outside the aggregate, the surrounding expression can use other aggregates and constants, but
not columns. For example, ``MAX(flux) - flux`` and ``COUNT(*) + objectId`` are rejected.
In that situation, select the column and the aggregate as separate items instead.

``DISTINCT`` inside aggregate arguments (for example, ``COUNT(DISTINCT objectId)``) is not
supported. An aggregate cannot contain another aggregate (for example, ``MAX(SUM(flux))``), and aggregates
cannot appear in ``WHERE``. For grouped filtering, use ``HAVING`` and include the aggregate
expression in the select list, preferably with an alias:

.. code-block:: sql

    SELECT objectId, COUNT(*) AS n
    FROM Source
    GROUP BY objectId
    HAVING n > 1

An aggregate expression in ``HAVING`` must match an entry or alias in the select list.

Spatial joins
-------------

Near-neighbor joins can use either ``scisql_angSep(...) < radius`` or
``scisql_s2PtInCircle(..., radius) = 1``. For example:

.. code-block:: sql

    SELECT a.objectId AS id1, b.objectId AS id2
    FROM Object AS a JOIN Object AS b
      ON scisql_s2PtInCircle(a.ra, a.decl, b.ra, b.decl, 0.01) = 1

The radius is a constant in degrees, and a radius larger than the available overlap cannot be
used to request an arbitrary cross-chunk join.

Ref-match joins across catalogs
-------------------------------

A registered ref-match table can connect directors in different catalogs. Qualify each table
with its database name when necessary, and join on the configured director keys. For example,
for a match table with columns ``objectId`` and ``deepSourceId``:

.. code-block:: sql

    SELECT o.objectId, d.deepSourceId
    FROM CatalogA.RefMatch AS m
    JOIN CatalogA.Object AS o ON m.objectId = o.objectId
    JOIN CatalogB.DeepSource AS d ON m.deepSourceId = d.deepSourceId

The catalogs must belong to the same database family.


Restrictions
============

Spatial filtering and chunk selection
-------------------------------------

Use ``qserv_areaspec_*`` or a recognized SciSQL spatial predicate when you want Qserv to select
chunks by region. Comparisons such as ``ra BETWEEN ... AND ...`` combined with a declination
range filter rows, but do not themselves provide spatial chunk selection.

``qserv_areaspec_*`` constraints may occur anywhere among the ``AND`` conditions in ``WHERE``.
Multiple such constraints are allowed and their row filters are combined conjunctively.
Chunk selection can conservatively include chunks covering any of those regions. In a join, these
constraints apply to the configured spatial columns of each partitioned table; a SciSQL
predicate explicitly names the table and columns to which its row filter applies.

Arguments passed to spatial constraints functions must be simple literals
-------------------------------------------------------------------------

The arguments passed to the ``qserv_areaspec_*`` functions must be simple literals. They may not contain any
references, e.g. may not refer to columns.

Example of an invalid entry:

..  code-block:: sql

    WHERE qserv_areaspec_box(3+4, ra*2, 0, 0)


``OR`` is not allowed after ``qserv_areaspec_*`` constraint
-----------------------------------------------------------

If the query has extra constraints after the ``qserv_areaspec_*`` constraint, ``OR`` is not allowed immediately after
``qserv_areaspec_*,`` for example:

..  code-block:: sql

    SELECT objectId, ra, decl, x
     FROM  Object
     WHERE qserv_areaspec_box(1, 35, 2, 38) AND x > 3.5

is valid, but:

..  code-block:: sql

    SELECT objectId, ra, decl, x
     FROM  Object
     WHERE qserv_areaspec_box(1, 35, 2, 38) OR x > 3.5

is not allowed. A ``qserv_areaspec_*`` constraint cannot occur under ``OR`` or ``NOT``.
Other predicates may use ``OR``, for example ``qserv_areaspec_box(1, 35, 2, 38) AND (x > 3.5 OR x < 0)``.


Director index constraints must be expressed through ``=`` or ``IN``
-----------------------------------------------------------------------

If the query has an objectId constraint, express it in one of these two forms so that Qserv can
use the director index to select only the chunks that hold those objects:

..  code-block:: sql

    SELECT * FROM Object WHERE objectId = 123
    SELECT * FROM Object WHERE objectId IN (123, 453, 3465)

The values must be literals, and the constraint must be one of the top-level ``AND`` conditions
of ``WHERE``. To select several objects, use ``IN`` rather than combining ``=`` comparisons with
``OR``.


Column(s) used in ``ORDER BY`` or ``GROUP BY`` must appear in ``SELECT``
------------------------------------------------------------------------

At the moment we require columns used in ``ORDER BY`` or ``GROUP BY`` to be listed in ``SELECT``.
Example of an invalid query:

..  code-block:: sql

    SELECT x FROM  T ORDER BY y

Correct version:

..  code-block:: sql

    SELECT y, x FROM T ORDER BY y

Functions in ``ORDER BY`` clauses are not allowed
-------------------------------------------------------------

Qserv rejects functions written directly in ``ORDER BY``. Select the expression under an alias
and order by that alias instead.

Example of an invalid use of the ``ORDER BY`` clause:

..  code-block:: sql

    SELECT id, ABS(x) FROM Source ORDER BY ABS(x)

However, one can bypass this by using an alias, for example:

..  code-block:: sql

    SELECT id, ABS(x) as ax FROM Source ORDER BY ax

Sub-queries are NOT supported
-----------------------------

Sub queries are not supported.


Commands that modify tables are disallowed
------------------------------------------

Commands for creating or modifying tables are disabled. These commands include ``INSERT``, ``UPDATE``, ``LOAD INTO``,
``CREATE``, ``ALTER``, ``TRUNCATE``, and ``DROP`` through the SQL query interface.
To upload and manage user tables, see :ref:`http-frontend-ingest`.


Outer joins are not supported with near-neighbor queries
--------------------------------------------------------

Qserv does not support ``LEFT`` or ``RIGHT`` joins with near-neighbor predicates.


Repeated column names through ``*`` are not supported
-----------------------------------------------------

Queries with a ``*`` that resolves to repeated column name are not supported. Example:

..  code-block:: sql

    SELECT *, id FROM Object

will fail if the table Object has a column called ``id``. Similarly, this query will fail:

..  code-block:: sql

    SELECT o.*, s.* FROM Object AS o, Source AS s

if both tables Object and Source have a column called ``id``.

A workaround would be to select columns explicitly and alias them, e.g. :

..  code-block:: sql

    SELECT o.id AS oId, s.id AS sId FROM Object AS o, Source AS s

``USE INDEX()`` is not supported
--------------------------------

Qserv will reject query with ``USE INDEX`` hint.

Variables are not supported
---------------------------

You can't select into a variable. For example:

..  code-block:: sql

    SELECT scisql_s2CPolyToBin(...) FROM T INTO @poly

will fail. Related story is at `DM-2874 <https://jira.lsstcorp.org/browse/DM-2874>`_.

User Defined Functions
======================

Qserv installation always comes with a set of predefined user defined functions:

- spherical geometry aimed to allow quick answers to the following sorts of questions:

  - Which points in a table lie inside a region on the sphere? For example, an astronomer might wish to know which stars and
    galaxies lie inside the region of the sky observed by a single camera CCD.
  - Which spherical regions in a table contain a particular point? For example, an astronomer might wish to know which telescope images
    overlap the position of interesting object X

- photometry, aimed to provide conversions between raw fluxes, calibrated (AB) fluxes and AB magnitudes.

For details, see `Science Tools for MySQL <https://www.slac.stanford.edu/exp/lsst/qserv/scisql/>`_.


Example Queries Supported
=========================

Counts and simple selections
----------------------------

You can count objects and run simple selections. Few examples:


Count the number of rows in a table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

..  code-block:: sql

    SELECT COUNT(*) FROM Object


Find rows with a particular ``id``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

..  code-block:: sql

    SELECT * FROM Object WHERE objectId = <id>

Select rows in a given area
^^^^^^^^^^^^^^^^^^^^^^^^^^^

..  code-block:: sql

    SELECT objectId FROM Object
     WHERE qserv_areaspec_box(1, 35, 2, 38)


Select rows in a given area meeting certain criteria
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

..  code-block:: sql

    SELECT COUNT(*) FROM Object
     WHERE qserv_areaspec_box(0.1, -6, 4, 6)
       AND x = 3.4
       AND y BETWEEN 1 AND 2

Joins
-----

Join two tables
^^^^^^^^^^^^^^^

..  code-block:: sql

    SELECT s.ra, s.decl, o.raRange, o.declRange
      FROM Object o, Source s
     WHERE o.objectId = <id>
       AND o.objectId = s.objectId

or:

..  code-block:: sql

    SELECT s.ra, s.decl, o.raRange, o.declRange
      FROM Object o JOIN Source s USING (objectId)
     WHERE o.objectId = <id>


Find near neighbors in a given region
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

..  code-block:: sql

    SELECT o1.objectId AS objId1,
           o2.objectId AS objId2,
           scisql_angSep(o1.ra_PS, 
                         o1.decl_PS,
                         o2.ra_PS,
                         o2.decl_PS) AS distance
      FROM Object o1, Object o2
     WHERE qserv_areaspec_box(0, 0, 0.2, 1)
       AND scisql_angSep(o1.ra_PS,
                         o1.decl_PS,
                         o2.ra_PS,
                         o2.decl_PS) < 0.05
       AND o1.objectId <> o2.objectId


``LIMIT``, ``ORDER BY``
-----------------------

Limit results, sort results
^^^^^^^^^^^^^^^^^^^^^^^^^^^

..  code-block:: sql

    SELECT * FROM Object
     WHERE x > 4
     ORDER BY x
     LIMIT 100


Performance Notes
=================

Use objectId when selecting sources
-----------------------------------

If you need to locate a small number of sources, try to use objectId if you can. If you don't, your query will
require an index scan for every chunk of the Source table (which can potentially mean thousands of
chunk-queries). For example this query will require it:

..  code-block:: sql

    SELECT * FROM Source WHERE sourceId = 500

but asking for sources related to a given object, like this one:

..  code-block:: sql

    SELECT * FROM Source WHERE objectId = 123 AND sourceId = 500

will require an index scan for just a single chunk, and thus will typically be much faster.
