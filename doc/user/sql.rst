
.. highlight:: sql

#################
Qserv SQL Dialect
#################

Introduction
============

LSST Query Services (Qserv) provides access to the LSST Database Catalogs. Users can query the catalogs using
standard SQL query language with a few restrictions described below. Why restricting it? We are intercepting
all queries, rewriting them and executing each large query as many sub-queries in parallel. Introducing these
restrictions greatly simplifies parsing incoming queries.

Our parser uses an open-source grammar built from the `SQL92 <https://en.wikipedia.org/wiki/SQL-92>`_ specification.
It does not include anything beyond SQL92, with minor exceptions (e.g., ``!=`` is allowed as well as ``<>``).

The simplest way to look at it is to treat it as a MySQL database server, modulo restrictions and extensions
described in this document.

If you run into any syntax that this Manual fails to document, please report it to the Qserv developers.

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
        ra DOUBLE                    -- spatial location (right ascension)
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

Note that as discussed in the "Restrictions" section below, spatial constraints **must** be expressed through
the ``qserv_areaspec_*`` functions.


Restrictions
============

This section covers restriction you need to be aware of when interacting with Qserv.

Spatial constraints should be expressed through our ``qserv_areaspec_*`` functions
----------------------------------------------------------------------------------

Spatial constraints should be expressed through ``qserv_areaspec_*`` functions (see Extensions section above for
details). Any other way of specifying spatial restrictions may be significantly slower (e.g., they might
devolve to be full table scan). For example, the form:

..  code-block:: sql

    WHERE ra BETWEEN <ra1> AND <ra2>
      AND decl BETWEEN <decl1> AND <decl2>

even though it is equivalent to:

..  code-block:: sql

    qserv_areaspec_box(<ra1>, <decl1>, <ra2>, <decl2>)

should not be used.


Spatial constraints must appear at the beginning of ``WHERE``
-------------------------------------------------------------

Spatial constraints must appear at the very beginning of the ``WHERE`` clause (before or after the ``objectId``
constraint, if there is any).


Only one spatial constraint is allowed per query
------------------------------------------------

Only one spatial constraint expressed through ``qserv_areaspec_*`` is allowed per query, e.g., these are examples
of invalid queries:

..  code-block:: sql

    WHERE qserv_areaspec_box(1, 35, 2, 38)
      AND qserv_areaspec_box(5, 77, 6, 78)

    WHERE qserv_areaspec_box(1, 35, 2, 38)
      AND qserv_areaspec_circle(5, 77, 0.1)


Arguments passed to spatial constraints functions must be simple literals
-------------------------------------------------------------------------

The arguments passed to the ``qserv_aresspec_*`` functions must be simple literals. They may not contain any
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

is not allowed. We expect to remove this restriction in the future, see
`DM-2888 <https://jira.lsstcorp.org/browse/DM-2888>`_.


The director index constraint must be expressed through ``=``, ``IN``, or ``BETWEEN``
-------------------------------------------------------------------------------------

If the query has objectId constraint, it should be expressed in one of these three forms:

..  code-block:: sql

    SELECT * FROM Object WHERE objectId = 123
    SELECT * FROM Object WHERE objectId IN (123, 453, 3465)
    SELECT * FROM Object WHERE objectId BETWEEN 123 AND 130

E.g., don't try to express it as ``WHERE objectId != 1``, or ``WHERE objectId > 123``, etc.

Note, we expect to allow decomposing objectId into bitfields (e.g., for sampling) in the future. See
`DM-2889 <https://jira.lsstcorp.org/browse/DM-2889>`_.


Column(s) used in ``ORDER BY`` or ``GROUP BY`` must appear in ``SELECT``
------------------------------------------------------------------------

At the moment we require columns used in ``ORDER BY`` or ``GROUP BY`` to be listed in ``SELECT``.
Example of an invalid query:

..  code-block:: sql

    SELECT x FROM  T ORDER BY y

Correct version:

..  code-block:: sql

    SELECT y, x FROM T ORDER BY y

Expressions/functions in ``ORDER BY`` clauses are not allowed
-------------------------------------------------------------

In SQL92 ``ORDER BY`` is limited to actual table columns, thus expressions or functions in ``ORDER BY`` are rejected.
This is true for Qserv too.

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
``CREATE``, ``ALTER``, ``TRUNCATE``, and ``DROP``. We will revisit this as we start adding support for Level 3.


Outer joins are not supported with near-neighbor queries
--------------------------------------------------------

Qserv does not support ``LEFT`` or ``RIGHT`` joins with near-neighbor predicates.


MySQL-specific syntax is not supported
--------------------------------------

MySQL-specific syntax is not supported. Example of unsupported syntax that will be rejected: ``NAME_CONST``.


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
      FROM Object o, Source s USING (objectId)
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


Known Bugs
==========

The list of all known / reported problems can be found at: `Data Access and Database Team User-facing
Bugs <https://jira.lsstcorp.org/issues/?filter=13501>`_.


Selecting by objectId can miss a row
------------------------------------

Selecting rows using objectId sometimes does not return rows it should.
For details, see: `DM-2864 <https://jira.lsstcorp.org/browse/DM-2864>`_.


``WHERE objectId BETWEEN`` fails
--------------------------------

As explained above, queries in the form ``WHERE objectId BETWEEN`` are discouraged. In fact, Qserv will
currently return a cryptic message when such query is executed. For details, see
`DM-2873 <https://jira.lsstcorp.org/browse/DM-2873>`_.


Notes of Performance
====================

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
