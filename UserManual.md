Qserv - User Manual
===================

Introduction
------------

LSST Query Services (Qserv) provides access to the LSST Database Catalogs. Users can query the catalogs using standard SQL query language with a few restrictions described below. Why restricting it? We are intercepting all queries, rewriting them and executing each large query as many sub-queries in parallel. Introducing these restrictions greatly simplifies parsing incoming queries.

Our parser uses an open-source grammar built from the SQL92 specification. It does not include anything beyond SQL92, with minor exceptions (e.g., != is allowed as well as <>).

The simplest way to look at it is to treat it as a MySQL database server, modulo restrictions and extensions described in this document.

If you run into any syntax that this Manual fails to document, please report it through the Qserv mailing list.


Selected Design Aspects
-----------------------

### Partitioning and Sharding

Qserv has been designed to handle large volumes of data that can be partitioned in one or more dimensions (for example, by spacial locality). Once the partitioning column(s) are selected and partitioning parameters (such as partition size) are chosen, loaded data gets directed to appropriate partitions ("chunks"), and chunks are distributed (sharded) across nodes available in the cluster.

In such scheme, a single large table typically consists of many chunks (it could even be tens of thousands of chunks). This helps with running full-table-scan queries, as each such query can be executed in pieces, in parallel. For example, imagine we have an Object table that we split into x chunks. Then Qserv will execute:

    SELECT * from Object

as:

    SELECT * from Object_00001
    SELECT * from Object_00002
    ...
    SELECT * from Object_x

in parallel.


### Director Table

Often, multiple large tables need to be partitioned and joined together. To ensure joining such tables is possible without sending lots of information between nodes, Qserv has a way to ensure all related chunks always end up on the same machine. To enable that, Qserv has a notion of "Director Table", which "drives" partitioning. For example, consider two tables:

    TABLE Object (
        objectId BIGINT PRIMARY KEY, # unique identifier
        ra DOUBLE,                   # spatial location (right ascension)
        decl DOUBLE                  # spatial location (declination)
    )

which contains information about astronomical objects (galaxies, stars), one row = one object, and:

    TABLE Source (
        sourceId BIGINT PRIMARY KEY, # unique identifier
        objectId BIGINT,             # pointer to corresponding object
                                     # Note, there maybe many sources per object
        ra DOUBLE                    # spatial location (right ascension)
        decl DOUBLE                  # spatial location (declination)
    )

which contains information about individual detections of astronomical objects, one row = one detection of one object.

Note that astronomical objects tend to move, so individual detections of the same object might have different ra/decl positions than the "average" location represented by the ra/decl of their corresponding object.

If we elect the Object table to be the "Director Table", not only the Object table will be partitioned according to its ra/decl values, but more importantly, Source table will be partitioned based on the ra/decl of corresponding objects.


### Secondary Index

The sharding scheme described above has a problem with locating data by objectId. To alleviate this, Qserv maintains a specialized index that maps a primary key of the director table to a chunkId of the chunk that contains a given row. Consider a query:

    SELECT * from Object WHERE objectId = <id>

behind the scene, it willbe executed as:

    SELECT chunkId FROM IdToChunkMapping WHERE objectId = <id>

which is a quick index lookup, followed by

    SELECT * from Object_<chunkId> WHERE objectId = <id>

which is another quick index lookup inside one small chunk.

By the way, do not attempt to issues queries directly on our internal chunk tables. It is blocked.


Extensions
----------

This section covers extensions to sql which we introduced.


### Spatial Constraints

Spatial constraints in Qserv can be expressed using one of the functions we introduced. Currently supported:

    qserv_areaspec_box(
        lonMin               DOUBLE PRECISION,  # [deg]    Minimum longitude angle
        latMin               DOUBLE PRECISION,  # [deg]    Minimum latitude angle
        lonMax               DOUBLE PRECISION,  # [deg]    Maximum longitude angle
        latMax               DOUBLE PRECISION   # [deg]    Maximum latitude angle
    )

    qserv_areaspec_circle(
        lon                  DOUBLE PRECISION,  # [deg]    Circle center longitude
        lat                  DOUBLE PRECISION,  # [deg]    Circle center latitude
        radius               DOUBLE PRECISION   # [deg]    Circle radius
    )

    qserv_areaspec_ellipse(
        semiMajorAxisAngle   DOUBLE PRECISION,  # [arcsec] Semi-major axis length
        semiMinorAxisAngle   DOUBLE PRECISION,  # [arcsec] Semi-minor axis length
        positionAngle        DOUBLE PRECISION   # [deg]    Ellipse position angle, east of north
    )

    qserv_areaspec_poly(
        v1Lon                DOUBLE PRECISION,  # [deg]    Longitude angle of first polygon vertex
        v1Lat                DOUBLE PRECISION,  # [deg]    Latitude angle of first polygon vertex
        v2Lon                DOUBLE PRECISION,  # [deg]    Longitude angle of second polygon vertex
        v2Lat                DOUBLE PRECISION,  # [deg]    Latitude angle of second polygon vertex
     ...
    )

Example:

    SELECT objectId
    FROM   Object
    WHERE  qserv_areaspec_box(0, 0, 3, 10)

Note that as discussed in the "Restrictions" section below, spatial constraints **must** be expressed through the qserv_areaspec_* functions.


Restrictions
------------

This section covers restriction you need to be aware of when interacting with Qserv.


### Spatial constraints should be expressed through our qserv_areaspec_* functions

Spatial constraints should be expressed through qserv_areaspec_* functions (see Extensions section above for details). Any other way of specifying spatial restrictions may be significantly slower (e.g., they might devolve to be full table scan). For example, the form:

    WHERE ra BETWEEN <ra1> AND <ra2>
      AND decl BETWEEN <decl1> AND <decl2>

even though it is equivalent to:

    qserv_areaspec_box(<ra1>, <decl1>, <ra2>, <decl2>)

should not be used.


### Spatial constraints must appear at the beginning of WHERE

Spatial constraint must appear at the very beginning of the WHERE clause (before or after the objectId constraint, if there is any).


### Only one spatial constraint is allowed per query

Only one spatial constraint expressed through qserv_areaspec_* is allowed per query, e.g., these are examples of invalid queries:

    WHERE qserv_areaspec_box(1, 35, 2, 38)
      AND qserv_areaspec_box(5, 77, 6, 78)

or

    WHERE qserv_areaspec_box(1, 35, 2, 38)
      AND qserv_areaspec_circle(5, 77, 0.1)


### Arguments passed to spatial constraints functions must be simple literals

The arguments passed to the qserv_aresspec_ functions must be simple literals. They may not contain any references, e.g. may not refer to columns.

Example of an invalid entry:

    WHERE qserv_areaspec_box(3+4, ra*2, 0, 0)


### OR is not allowed after qserv_areaspec_* constraint

If the query has extra constraints after the qserv_areaspec_* constraint, OR is not allowed immediately after qserv_areaspec_*, for example:

    SELECT objectId, ra, decl, x
    FROM   Object
    WHERE  qserv_areaspec_box(1, 35, 2, 38)
    AND    x > 3.5

is valid, but

    SELECT objectId, ra, decl, x
    FROM   Object
    WHERE  qserv_areaspec_box(1, 35, 2, 38)
    OR     x > 3.5

is not allowed. We expect to remove this restriction in the future, see [DM-2888](https://jira.lsstcorp.org/browse/DM-2888).


### objectId constraint must be expressed through "=" on "IN"

If the query has objectId constraint, it should be expressed in one of these two forms:

    SELECT * FROM Object WHERE objectId = 123

    SELECT * FROM Object WHERE objectId IN (123, 453, 3465)

E.g., don't try to express it as "WHERE objectId BETWEEN 1 AND 2" etc.

Note, we expect to allow decomposing objectId into bitfields (e.g., for sampling) in the future. See [DM-2889](https://jira.lsstcorp.org/browse/DM-2889).


### Column(s) used in ORDER BY must appear in SELECT

At the moment we require columns used in ORDER BY to be listed in SELECT. Example of an invalid query:

    SELECT x
    FROM   T
    ORDER BY y


### Expressions/functions in ORDER BY clauses are not allowed

In SQL92 ORDER BY is limited to actual table columns, thus expressions or functions in ORDER BY are rejected. This is true for Qserv too.

Example of an invalid ORDER BY:

    SELECT id, ABS(x)
    FROM   Source
    ORDER BY ABS(x)

However, one can bypass this by using an alias, for example:

    SELECT id, ABS(x) as ax
    FROM   Source
    ORDER BY ax


### Sub-queries are NOT supported

Sub queries are not supported.


### Commands that modify tables are disallowed

Commands for creating or modifying tables are disabled. These commands include "INSERT, UPDATE, LOAD INTO, CREATE, ALTER, TRUNCATE, DROP". We will revisit this as we start adding support for Level 3.


### Outer joins are not supported with near-neighbor queries

Qserv does not support LEFT or RIGHT joins with near-neighbor predicates.


### MySQL-specific syntax is not supported

MySQL-specific syntax is not supported. Example of unsupported syntax that will be rejected: NAME_CONST.


### Repeated column names through * are not supported

Queries with a * that resolves to repeated column name are not supported. Example:

    SELECT *, id
    FROM   Object

will fail if the table Object has a column called "id". Similarly, this query will fail:

    SELECT o.*, s.*
    FROM   Object AS o,
           Source AS s

if both tables Object and Source have a column called "id".

A workaround would be to select columns explicitly and alias them, e.g.

    SELECT o.id AS oId, s.id AS sId
    FROM   Object AS o,
           Source AS s


### "USE INDEX()" is not supported

Qserv will reject query with "USE INDEX" hint.


### Variables are not supported

You can't select into a variable. For example

    SELECT scisql_s2CPolyToBin(...)
    FROM   T
    INTO   @poly

will fail. Related story [DM-2874](https://jira.lsstcorp.org/browse/DM-2874).

User Defined Functions
----------------------

Qserv installation always comes with a set of predefined user defined functions:
 * spherical geometry aimed to allow quick answers to the following sorts of questions:
   * Which points in a table lie inside a region on the sphere? For example, an astronomer might wish to know which stars and galaxies lie inside the region of the sky observed by a single camera CCD.
   * Which spherical regions in a table contain a particular point? For example, an astronomer might with to know which telescope images overlap the position of interesting object X
 * photometry, aimed to provide conversions between raw fluxes, calibrated (AB) fluxes and AB magnitudes.

For details, see [Science Tools for MySQL](http://lsst-web.ncsa.illinois.edu/schema/sciSQL_0.3/)


Example Queries Supported
-------------------------


### Counts and simple selections

You can count objects and run simple selections. Few examples:


#### Count the number of rows in a table

    SELECT COUNT(*)
    FROM   Object


#### Find rows with a particular id

    SELECT *
    FROM   Object
    WHERE  objectId = <theId>


#### Select rows in a given area

    SELECT objectId
    FROM   Object
    WHERE  qserv_areaspec_box(1, 35, 2, 38)


#### Select rows in a given area meeting certain criteria

    SELECT COUNT(*)
    FROM   Object
    WHERE  qserv_areaspec_box(0.1, -6, 4, 6)
    AND    x = 3.4
    AND    y BETWEEN 1 AND 2

#### Find a row with a particular id

    SELECT *
    FROM   Object
    WHERE  objectId = <theId>


### Joins

#### Join two tables

    SELECT s.ra, s.decl, o.raRange, o.declRange
    FROM   Object o,
           Source s
    WHERE  o.objectId = <theId>
      AND  o.objectId = s.objectId

or

    SELECT s.ra, s.decl, o.raRange, o.declRange
    FROM   Object o,
           Source s USING (objectId
    WHERE  o.objectId = <theId>


#### Find near neighbors in a given region

    SELECT o1.objectId AS objId1,
           o2.objectId AS objId2,
           scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) AS distance
    FROM   Object o1,
           Object o2
    WHERE  qserv_areaspec_box(0, 0, 0.2, 1)
    AND    scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.05
    AND    o1.objectId <> o2.objectId

### LIMIT, ORDER BY

#### Limit results, sort results

    SELECT *
    FROM   Object
    WHERE  x > 4
    ORDER BY x
    LIMIT  100


Known Bugs
----------

The list of all known / reported problems can be found at: [Data Access and Database Team User-facing Bugs](https://jira.lsstcorp.org/issues/?filter=13501)


#### Selecting by objectId can miss a row

Selecting rows using objectId sometimes does not return rows it should.
For details, see: [DM-2864](https://jira.lsstcorp.org/browse/DM-2864).


#### WHERE objectId BETWEEN fails

As explained above, queries in the form "WHERE objectId BETWEEN" are discouraged. In fact, Qserv will currently return a cryptic message when such query is executed. For details, see [DM-2873](https://jira.lsstcorp.org/browse/DM-2873).


Notes of Performance
--------------------

### Use objectId when selecting sources

If you need to locate a small number of sources, try to use objectId if you can. If you don't, your query will require an index scan for every chunk of the Source table (which can potentially mean thousands of chunk-queries). For example this query will require it:

    SELECT * FROM Source WHERE sourceId = 500

but asking for sources related to a given object, like this one:

    SELECT * FROM Source WHERE objectId = 123 AND sourceId = 500

will require an index scan for just a single chunk, and thus will typically be much faster.
