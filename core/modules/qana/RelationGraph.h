// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

#ifndef LSST_QSERV_QANA_RELATIONGRAPH_H
#define LSST_QSERV_QANA_RELATIONGRAPH_H

/// \file
/// \brief A data structure used for parallel query validation and rewriting.

/// \page par_query_rewriting Parallel Query Validation and Rewriting
///
/// [TOC]
///
/// As a consequence of its shared-nothing nature, there are limits on the
/// types of queries that Qserv can evaluate. In particular, any query
/// involving partitioned tables must be analyzed to make sure that it can
/// be decomposed into per-partition queries that are evaluable using only
/// data from that partition (on worker MySQL instances), plus a global
/// aggregation/merge step (on a czar MySQL instance). In the description
/// below, we focus on the validation and rewriting strategy for generating
/// parallel (worker-side) queries, and ignore the merge/aggregation step
/// that happens on the czar.
///
/// Join Types
/// ----------
///
/// Broadly speaking, Qserv supports equi-joins between director and match
/// or child tables, and near-neighbor spatial joins between director tables.
/// Please see the [table types](\ref table_types) page for descriptions of
/// the different kinds of tables Qserv supports.
///
/// Director-child Equi-joins
/// -------------------------
///
/// Equi-joins between director and child tables are easy to evaluate because
/// matching rows will always fall into the same chunk and sub-chunk. This
/// means that evaluating such a query in parallel over N (sub-)chunks is just
/// a matter of issuing the original query on each (sub-)chunk after replacing
/// the original table names with (sub-)chunk table names. Left and right
/// outer joins are easily supported in the same way.
///
/// Near-neighbor Joins
/// -------------------
///
/// Near-neighbor joins are harder to deal with because partition overlap must
/// be utilized. Qserv's evaluation strategy is best illustrated by means of
/// an example:
///
///     SELECT a.*, b.*
///         FROM Object AS a, Object AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///
/// The naive evaluation strategy for this join is to consider all pairs of
/// rows (in this case, astronomical objects) and only retain those with
/// sky-positions separated by less than 0.001 degrees. We improve on this
/// wasteful O(N²) strategy by running the following pair of queries for each
/// sub-chunk of each chunk and taking the union of the results:
///
///     SELECT a.*, b.*
///         FROM Object_%CC%_%SS% AS a, Object_%CC%_%SS% AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///     SELECT a.*, b.*
///         FROM Object_%CC%_%SS% AS a, ObjectFullOverlap_%CC%_%SS% AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///
/// In the above, `%CC%` and `%SS%` are placeholders for a chunk and sub-chunk
/// numbers. This is O(kN), where k is the number of objects per partition,
/// and can be evaluated under the constraints of Qserv's shared-nothing model
/// so long as an overlap sub-chunk contains all objects within 0.001 degrees
/// of the corresponding sub-chunk boundary.
///
/// Clearly, k should be kept small to avoid quadratic blowup. But making
/// it too small leads to excessive query dispatch and issue overhead. This
/// is the raison d'être for sub-chunks: using them allows us to lower k
/// without having to deal with dispatching a crippling number of chunk
/// queries to workers. In practice, sub-chunk tables are not materialized
/// on-disk, but are created by workers on the fly from chunk tables using
/// `CREATE TABLE ... ENGINE=MEMORY AS SELECT`.
///
/// Notice that query rewriting is still just a matter of duplicating the
/// original query and replacing table names with sub-chunk specific names.
/// Also, there are actually two ways to decompose the query. The decomposition
/// above finds all matches for a sub-chunk of `a`, but we can instead find
/// all matches for a sub-chunk of `b`:
///
///     SELECT a.*, b.*
///         FROM Object_%CC%_%SS% AS a, Object_%CC%_%SS% AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///     SELECT a.*, b.*
///         FROM ObjectFullOverlap_%CC%_%SS% AS a, Object_%CC%_%SS% AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///
/// Finally, the example could just as easily have used an INNER JOIN with
/// an ON clause, instead of the abbreviated JOIN syntax and WHERE clause.
///
/// What of outer joins? FULL OUTER JOIN is not supported by MySQL, so that
/// leaves the question of what to do with:
///
///     SELECT a.*, b.*
///         FROM Object AS a LEFT OUTER JOIN
///              Object AS b ON (
///                  scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///                  a.objectId != b.objectId);
///
/// This is not evaluable using the strategy described thus far, because
/// the sub-chunk overlap is in a separate table from the sub-chunk. Instead,
/// we would have to issue the following per sub-chunk:
///
///     SELECT a.*, b.*
///         FROM Object_%CC%_%SS% AS a LEFT OUTER JOIN
///              (SELECT * FROM Object_%CC%_%SS% UNION ALL
///               SELECT * FROM ObjectFullOverlap_%CC%_%SS%) AS b ON (
///                  scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///                  a.objectId != b.objectId);
///
/// Implementing this is somewhat painful and would require changes to the
/// query IR classes. Sub-chunk generation could be changed to generate the
/// UNION above directly (rather than the `FullOverlap` tables), but this
/// approach can almost double the memory required to hold an entire chunk
/// of sub-chunks in RAM. Since the worker wants chunks to fit entirely
/// in memory (so that disk I/O for table scans can be shared across multiple
/// queries), this may not be an option. Another possibility is to only
/// generate sub-chunk tables containing both sub-chunk and overlap rows,
/// along with a flag indicating whether rows belong to the overlap region.
/// This halves the number of in-memory tables that must be created and
/// populated and has identical memory requirements to the current strategy,
/// but means that flag-based duplicate removal logic must be added to many
/// queries. RIGHT joins have the same problem, as they are equivalent to
/// LEFT joins after commuting the left and right table references.
///
/// For now, Qserv does not support LEFT or RIGHT joins with near-neighbor
/// predicates.
///
/// Match Table Joins
/// -----------------
///
/// Match table equi-joins are also complicated by overlap. If a match-table
/// is joined against only one of the director tables it matches together, the
/// situation is simple:
///
///     SELECT d1.*, m.*
///         FROM Director1 AS d1 JOIN
///              Match AS m ON (d1.id = m.id1);
///
/// can be executed by rewriting table references as before:
///
///     SELECT d1.*, m.*
///         FROM Director1_%CC% AS d1 JOIN
///              Match_%CC% AS m ON (d1.id = m.id1);
///
/// since a match to a director table row from chunk C is guaranteed to
/// lie in chunk C of the match table. Note that the query can be
/// parallelized either over director table chunks or sub-chunks. However,
/// if the join involves both director tables:
///
///     SELECT d1.*, m.*, d2.*
///         FROM Director1 AS d1 JOIN
///              Match AS m ON (d1.id = m.id1) JOIN
///              Director2 AS d2 ON (m.id2 = d2.id);
///
/// then, since it is possible for rows in `d2` to match rows in `d1` from a
/// different chunk, overlap must be used:
///
///     SELECT d1.*, m.*, d2.*
///         FROM Director1_%CC%_%SS% AS d1 JOIN
///              Match_%CC% AS m ON (d1.id = m.id1) JOIN
///              Director2_%CC%_%SS% AS d2 ON (m.id2 = d2.id);
///     SELECT d1.*, m.*, d2.*
///         FROM Director1_%CC%_%SS% AS d1 JOIN
///              Match_%CC% AS m ON (d1.id = m.id1) JOIN
///              Director2FullOverlap_%CC%_%SS% AS d2 ON (m.id2 = d2.id);
///
/// Note that while sub-chunking could be enabled for match table chunks as
/// well, doing so would increase match table storage costs since matches
/// between different sub-chunks (rather than chunks) would have to be stored
/// twice. Furthermore, it would require additional in-memory tables to be
/// created and populated, and those tables would not come with prebuilt
/// indexes on their foreign keys.
///
/// As in the near-neighbor case, there are two ways to decompose the query:
/// overlap from either `d1` or `d2` can be utilized. And again, because the
/// union of overlap and non-overlap results is not performed within a single
/// query, Qserv cannot support arbitrary outer equi-joins between match and
/// director tables - LEFT and RIGHT joins involving match tables are not
/// supported. Additionally, match/match table joins are not currently
/// allowed.
///
/// Query Validation Algorithm
/// --------------------------
///
/// The query validation algorithm operates on a relation graph. This is an
/// undirected graph built from the input query, with vertices corresponding
/// to partitioned table references and edges corresponding to those join
/// predicates that can be used to make inferences about the partition of
/// results from one table based on the partition of results from another.
/// Such predicates are said to be admissible. For example, the graph for the
/// following query:
///
///     SELECT * FROM Object AS o INNER JOIN
///                   Source AS s ON (o.objectId = s.objectId);
///
/// would contain two vertices, one for Object (a director table) and one
/// for Source (a child table). Since the equi-join predicate forces matching
/// Object and Source rows to have the same partition, it is admissible and
/// so the graph has a single edge between the Object and Source vertices.
///
/// Equi-join predicates are not the only ones that can be used for partition
/// inference. Consider the query:
///
///     SELECT * FROM Director1 AS d1, Director2 AS d2 WHERE
///         scisql_angSep(d1.ra, d1.decl, d2.ra, d2.decl) < 0.01;
///
/// The spatial constraint says that rows from d1 are within 0.01 degrees of
/// matching rows in d2. If that is less than or equal to the partition
/// overlap and the directors are partitioned in the same way, then matching
/// rows from d1 and d2 must either belong to the same partition or each lie
/// in the overlap of the others partition. Admissible spatial constraints are
/// therefore represented by edges tagged with their angular separation
/// thresholds (0.01 degrees in the example).
///
/// The goal of the validation algorithm is to infer result row locality for
/// all table references in the query. It attempts to do this by first assuming
/// that all result rows for some initial vertex (table reference) V belong to
/// some partition. (Note that if there are any references to partitioned tables
/// in a query, then we must refrain from using overlap for at least one of
/// them to avoid duplicate result rows). The algorithm then uses the incident
/// graph edges to deduce that result rows from adjacent vertices have the same
/// partition, or lie in its overlap. The same process is repeated on the
/// immediately adjacent vertices to reach new graph vertices, and so on, until
/// no new vertices are reachable. If all the vertices in the graph were
/// visited and shown to have the required locality with V, then we know that a
/// Qserv worker need never consult with comrades to perform its share of query
/// evaluation work.
///
/// But how exactly are the edges used to infer partition locality? Well, an
/// edge tagged with angular separation α means that rows from adjacent
/// vertices are no more than α degrees apart (which is less than the partition
/// overlap). Because equality predicates say that rows from two vertices have
/// the same partitioning position, α = 0 for the corresponding edges. So if
/// there is a path between two vertices U and V, we know that the partitioning
/// positions of the rows from U are within distance Σα of V, where Σα is the
/// sum of angular separations for the edges on the path between them. If there
/// is more than one possible path between U and V, then we can say that their
/// rows are separated by at most min(Σα) along any path between them. If
/// min(Σα) is not more than the partition overlap, then U and V have the
/// required locality.
///
/// On the other hand, if there is no path between U and V, then the graph is
/// disconnected and we will never be able to infer locality of results for all
/// table references. In that case, Qserv must assume that it cannot evaluate
/// the query using only worker-local data and must report an error back to the
/// user.
///
/// If the validation algorithm fails to prove partition locality for a
/// particular choice of initial vertex, we try again with a different initial
/// vertex. If no choice of initial vertex V leads to a locality proof, the
/// input query is not evaluable, and an error is returned to the user. Note in
/// passing that since a locality proof computes min(Σα) to every graph vertex
/// from V, it also identifies the table references requiring overlap (those
/// with min(Σα) > 0). This is critical information for the query rewriting
/// stage, described in more detail later.
///
/// A more formal description of the algorithm is below, followed by a pair
/// of illustrative examples.
///
/// 1. Let S be the set of vertices corresponding to child or director tables.
///
/// 2. Choose a vertex V ∈ S and assume that the corresponding rows are
///    strictly within a partition. That is, the overlap oᵥ required for V
///    is 0. Set the required overlap for all other vertices to ∞, and create
///    an empty vertex queue Q.
///
/// 3. For each edge e incident to vertex V, infer the overlap oᵤ required
///    for vertex U reachable from V via e. If oᵤ is greater than the
///    available overlap, ignore U. Otherwise, set the required overlap for
///    U to the minimum of oᵤ and its current required overlap. If oᵤ was
///    smaller than the previous required overlap and U is not already in Q,
///    insert U into Q. oᵤ is determined from oᵥ based on the kinds of tables
///    linked by e (V → U):
///
///    - director → director:
///      oᵤ = oᵥ for an equi-join edge
///      oᵤ = oᵥ + α for a spatial edge with angular separation threshold α.
///
///    - match → match:
///      oᵤ = oᵥ + ρ, where ρ is the partition overlap
///
///    - all other edges:
///      oᵤ = oᵥ
///
///    There is a subtlety in the handling of match tables. Intuitively, these
///    tables are materialized near-neighbor joins between two directors. They
///    are therefore modeled by creating two vertices linked with a spatial
///    edge with angular separation threshold equal to the partition overlap ρ.
///    Since join predicates involving two match tables are not admissible,
///    this is the only way match → match edges can be created. Each vertex
///    in the pair receives edges for equi-join predicates involving one of the
///    match table foreign keys.
///
/// 4. If Q is non-empty, set V to the next vertex in Q, remove it from Q, and
///    continue at step 3. Otherwise, continue at step 5.
///
/// 5. If no vertex has a required overlap of ∞ after Q has been emptied, then
///    the query is evaluable; the directors requiring overlap will have been
///    identified by the graph traversal above. Otherwise, choose a different
///    starting vertex from S, and repeat the process starting from step 2.
///
/// 6. If all graph traversals starting from vertices in S result in one or
///    more vertices having a required overlap of ∞, then the query is not
///    evaluable by Qserv.
///
/// To illustrate the algorithm, consider its operation on the following query:
///
///     SELECT * FROM Director1 AS d1,
///                   Director2 AS d2,
///                   Director3 AS d3
///     WHERE scisql_angSep(d1.ra, d1.decl, d2.ra, d2.decl) < 0.1 AND
///           scisql_angSep(d2.ra, d2.decl, d3.ra, d3.decl) < 0.2;
///
/// Let's assume that all 3 directors are partitioned the same way, and that
/// partition overlap is 0.25 degrees. The relation graph for this query looks
/// like:
///
///     D₁ <-------> D₂ <-------> D₃
///           0.1          0.2
///
/// where Dᵢ is the vertex for the i-th director. We start by
/// picking D₁ as the initial, no-overlap vertex. From D₁ we visit D₂,
/// determining that D₂ has required overlap 0.1. From D₂ we reach D₃,
/// which has required overlap 0.3 (= 0.1 + 0.2), which is greater than the
/// partition overlap. In other words, the query is not evaluable starting
/// from D₁. So, we start from D₂ instead. We visit adjacent vertices D₁
/// and D₃ and determine that their required overlaps are 0.1 and 0.2. Both
/// are under the partition overlap, and all vertices were visited, so we
/// have produced a locality proof. The query can therefore be parallelized
/// by running the equivalent of
///
///     SELECT * FROM (SELECT * FROM Director1_%CC%_%SS% UNION ALL
///                    SELECT * FROM Director1FullOverlap_%CC%_%SS%) AS d1,
///                   Director2 AS d2,
///                   (SELECT * FROM Director3_%CC%_%SS% UNION ALL
///                    SELECT * FROM Director3FullOverlap_%CC%_%SS%) AS d3,
///     WHERE scisql_angSep(d1.ra, d1.decl, d2.ra, d2.decl) < 0.1 AND
///           scisql_angSep(d2.ra, d2.decl, d3.ra, d3.decl) < 0.2;
///
/// over all the sub-chunks on the sky and taking the union of the results.
///
/// Here is another example involving a match table.
///
///     SELECT * FROM Child1 AS c1,
///                   Match AS m,
///                   Child2 AS c2
///     WHERE c1.dirId = m.dir1Id AND m.dir2Id = c2.dirId;
///
/// has the following relation graph:
///
///     C₁ <-------------------> M₁ <------> M₂ <-------------------> C₂
///         c1.dirId = m.dir1Id       0.25       m.dir2Id = c2.dirId
///
/// where M₁ and M₂ are the pair of vertices used to represent the match
/// table M. Walking through the validation algorithm steps again, we see that
/// from initial vertex C₁ we visit M₁ and get a required overlap of 0 (from
/// the equi-join predicate). From M₁ we jump to M₂ and obtain a required
/// overlap of 0.25 degrees (from the spatial edge). Since C₂ is linked to
/// M₂ via an equality predicate, it has the same required overlap of 0.25
/// degrees.
///
/// Now because overlap isn't stored for child tables, that means the query is
/// not evaluable starting from C₁. So we repeat the graph traversal and start
/// from C₂ instead, concluding that the required overlap for C₁, also a
/// child table, is 0.25 degrees. Again the query isn't evaluable. Since we
/// cannot produce a locality proof from any starting vertex, we must report
/// an error back to the user.
///
/// Query Rewriting
/// ---------------
///
/// As alluded to earlier, the current query rewriting strategy involves
/// copying the input query and replacing the table references in its FROM
/// clause with chunk and sub-chunk specific table name patterns. The result
/// is a set of query templates into which specific (sub-)chunk numbers can
/// be substituted to obtain the actual queries that run on Qserv workers.
///
/// If the input query does not require overlap for any directors, then the
/// task is simple - we replace all partitioned table-references with
/// chunk-specific table name patterns. The input query is rewritten to a
/// single output query template.
///
/// If overlap is required for one or more directors, the task is more
/// complicated. Recall that overlap is stored in a separate in-memory table
/// per sub-chunk. Given an input query that looks like:
///
///     SELECT * FROM D1, D2, ... Dn ...;
///
/// where D1, D2, ... Dn are the directors requiring overlap, the rewriting
/// must produce the same results as:
///
///     SELECT * FROM
///         (SELECT * FROM D1_%CC%_%SS% UNION ALL SELECT * D1FullOverlap_%CC%_%SS%),
///         (SELECT * FROM D2_%CC%_%SS% UNION ALL SELECT * D2FullOverlap_%CC%_%SS%),
///         ...
///         (SELECT * FROM Dn_%CC%_%SS% UNION ALL SELECT * DnFullOverlap_%CC%_%SS%)
///     ...;
///
/// Unfortunately, the current IR class design does not allow that specific
/// rewriting due to lack of subquery support. However:
///
///     SELECT * FROM (SELECT * FROM A₀ UNION ALL SELECT * FROM A₁), B, ...;
///
/// is equivalent to the union of the results of the following pair of queries
/// in the absence of aggregation and sorting:
///
///     (SELECT * FROM A₀, B, ...);
///     (SELECT * FROM A₁, B, ...);
///
/// Applying the same rule twice allows us to transform:
///
///     SELECT ... FROM (SELECT * FROM A₀ UNION ALL SELECT * FROM A₁),
///                     (SELECT * FROM B₀ UNION ALL SELECT * FROM B₁), ...;
///
/// to a union of the following 4 queries:
///
///     (SELECT * FROM A₀, B₀, ...);
///     (SELECT * FROM A₀, B₁, ...);
///     (SELECT * FROM A₁, B₀, ...);
///     (SELECT * FROM A₁, B₁, ...);
///
/// In our case, the deferral of aggregation/sorting to the merge step on the
/// czar in conjunction with the join limitations discussed earlier allow us
/// to apply the same transformation in general, not just for the cross joins
/// illustrated above. So an input query containing N union-pair sub-queries
/// can be transformed to a union of 2ᴺ queries without such sub-queries.
///
/// The actual rewriting is performed by assigning a bit to each of the N
/// directors requiring overlap. A bit value of 0 is taken to mean that a
/// director table reference should be replaced with a sub-chunk specific
/// table name pattern. A value of 1 means it should be replaced with an
/// overlap sub-chunk table name pattern instead. Concatenating these bits
/// yields an N-bit integer where each possible value (0, 1, ..., 2ᴺ-1)
/// specifies the table reference substitutions required to obtain a single
/// output query template.
///
/// Because the time and space complexity of our query rewriting/execution
/// strategy is exponential in the number of table references requiring
/// overlap, we impose a strict limit on the maximum number of such
/// references.

// System headers
#include <limits>
#include <list>
#include <string>
#include <vector>

// Third-party headers
#include "boost/math/special_functions/fpclassify.hpp" // for isnan, isinf
#include "boost/shared_ptr.hpp"

// Local headers
#include "ColumnVertexMap.h"
#include "TableInfo.h"

#include "query/BoolTerm.h"
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "query/TableRef.h"
#include "query/typedefs.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class ColumnRef;
    class QueryContext;
    class SelectStmt;
}
namespace qana {
    class QueryMapping;
    class TableInfoPool;
}}}


namespace lsst {
namespace qserv {
namespace qana {

using query::SelectStmtPtrVector;

struct Vertex;

/// An `Edge` is a minimal representation of an admissible join predicate.
/// An admissible join predicate is one that can be used to infer the
/// partition of rows in one table from the partition of rows in another.
///
/// An edge corresponds to an equi-join predicate iff `angSep` is NaN.
/// Otherwise, it corresponds to a spatial predicate that constrains the
/// angle between two spherical coordinate pairs to be less than or equal to
/// `angSep`.
///
/// Note that the names of the columns involved in a predicate can be obtained
/// by examining the table references that are linked by its edge; for any
/// pair of references there is at most one equi-join and one spatial predicate
/// that can link them. Only one of the edge vertices is stored; the other owns
/// the `Edge` and is therefore implicitly available.
struct Edge {
    Vertex* vertex; // unowned
    double angSep;

    Edge() : vertex(0), angSep(0.0) {}
    Edge(Vertex* v, double a) : vertex(v), angSep(a) {}

    bool isSpatial() const { return !(boost::math::isnan)(angSep); }
    bool operator<(Edge const& p) const { return vertex < p.vertex; }
    bool operator==(Edge const& p) const { return vertex == p.vertex; }
    bool operator!=(Edge const& p) const { return vertex != p.vertex; }
};


/// A `Vertex` corresponds to an in-query partitioned table reference. A
/// reference to the underlying table metadata and a list of edges (join
/// predicates/constraints) that involve the table reference are bundled
/// alongside.
struct Vertex {
    /// `tr` is a table reference from the input query IR.
    query::TableRef& tr;
    /// `info` is an unowned pointer to metadata
    /// for the table referenced by `tr`.
    TableInfo const* info;
    /// `next` is unowned storage for the links in a singly-linked list.
    Vertex* next;
    /// `overlap` is the amount of overlap that must be available in partitions
    /// of the table referenced by `tr`. This member is used during query
    /// validation and rewriting.
    double overlap;
    /// `edges` is the set of edges incident to this vertex, implemented
    /// as a sorted vector. It will contain at most one edge to another
    /// vertex in the relation graph, and will never contain a loop.
    std::vector<Edge> edges;

    Vertex(query::TableRef& t, TableInfo const* i) :
        tr(t),
        info(i),
        next(0),
        overlap(std::numeric_limits<double>::infinity())
    {}

    /// `insert` adds the given join predicate to the set of predicates
    /// involving this table reference. If a predicate between the same
    /// vertices as `e` already exists, then the non-spatial predicate
    /// is retained (if there is one). Note that if both are non-spatial,
    /// the predicates must be duplicates of eachother. If both are spatial,
    /// the one with the smaller angular separation threshold is retained.
    void insert(Edge const& e);

    /// `rewriteAsChunkTemplate` rewrites `tr` to contain a chunk specific
    /// name pattern.
    void rewriteAsChunkTemplate() {
        tr.setDb(info->database);
        tr.setTable(info->getChunkTemplate());
    }

    /// `rewriteAsSubChunkTemplate` rewrites `tr` to contain a sub-chunk
    /// specific name pattern.
    void rewriteAsSubChunkTemplate() {
        tr.setDb(info->getSubChunkDb());
        tr.setTable(info->getSubChunkTemplate());
    }

    /// `rewriteAsOverlapTemplate` rewrites `tr` to contain an overlap
    /// sub-chunk specific name pattern.
    void rewriteAsOverlapTemplate() {
        tr.setDb(info->getSubChunkDb());
        tr.setTable(info->getOverlapTemplate());
    }
};


/// A relation graph consists of a list of vertices, representing the
/// partitioned table references of a query, linked by an edge for each join
/// predicate that can be used to infer the partition of rows in one table
/// from the partition of rows in another.
///
/// An empty relation graph represents a set of references to unpartitioned
/// tables that are joined in some arbitrary way.
///
/// Methods provide only basic exception safety - if a problem occurs, no
/// memory is leaked, but the graph and any output parameters may be in
/// inconsistent states and should no longer be used for query analysis.
class RelationGraph {
public:
    /// The maximum number of table references in a query that can require
    /// overlap before Qserv will throw up its digital hands in protest.
    static size_t const MAX_TABLE_REFS_WITH_OVERLAP = 8;

    /// This constructor creates a relation graph from a query.
    /// If the query is not evaluable, an exception is thrown.
    RelationGraph(query::QueryContext const& ctx,
                  query::SelectStmt& stmt,
                  TableInfoPool& pool);

    /// `empty` returns `true` if this graph has no vertices.
    bool empty() const { return _vertices.empty(); }

    /// `rewrite` rewrites the input query into a set of output queries.
    void rewrite(SelectStmtPtrVector& outputs,
                 QueryMapping& mapping);

    void swap(RelationGraph& g) {
        _vertices.swap(g._vertices);
        _map.swap(g._map);
    }

private:
    std::list<Vertex> _vertices;
    ColumnVertexMap _map;
    query::SelectStmt* _query; // unowned

    // Not implemented
    RelationGraph(RelationGraph const&);
    RelationGraph& operator=(RelationGraph const&);

    // Constructors and related helpers
    RelationGraph() : _query(0) {}
    RelationGraph(query::TableRef& tr,
                  TableInfo const* info,
                  double overlap);
    RelationGraph(query::QueryContext const& ctx,
                  query::TableRef::Ptr const& tr,
                  double overlap,
                  TableInfoPool& pool);

    size_t _addOnEqEdges(query::BoolTerm::Ptr on,
                         bool outer,
                         RelationGraph& g);
    size_t _addNaturalEqEdges(bool outer, RelationGraph& g);
    size_t _addUsingEqEdges(query::ColumnRef const& c,
                            bool outer,
                            RelationGraph& g);
    size_t _addWhereEqEdges(query::BoolTerm::Ptr where);
    size_t _addSpEdges(query::BoolTerm::Ptr bt, double overlap);
    void _fuse(query::JoinRef::Type joinType,
               bool natural,
               query::JoinSpec::Ptr const& joinSpec,
               double overlap,
               RelationGraph& g);

    // Graph validation
    bool _validate(double overlap);
};

}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_RELATIONGRAPH_H
