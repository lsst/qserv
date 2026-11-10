// -*- LSST-C++ -*-
/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef LSST_QSERV_QANA_MATCHEQUIJOINREWRITE_H
#define LSST_QSERV_QANA_MATCHEQUIJOINREWRITE_H

/// \file
/// \brief Chunk-level equi-join rewrite optimization for ref-match tables
///
/// Consider a ref-match query like the following that equi-joins between a ref-match table and both of its
/// directors:
///
///     SELECT a.ra, a.dec, n.a_ra, n.a_dec,
///            b.ra, b.dec, n.b_ra, n.b_dec, b.id_b
///     FROM catalog_a.objects AS a
///     JOIN matches_a_b.matches AS n ON a.id_a = n.id_a
///     JOIN catalog_b.objects AS b ON n.id_b = b.id_b
///     WHERE n.mag BETWEEN 10 AND 10.5;
///
/// Resolving this query will require us to use overlap tables since a match can link across different chunks.
/// However, using subchunks in this case is not strictly necessary and can cause a performance hit because of
/// subchunk materialization costs and higher query load on the system. This rewrites such queries to avoid
/// subchunking, using a pair of chunk-level queries:
///
///     SELECT a.ra, a.dec, n.a_ra, n.a_dec,
///            b.ra, b.dec, n.b_ra, n.b_dec, b.id_b
///     FROM catalog_a.objects_%CC% AS a
///     JOIN matches_a_b.matches_%CC% AS n ON a.id_a = n.id_a
///     JOIN catalog_b.objects_%CC% AS b ON n.id_b = b.id_b
///     WHERE n.mag BETWEEN 10 AND 10.5;
///
///     SELECT a.ra, a.dec, n.a_ra, n.a_dec,
///            b.ra, b.dec, n.b_ra, n.b_dec, b.id_b
///     FROM catalog_a.objects_%CC% AS a
///     JOIN matches_a_b.matches_%CC% AS n ON a.id_a = n.id_a
///     JOIN catalog_b.objectsFullOverlap_%CC% AS b ON n.id_b = b.id_b
///     LEFT JOIN catalog_b.objectsFullOverlap_%CC% AS _qserv_overlap_dup
///         ON _qserv_overlap_dup.id_b = b.id_b AND
///            _qserv_overlap_dup.subChunkId < b.subChunkId
///     WHERE (n.flags & 2) = 0 AND _qserv_overlap_dup.id_b IS NULL AND
///           (n.mag BETWEEN 10 AND 10.5);
///
/// Here `b` requires overlap, and 2 is its partitioning flag. The first query finds matches where `b` rows
/// are in the chunk. The second query is restricted (via the flag) to matches in a neighboring chunk, and
/// therefore in the chunk overlap. The anti-join handles duplicates.
///
/// NOTE: the rewrite assumes that the directors' primary keys are unique. Using this optimization with
/// `unique_primary_key` turned off can produce incorrect results.

// System headers
#include <list>
#include <optional>
#include <vector>

// Qserv headers
#include "query/typedefs.h"

// Forward declarations
namespace lsst::qserv::query {
class SelectStmt;
}  // namespace lsst::qserv::query

namespace lsst::qserv::qana {

struct Vertex;

/// Applies the ref-match equi-join rewrite optimization if it's turned on and the given statement is eligible
/// for it.
///
/// @param stmt  Original query [in]. Modified if it can be rewritten.
/// @param vertices  The vertices of the query's relation graph.
/// @param overlapRefs  The director vertices in `vertices` that require overlap.
/// @param outputs  Chunk-level queries are appended here.
/// @return `true` if the query could be rewritten.
bool tryRewritingMatchEquiJoin(query::SelectStmt& stmt, std::list<Vertex>& vertices,
                               std::vector<Vertex*> const& overlapRefs, query::SelectStmtPtrVector& outputs);

/// For testing: force the optimization for ref-match equi-join overlap rewriting to run
void forceMatchEquiJoinRewrite(std::optional<bool> enabled);

}  // namespace lsst::qserv::qana

#endif  // LSST_QSERV_QANA_MATCHEQUIJOINREWRITE_H
