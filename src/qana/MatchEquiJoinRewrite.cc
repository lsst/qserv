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

// Class header
#include "qana/MatchEquiJoinRewrite.h"

// System headers
#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

// Third-party headers
#include "boost/algorithm/string/predicate.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "global/constants.h"
#include "qana/RelationGraph.h"
#include "qana/TableInfo.h"
#include "query/AndTerm.h"
#include "query/BoolFactor.h"
#include "query/ColumnRef.h"
#include "query/CompPredicate.h"
#include "query/FromList.h"
#include "query/GroupByClause.h"
#include "query/HavingClause.h"
#include "query/NullPredicate.h"
#include "query/OrderByClause.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"

namespace lsst::qserv::qana {

using query::AndTerm;
using query::BoolFactor;
using query::ColumnRef;
using query::CompPredicate;
using query::JoinRef;
using query::JoinSpec;
using query::NullPredicate;
using query::SelectStmt;
using query::SelectStmtPtrVector;
using query::TableRef;
using query::ValueFactor;
using query::WhereClause;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.MatchEquiJoinRewrite");

/// For testing: turns this optimization on, regardless of configuration.
std::optional<bool> forceRewrite;

/// @return `true` if `target` is a table ref in its JOIN sequence
bool containsTableRef(TableRef const& tr, TableRef const* target) {
    if (&tr == target) {
        return true;
    }
    for (auto const& join : tr.getJoins()) {
        if (join->getRight() != nullptr && containsTableRef(*join->getRight(), target)) {
            return true;
        }
    }
    return false;
}

/// Checks whether this ref-match query can have the chunk-level optimization applied.
MatchTableInfo const* isEligible(SelectStmt const& stmt, std::list<Vertex> const& vertices,
                                 Vertex const& overlapVertex) {
    MatchTableInfo const* match = nullptr;
    size_t numMatchVertices = 0;
    for (Vertex const& v : vertices) {
        if (v.info->kind == TableInfo::MATCH) {
            ++numMatchVertices;
            match = static_cast<MatchTableInfo const*>(v.info);
        }
        for (Edge const& e : v.edges) {
            // Spatial edges **require** sub-chunking
            if (e.isSpatial() &&
                (v.info->kind != TableInfo::MATCH || e.vertex->info->kind != TableInfo::MATCH)) {
                return nullptr;
            }
        }
    }
    // We require one pair of match vertices and their partitioning flags
    if (numMatchVertices != 2 || match->flagCol.empty()) {
        return nullptr;
    }
    // This cannot apply if the directors we received are the same table
    if (*match->director.first == *match->director.second) {
        return nullptr;
    }
    if (!(*overlapVertex.info == *match->director.first) &&
        !(*overlapVertex.info == *match->director.second)) {
        return nullptr;
    }

    // SELECT * would break our anti-join to remove duplicates. Not allowed upstream, but worth being sure.
    for (auto const& valueExpr : *stmt.getSelectList().getValueExprList()) {
        if (valueExpr->isStar()) {
            return nullptr;
        }
    }

    // If we're operating on subChunkIds, then we can't use the optimized plan since it explicitly excludes
    // them. While this scenario is very unlikely (perhaps only operators would execute such a query), the
    // check is still here for safety.
    query::ValueExprPtrVector expressions = *stmt.getSelectList().getValueExprList();
    if (stmt.hasWhereClause()) stmt.getWhereClause().findValueExprs(expressions);
    if (stmt.hasGroupBy()) stmt.getGroupBy().findValueExprs(expressions);
    if (stmt.hasHaving()) stmt.getHaving().findValueExprs(expressions);
    if (stmt.hasOrderBy()) stmt.getOrderBy().findValueExprs(expressions);
    auto pending = stmt.getFromList().getTableRefList();
    while (!pending.empty()) {
        auto table = pending.back();
        pending.pop_back();
        for (auto const& join : table->getJoins()) {
            // NATURAL joins and USING(subChunkId) compare subChunkId implicitly.
            if (join->isNatural()) return nullptr;
            if (auto const& spec = join->getSpec()) {
                if (spec->getUsing() && boost::iequals(spec->getUsing()->getColumn(), SUB_CHUNK_COLUMN)) {
                    return nullptr;
                }
                if (spec->getOn()) spec->getOn()->findValueExprs(expressions);
            }
            if (join->getRight()) pending.push_back(join->getRight());
        }
    }

    // An unqualified subChunkId may belong to the overlap director
    ColumnRef::Vector columns;
    for (auto const& expression : expressions) expression->findColumnRefs(columns);
    bool const referencesOverlapSubChunk =
            std::any_of(columns.begin(), columns.end(), [&](auto const& column) {
                return boost::iequals(column->getColumn(), SUB_CHUNK_COLUMN) &&
                       (column->getTableAlias().empty() ||
                        column->getTableAlias() == overlapVertex.tr.getAlias());
            });
    return referencesOverlapSubChunk ? nullptr : match;
}

/// Appends the pair of chunk-level queries described in `MatchEquiJoinRewrite.h` to `outputs`
void rewriteChunkLevel(SelectStmt& stmt, std::list<Vertex> const& vertices, Vertex& overlapVertex,
                       MatchTableInfo const& match, SelectStmtPtrVector& outputs) {
    // Table refs have been rewritten as chunk templates.
    outputs.push_back(stmt.clone());

    TableRef const* matchRef = nullptr;
    for (Vertex const& v : vertices) {
        if (v.info == &match) {
            matchRef = &v.tr;
            break;
        }
    }
    DirTableInfo const& dir = static_cast<DirTableInfo const&>(*overlapVertex.info);
    std::string const flagBit = (dir == *match.director.first) ? "1" : "2";
    std::string const& alias = overlapVertex.tr.getAlias();
    std::string const dupAlias = "_qserv_overlap_dup";
    std::string const overlapTemplate = dir.table + "FullOverlap_" + CHUNK_TAG;

    overlapVertex.tr.setDb(dir.database);
    overlapVertex.tr.setTable(overlapTemplate);

    // Returns a reference to `column` of the overlap director, qualified by `tableAlias`.
    auto dirColumn = [&dir](std::string const& tableAlias, std::string const& column) {
        return query::ValueExpr::newColumnExpr(dir.database, dir.table, tableAlias, column);
    };

    // Add anti-join containing the overlap reference.
    auto onTerm = std::make_shared<AndTerm>();
    onTerm->addBoolTerm(std::make_shared<BoolFactor>(std::make_shared<CompPredicate>(
            dirColumn(dupAlias, dir.pk), CompPredicate::EQUALS_OP, dirColumn(alias, dir.pk))));
    onTerm->addBoolTerm(std::make_shared<BoolFactor>(std::make_shared<CompPredicate>(
            dirColumn(dupAlias, SUB_CHUNK_COLUMN), CompPredicate::LESS_THAN_OP,
            dirColumn(alias, SUB_CHUNK_COLUMN))));
    auto dupRef = std::make_shared<TableRef>(dir.database, overlapTemplate, dupAlias);
    auto join = std::make_shared<JoinRef>(dupRef, JoinRef::LEFT, false, std::make_shared<JoinSpec>(onTerm));
    bool joinAdded = false;
    for (auto const& tableRef : stmt.getFromList().getTableRefList()) {
        if (containsTableRef(*tableRef, &overlapVertex.tr)) {
            tableRef->addJoin(join);
            joinAdded = true;
            break;
        }
    }
    if (!joinAdded) {
        throw std::logic_error("MatchEquiJoinRewrite: overlap table reference not found in FROM list.");
    }

    // Restrict the query to cross-chunk matches and to one copy of each overlap row.
    query::ValueExpr::FactorOpVector flagOps;
    flagOps.emplace_back(ValueFactor::newColumnRefFactor(std::make_shared<ColumnRef>(
                                 match.database, match.table, matchRef->getAlias(), match.flagCol)),
                         query::ValueExpr::BIT_AND);
    flagOps.emplace_back(ValueFactor::newConstFactor(flagBit));
    auto flagFactor = std::make_shared<BoolFactor>(std::make_shared<CompPredicate>(
            std::make_shared<query::ValueExpr>(flagOps), CompPredicate::EQUALS_OP,
            query::ValueExpr::newSimple(ValueFactor::newConstFactor("0"))));
    auto nullFactor =
            std::make_shared<BoolFactor>(std::make_shared<NullPredicate>(dirColumn(dupAlias, dir.pk), false));
    if (!stmt.hasWhereClause()) {
        stmt.setWhereClause(std::make_shared<WhereClause>());
    }
    stmt.getWhereClause().prependAndTerm(nullFactor);
    stmt.getWhereClause().prependAndTerm(flagFactor);

    outputs.push_back(stmt.clone());
}

}  // anonymous namespace

bool tryRewritingMatchEquiJoin(SelectStmt& stmt, std::list<Vertex>& vertices,
                               std::vector<Vertex*> const& overlapRefs, SelectStmtPtrVector& outputs) {
    if (overlapRefs.size() != 1) {
        return false;
    }
    Vertex& overlapVertex = *overlapRefs.front();
    MatchTableInfo const* match = isEligible(stmt, vertices, overlapVertex);
    if (match == nullptr) {
        return false;
    }
    bool const enabled = forceRewrite.has_value()
                                 ? *forceRewrite  // for testing
                                 : cconfig::CzarConfig::instance()->getMatchEquiJoinRewrite();
    if (!enabled) {
        return false;
    }
    rewriteChunkLevel(stmt, vertices, overlapVertex, *match, outputs);
    LOGS(_log, LOG_LVL_INFO,
         "Ref-match equi-join optimization applied: match="
                 << match->database << "." << match->table << ", overlap=" << overlapVertex.info->database
                 << "." << overlapVertex.info->table << ", alias=" << overlapVertex.tr.getAlias()
                 << ", plan=chunk-level, statements=2");
    return true;
}

void forceMatchEquiJoinRewrite(std::optional<bool> enabled) { forceRewrite = enabled; }

}  // namespace lsst::qserv::qana
