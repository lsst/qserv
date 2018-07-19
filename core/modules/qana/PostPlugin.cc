// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 AURA/LSST.
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

/**
  * @file
  *
  * @brief PostPlugin does the right thing to handle LIMIT (and
  * perhaps ORDER BY and GROUP BY) clauses.
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "qana/PostPlugin.h"

// System headers
#include <cstddef>
#include <stdexcept>
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"
#include "global/stringUtil.h"
#include "qana/AnalysisError.h"
#include "query/FuncExpr.h"
#include "query/OrderByClause.h"
#include "query/QueryContext.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/ValueFactor.h"
#include "util/IterableFormatter.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.PostPlugin");
}

namespace lsst {
namespace qserv {
namespace qana {

////////////////////////////////////////////////////////////////////////
// PostPlugin implementation
////////////////////////////////////////////////////////////////////////
void
PostPlugin::applyLogical(query::SelectStmt& stmt,
                         query::QueryContext& context) {
    _limit = stmt.getLimit();
    if (stmt.hasOrderBy()) {
        _orderBy = stmt.getOrderBy().clone();
    }
}

void
PostPlugin::applyPhysical(QueryPlugin::Plan& plan,
                          query::QueryContext& context) {
    // Idea: If a limit is available in the user query, compose a
    // merge statement (if one is not available)
    LOGS(_log, LOG_LVL_DEBUG, "Apply physical");

    if (_limit != NOTSET) {
        // [ORDER BY ...] LIMIT ... is a special case which require sort on worker and sort/aggregation on czar
        if (context.hasChunks()) {
             LOGS(_log, LOG_LVL_DEBUG, "Add merge operation");
             context.needsMerge = true;
         }
    } else if (_orderBy) {
        // If there is no LIMIT clause, remove ORDER BY clause from all Czar queries because it is performed by
        // mysql-proxy (mysql doesn't garantee result order for non ORDER BY queries)
        LOGS(_log, LOG_LVL_TRACE, "Remove ORDER BY from parallel and merge queries: \""
             << *_orderBy << "\"");
        for (auto i = plan.stmtParallel.begin(), e = plan.stmtParallel.end(); i != e; ++i) {
            (**i).setOrderBy(nullptr);
        }
        if (context.needsMerge) {
            plan.stmtMerge.setOrderBy(nullptr);
        }
    }

    // For query results to be ordered, the columns and/or aliases used by the ORDER BY statement must also
    // be present in the SELECT statement. Only unqualified column names in the SELECT statement and that are
    // *not* in a function or expression may be used by the ORDER BY statement. For example, things like
    // ABS(col), t.col,  and col * 5 must be aliased if they will be used by the ORDER BY statement.
    //
    // This block creates a list of column names & aliases in the select list that may be used by the
    // ORDER BY statement.
    if (_orderBy) {
        auto const& selectList = plan.stmtOriginal.getSelectList();
        auto const& selValExprList = selectList.getValueExprList();
        std::vector<std::string> validSelectCols;
        LOGS(_log, LOG_LVL_DEBUG, "finding columns usable by ORDER BY from SELECT valueExprs:"
                << util::printable(*selValExprList));
        for (auto const& selValExpr : *selValExprList) {
            std::string alias = selValExpr->getAlias();
            // If the SELECT column has an alias, the ORDER BY statement must use the alias.
            if (!alias.empty()) {
                validSelectCols.push_back(alias);
            } else {
                // if the ValueExpr is not of type COLUMN, getColumnRef will return nullptr;
                auto const & selColumnRef = selValExpr->getColumnRef();
                if (nullptr == selColumnRef) {
                    continue;
                }
                if (selColumnRef->db.empty() == false || selColumnRef->table.empty() == false) {
                    continue;
                }
                validSelectCols.push_back(selColumnRef->column);
            }
        }
        LOGS(_log, LOG_LVL_DEBUG, "valid colNames=" << util::printable(validSelectCols));

        // For each element in the ORDER BY clause, see if it matches one item in validSelectCol
        auto orderBy = plan.stmtOriginal.getOrderBy();
        auto ordByTerms = orderBy.getTerms();
        for (auto const& ordByTerm : *ordByTerms) {
            LOGS(_log, LOG_LVL_DEBUG, "order by term=" << ordByTerm);
            auto const& expr = ordByTerm.getExpr();
            query::ValueExpr::FactorOpVector& factorOps = expr->getFactorOps();
            for (auto& factorOp:factorOps) {
                query::ColumnRef::Vector orderByColumnRefVec;
                factorOp.factor->findColumnRefs(orderByColumnRefVec);
                for (auto const & ordByColRef : orderByColumnRefVec) {
                    std::string orderByDb = ordByColRef->db;
                    std::string orderByTbl = ordByColRef->table;
                    std::string orderByCol = ordByColRef->column;
                    LOGS(_log, LOG_LVL_DEBUG, "Order By factorOp->factor=" << factorOp.factor
                         << " db=" << orderByDb << " tbl=" << orderByTbl << " col=" << orderByCol);

                    // If db or table is not empty, that's an error as those typically will not
                    // be passed back to the proxy in the table column name and the proxy will
                    // throw an error when it can't locate the ORDER BY argument in the result columns.
                    if (!orderByDb.empty() || !orderByTbl.empty()) {
                        if (!orderByTbl.empty()) orderByTbl += ".";
                        std::string msg = "ORDER BY argument should not include database or table for "
                            + orderByDb + " " + orderByTbl + orderByCol + ".\n Remove qualifiers or "
                            "use an alias.\n  ex:'SELECT a.val AS aVal ... ORDER BY aVal";
                        LOGS(_log, LOG_LVL_WARN, msg);
                        throw AnalysisError(msg);
                    }

                    // Check if the ORDER BY column matches a single valid column
                    int matchCount = 0;
                    for (auto& selCol : validSelectCols) {
                        if (selCol == orderByCol) ++matchCount;
                    }
                    if (matchCount == 0) {
                        // error, no match for ORDER BY column
                        std::string msg = "ORDER BY No match for " + orderByCol
                                                    + " in " + toString(util::printable(validSelectCols)) + ".\n"
                                                    "  Consider an alias. "
                                                    "ex:'SELECT a.val AS aVal, ABS(val) AS absVal ... ORDER BY aVal, absVal'";
                        LOGS(_log, LOG_LVL_WARN, msg);
                        throw AnalysisError(msg);
                    }
                    if (matchCount > 1) {
                        // error, the output column is ambiguous
                        std::string msg = "ORDER BY Duplicate match for " + orderByCol
                                + " in " + toString(util::printable(validSelectCols)) + ".\n Use an alias. "
                                "ex:'SELECT a.val, b.val AS bVal ... ORDER BY bVal";
                        LOGS(_log, LOG_LVL_WARN, msg);
                        throw AnalysisError(msg);
                    }
                }
            }

        }
    }

    if (context.needsMerge) {
        // Prepare merge statement.
        // If empty select in merger, create one with *
        query::SelectList& mList = plan.stmtMerge.getSelectList();
        auto vlist = mList.getValueExprList();
        if (not vlist) {
            throw std::logic_error("Unexpected NULL ValueExpr in SelectList");
        }
        // FIXME: is it really useful to add star if select clause is empty?
        if (vlist->empty()) {
            mList.addStar(std::string());
        }
    }
}

}}} // namespace lsst::qserv::qana
