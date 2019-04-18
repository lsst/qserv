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

/// \file
/// \brief TablePlugin implementation.
///
/// TablePlugin modifies the parsed query to assign an alias to all the table
/// references in the query from-list. It then rewrites all column references
/// (e.g. in the where clause) to use the appropriate aliases. This allows
/// changing a table reference in a query without editing anything except the
/// from-clause.
///
/// During the concrete query planning phase, TablePlugin determines whether
/// each query proposed for parallel (worker-side) execution is actually
/// parallelizable and how this should be done - that is, it determines whether
/// or not sub-chunking should be used and which director table(s) to use
/// overlap for. Finally, it rewrites table references to use name patterns
/// into which (sub-)chunk numbers can be substituted. This act of substitution
/// is the final step in generating the queries sent out to workers.
///
/// \author Daniel L. Wang, SLAC

// Class header
#include "qana/TablePlugin.h"

// System headers
#include <string>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qana/QueryMapping.h"
#include "qana/RelationGraph.h"
#include "qana/TableInfoPool.h"

#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/GroupByClause.h"
#include "query/HavingClause.h"
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "query/OrderByClause.h"
#include "query/QueryContext.h"
#include "query/SelectList.h"
#include "query/SelectStmt.h"
#include "query/TableAlias.h"
#include "query/TableRef.h"
#include "query/typedefs.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"
#include "util/common.h"
#include "util/IterableFormatter.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qana.TablePlugin");

template <typename CLAUSE_T>
void matchValueExprs(lsst::qserv::query::QueryContext& context, CLAUSE_T & clause) {
    lsst::qserv::query::ValueExprPtrRefVector valueExprRefs;
    clause.findValueExprRefs(valueExprRefs);
    for (auto&& valueExprRef : valueExprRefs) {
        auto&& valueExprMatch = context.selectListAliases.getValueExprMatch(valueExprRef.get());
        if (nullptr != valueExprMatch) {
            valueExprRef.get() = valueExprMatch;
        }
    }
}


// Change the contents of the ValueExprs to use the TableRef objects that are stored in the context, instead
// of allowing these ValueExprs to own their own unique TableRef objects.
void matchTableRefs(lsst::qserv::query::QueryContext& context,
                    lsst::qserv::query::ValueExprPtrVector& valueExprs) {
    for (auto&& valueExpr : valueExprs) {
        if (valueExpr->isStar()) {
            auto valueFactor = valueExpr->getFactor();
            auto tableRefMatch = context.tableAliases.getTableRefMatch(valueFactor->getTableStar());
            if (nullptr != tableRefMatch) {
                valueFactor->setStar(tableRefMatch);
            }
            continue;
        }
        // Otherwise, get all the contained column refs and handle them.
        std::vector<std::shared_ptr<lsst::qserv::query::ColumnRef>> columnRefs;
        valueExpr->findColumnRefs(columnRefs);
        for (auto& columnRef : columnRefs) {
            std::shared_ptr<lsst::qserv::query::TableRefBase>& tableRef = columnRef->getTableRef();
            auto&& tableRefMatch = context.tableAliases.getTableRefMatch(tableRef);
            if (nullptr != tableRefMatch) {
                tableRef = tableRefMatch;
            }
        }
    }
}

template <typename CLAUSE_T>
void matchTableRefs(lsst::qserv::query::QueryContext& context, CLAUSE_T & clause) {
    lsst::qserv::query::ValueExprPtrVector valueExprs;
    clause.findValueExprs(valueExprs);
    matchTableRefs(context, valueExprs);
}

} // namespace

namespace lsst {
namespace qserv {
namespace qana {


////////////////////////////////////////////////////////////////////////
// TablePlugin implementation
////////////////////////////////////////////////////////////////////////
void
TablePlugin::applyLogical(query::SelectStmt& stmt,
                          query::QueryContext& context) {
    LOGS(_log, LOG_LVL_TRACE, "applyLogical begin:\n\t" << stmt.getQueryTemplate() << "\n\t" << stmt);
    query::FromList& fromList = stmt.getFromList();
    context.collectTopLevelTableSchema(fromList);

    // for each top-level ValueExpr in the SELECT list that does not have an alias, assign an alias that
    // matches the original user query and add that item to the selectListAlias list.
    for (auto& valueExpr : *(stmt.getSelectList().getValueExprList())) {
        if (not valueExpr->hasAlias()) {
            if (not valueExpr->isStar()) {
                valueExpr->setAlias(valueExpr->sqlFragment(false));
                if (not context.selectListAliases.set(valueExpr, valueExpr->getAlias())) {
                    throw std::logic_error("could not set alias for " + valueExpr->sqlFragment(false));
                }
            }
        } else {
            valueExpr->setAliasIsUserDefined(true);
        }
    }

    // make sure the TableRefs in the from list are all completetly populated (db AND table)
    query::TableRefList& fromListTableRefs = fromList.getTableRefList();
    for (auto&& tableRef : fromListTableRefs) {
        tableRef->verifyPopulated(context.defaultDb);
    }

    // update the dominant db in the context ("dominant" is not the same as the default db)
    if (fromListTableRefs.size() > 0) {
        context.dominantDb = fromListTableRefs[0]->getDb();
        _dominantDb = context.dominantDb;
    }

    // Add aliases to all table references in the from-list (if
    // they don't exist already) and then patch the other clauses so
    // that they refer to the aliases.
    //
    // The purpose of this is to confine table name references to the
    // from-list so that the later table-name substitution is confined
    // to modifying the from-list.
    //
    // Note also that this must happen after the default db context
    // has been filled in, or alias lookups will be incorrect.

    std::function<void(query::TableRef::Ptr)> aliasSetter = [&] (query::TableRef::Ptr tableRef) {
        if (nullptr == tableRef) {
            return;
        }
        if (not tableRef->hasAlias()) {
            tableRef->setAlias(tableRef->getDb() + "." + tableRef->getTable());
        }
        if (not context.tableAliases.set(tableRef, tableRef->getAlias())) {
            throw std::logic_error("could not set alias for " + tableRef->sqlFragment());
        }
        for (auto&& joinRef : tableRef->getJoins()){
            aliasSetter(joinRef->getRight());
        }
    };
    std::for_each(fromListTableRefs.begin(), fromListTableRefs.end(), aliasSetter);

    matchTableRefs(context, *stmt.getSelectList().getValueExprList());

    if (stmt.hasOrderBy()) {
        matchTableRefs(context, stmt.getOrderBy());
        matchValueExprs(context, stmt.getOrderBy());
    }
    if (stmt.hasWhereClause()) {
        matchTableRefs(context, stmt.getWhereClause());
        matchValueExprs(context, stmt.getWhereClause());
    }
    if (stmt.hasGroupBy()) {
        matchTableRefs(context, stmt.getGroupBy());
        matchValueExprs(context, stmt.getGroupBy());
    }
    if (stmt.hasHaving()) {
        matchTableRefs(context, stmt.getHaving());
        matchValueExprs(context, stmt.getHaving());
    }

    LOGS(_log, LOG_LVL_TRACE, "OnClauses of Join:");
    // and in the on clauses of all join specifications.
    for (auto&& tableRef : fromListTableRefs) {
        for (auto&& joinRef : tableRef->getJoins()) {
            auto&& joinSpec = joinRef->getSpec();
            if (joinSpec) {
                // A column name in a using clause should be unqualified,
                // so only patch on clauses.
                auto&& onBoolTerm = joinSpec->getOn();
                if (onBoolTerm) {
                    matchTableRefs(context, *onBoolTerm);
                    matchValueExprs(context, *onBoolTerm);
                }
            }
        }
    }
    LOGS(_log, LOG_LVL_TRACE, "applyLogical end:\n\t" << stmt.getQueryTemplate() << "\n\t" << stmt);
}

void
TablePlugin::applyPhysical(QueryPlugin::Plan& p,
                           query::QueryContext& context)
{
    TableInfoPool pool(context.defaultDb, *context.css);
    if (!context.queryMapping) {
        context.queryMapping = std::make_shared<QueryMapping>();
    }

    if ((not p.stmtParallel.empty()) && p.stmtParallel.front() != nullptr) {
        p.stmtPreFlight = p.stmtParallel.front()->clone();
        LOGS(_log, LOG_LVL_TRACE, "set local worker query:" << p.stmtPreFlight->getQueryTemplate().sqlFragment());
    }

    // Process each entry in the parallel select statement set.
    typedef SelectStmtPtrVector::iterator Iter;
    SelectStmtPtrVector newList;
    for(Iter i=p.stmtParallel.begin(), e=p.stmtParallel.end(); i != e; ++i) {
        RelationGraph g(**i, pool);
        g.rewrite(newList, *context.queryMapping);
    }
    p.dominantDb = _dominantDb;
    p.stmtParallel.swap(newList);
}

}}} // namespace lsst::qserv::qana
